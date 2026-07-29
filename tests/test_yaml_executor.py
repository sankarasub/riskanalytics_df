from __future__ import annotations

import importlib
import sys
import tempfile
import unittest
from unittest.mock import patch

from tests.support import build_fake_pyspark


class FakeWriter:
    def __init__(self) -> None:
        self.append_calls = 0
        self.overwrite_partition_calls = 0

    def append(self) -> None:
        self.append_calls += 1

    def overwritePartitions(self) -> None:
        self.overwrite_partition_calls += 1


class FakeFrame:
    def __init__(self, columns: list[str] | None = None) -> None:
        self.columns = columns or []
        self.selected: tuple[str, ...] | None = None
        self.temp_view_name: str | None = None
        self.writer = FakeWriter()

    def select(self, *columns):
        self.selected = tuple(columns)
        return self

    def filter(self, _condition):
        return self

    def createOrReplaceTempView(self, view_name: str) -> None:
        self.temp_view_name = view_name

    def writeTo(self, _table_name: str) -> FakeWriter:
        return self.writer


class FakeReader:
    def __init__(self) -> None:
        self.format_name: str | None = None
        self.options: dict[str, str] = {}
        self.loaded_path: str | None = None
        self.frame = FakeFrame(columns=["trade_id", "volatility"])

    def format(self, name: str) -> "FakeReader":
        self.format_name = name
        return self

    def option(self, key: str, value: str) -> "FakeReader":
        self.options[key] = value
        return self

    def load(self, path: str) -> FakeFrame:
        self.loaded_path = path
        return self.frame


class FakeCatalog:
    def __init__(self) -> None:
        self.dropped_views: list[str] = []

    def dropTempView(self, view_name: str) -> None:
        self.dropped_views.append(view_name)


class FakeSpark:
    def __init__(self) -> None:
        self.read = FakeReader()
        self.catalog = FakeCatalog()
        self.sql_statements: list[str] = []

    def sql(self, statement: str) -> None:
        self.sql_statements.append(statement)


class YamlExecutorTests(unittest.TestCase):
    def _import_module(self):
        fake_modules, _builder = build_fake_pyspark()
        sys.modules.pop("risk_analytics.yaml_executor", None)

        with patch.dict(sys.modules, fake_modules, clear=False):
            module = importlib.import_module("risk_analytics.yaml_executor")

        return module

    def _import_expressions_module(self):
        fake_modules, _builder = build_fake_pyspark()
        sys.modules.pop("risk_analytics.transformations.expressions", None)

        with patch.dict(sys.modules, fake_modules, clear=False):
            module = importlib.import_module("risk_analytics.transformations.expressions")

        return module

    def test_load_source_file_uses_reader_format_options_and_path(self) -> None:
        module = self._import_module()
        spark = FakeSpark()

        source_spec = {
            "type": "file",
            "format": "json",
            "path": "/tmp/sourceb/product.json",
            "options": {"multiline": True},
            "select": ["trade_id", "volatility"],
        }

        frame = module._load_source(spark, source_spec, config={})

        self.assertIs(frame, spark.read.frame)
        self.assertEqual(spark.read.format_name, "json")
        self.assertEqual(spark.read.options, {"multiline": "True"})
        self.assertEqual(spark.read.loaded_path, "/tmp/sourceb/product.json")
        self.assertEqual(frame.selected, ("trade_id", "volatility"))

    def test_load_source_file_requires_dict_options(self) -> None:
        module = self._import_module()
        spark = FakeSpark()

        with self.assertRaises(module.PipelineValidationError):
            module._load_source(
                spark,
                {"type": "file", "format": "csv", "path": "/tmp/in.csv", "options": ["bad"]},
                config={},
            )

    def test_merge_target_generates_merge_sql_and_drops_temp_view(self) -> None:
        module = self._import_module()
        spark = FakeSpark()
        frame = FakeFrame(columns=["customer_id", "as_of_date", "rating", "source_system", "ingest_timestamp"])

        module._merge_target(
            spark,
            frame,
            "nessie.risk_analytics.customer_canonical",
            {"name": "customer_canonical", "keys": ["customer_id", "as_of_date"]},
        )

        self.assertIsNotNone(frame.temp_view_name)
        self.assertTrue(frame.temp_view_name.startswith("__yaml_merge_"))
        self.assertEqual(len(spark.sql_statements), 1)
        sql = spark.sql_statements[0]
        self.assertIn("MERGE INTO nessie.risk_analytics.customer_canonical AS target", sql)
        self.assertIn("target.customer_id = source.customer_id", sql)
        self.assertIn("target.as_of_date = source.as_of_date", sql)
        self.assertIn("target.rating = source.rating", sql)
        self.assertIn("target.source_system = source.source_system", sql)
        self.assertIn("target.ingest_timestamp = source.ingest_timestamp", sql)
        self.assertIn(frame.temp_view_name, spark.catalog.dropped_views)

    def test_merge_target_requires_non_empty_keys(self) -> None:
        module = self._import_module()
        spark = FakeSpark()
        frame = FakeFrame(columns=["customer_id"])

        with self.assertRaises(module.PipelineValidationError):
            module._merge_target(
                spark,
                frame,
                "nessie.risk_analytics.customer_canonical",
                {"name": "customer_canonical", "keys": []},
            )

    def test_merge_target_falls_back_to_overwrite_partitions_when_sql_merge_fails(self) -> None:
        module = self._import_module()

        class FailingSpark(FakeSpark):
            def sql(self, _statement: str) -> None:
                raise RuntimeError("No plan for TableReference")

        spark = FailingSpark()
        frame = FakeFrame(columns=["customer_id", "as_of_date", "rating"])

        module._merge_target(
            spark,
            frame,
            "nessie.risk_analytics.customer_canonical",
            {"name": "customer_canonical", "keys": ["customer_id", "as_of_date"]},
        )

        self.assertEqual(frame.writer.overwrite_partition_calls, 1)
        self.assertEqual(frame.writer.append_calls, 0)

    def test_expression_builds_map_from_json_object(self) -> None:
        module = self._import_expressions_module()

        expression = module.build_expression(
            {
                "op": "map_from_json",
                "args": [
                    {"col": "product_attributes"},
                ],
            },
            {},
        )

        self.assertIsNotNone(expression)

    def test_preview_pipeline_yaml_renders_runtime_parameters(self) -> None:
        module = self._import_module()
        with tempfile.TemporaryDirectory() as temp_dir:
            pipeline_path = f"{temp_dir}/preview.yaml"
            with open(pipeline_path, "w", encoding="utf-8") as handle:
                handle.write(
                    """
version: 1
name: preview_case
sources:
  - name: in
    type: table
    table: nessie.risk_analytics.customer_canonical
    filters:
      - as_of_date = '{{as_of_date}}'
steps:
  - id: noop
    type: reformat
    input: in
    select:
      - customer_id
    emit:
      output: out
targets:
  - name: out_target
    dataset: out
    table: nessie.risk_analytics.customer_sourcea_stg
    mode: append
""".strip()
                )

            preview = module.preview_pipeline_yaml(pipeline_path, {"as_of_date": "2026-07-18"})
            self.assertEqual(preview["summary"]["name"], "preview_case")
            rendered_filter = preview["rendered"]["sources"][0]["filters"][0]
            self.assertIn("2026-07-18", rendered_filter)

    def test_preview_pipeline_yaml_fails_when_templates_unresolved(self) -> None:
        module = self._import_module()
        with tempfile.TemporaryDirectory() as temp_dir:
            pipeline_path = f"{temp_dir}/preview_invalid.yaml"
            with open(pipeline_path, "w", encoding="utf-8") as handle:
                handle.write(
                    """
version: 1
name: preview_invalid
sources:
  - name: in
    type: table
    table: nessie.risk_analytics.customer_canonical
    filters:
      - as_of_date = '{{as_of_date}}'
steps:
  - id: noop
    type: reformat
    input: in
    select:
      - customer_id
    emit:
      output: out
targets:
  - name: out_target
    dataset: out
    table: nessie.risk_analytics.customer_sourcea_stg
    mode: append
""".strip()
                )

            with self.assertRaises(module.PipelineValidationError):
                module.preview_pipeline_yaml(pipeline_path, {})
