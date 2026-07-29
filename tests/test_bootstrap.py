from __future__ import annotations

import importlib
import sys
import unittest
from unittest.mock import patch

from tests.support import build_fake_pyspark


class FakeTableProbe:
    def limit(self, _count: int) -> "FakeTableProbe":
        return self

    def count(self) -> int:
        return 0


class FakeWriter:
    def __init__(self, spark: "FakeSpark", table_name: str) -> None:
        self.spark = spark
        self.table_name = table_name

    def append(self) -> None:
        self.spark.written_tables.append(self.table_name)


class FakeFrame:
    def __init__(self, spark: "FakeSpark", rows: list[dict], schema) -> None:
        self.spark = spark
        self.rows = rows
        self.schema = schema

    def writeTo(self, table_name: str) -> FakeWriter:
        return FakeWriter(self.spark, table_name)


class FakeSpark:
    def __init__(self) -> None:
        self.sql_statements: list[str] = []
        self.created_frames: list[FakeFrame] = []
        self.requested_tables: list[str] = []
        self.written_tables: list[str] = []
        self.stopped = False

    def sql(self, statement: str) -> None:
        self.sql_statements.append(statement)

    def table(self, table_name: str) -> FakeTableProbe:
        self.requested_tables.append(table_name)
        return FakeTableProbe()

    def createDataFrame(self, rows, schema=None) -> FakeFrame:
        frame = FakeFrame(self, list(rows), schema)
        self.created_frames.append(frame)
        return frame

    def stop(self) -> None:
        self.stopped = True


class BootstrapJobTests(unittest.TestCase):
    def _import_module(self):
        fake_modules, _builder = build_fake_pyspark()
        sys.modules.pop("jobs.bootstrap", None)

        with patch.dict(sys.modules, fake_modules, clear=False):
            module = importlib.import_module("jobs.bootstrap")

        return module

    def test_create_all_tables_emits_namespace_and_all_table_ddl(self) -> None:
        module = self._import_module()
        spark = FakeSpark()

        module.create_all_tables(spark)

        self.assertEqual(spark.sql_statements[0], "CREATE NAMESPACE IF NOT EXISTS nessie.risk_analytics")
        expected_count = (
            1
            + len(module.SOURCE_TABLES)
            + len(module.STAGING_TABLES)
            + len(module.CANONICAL_TABLES)
            + 1
            + len(module.SOURCE_TO_ODS_CREATE_ORDER)
        )
        self.assertEqual(len(spark.sql_statements), expected_count)
        self.assertTrue(
            any("CREATE TABLE IF NOT EXISTS nessie.risk_analytics.risk_metrics" in stmt for stmt in spark.sql_statements)
        )
        self.assertTrue(
            any("CREATE TABLE IF NOT EXISTS nessie.risk_analytics_ods.risk_metrics" in stmt for stmt in spark.sql_statements)
        )
        self.assertTrue(spark.stopped is False)

    def test_seed_table_writes_customer_rows_when_table_is_empty(self) -> None:
        module = self._import_module()
        spark = FakeSpark()

        module.seed_table(spark, "customer", module.DEFAULT_AS_OF_DATE)

        self.assertEqual(spark.requested_tables, ["nessie.risk_analytics.customer"])
        self.assertEqual(spark.written_tables, ["nessie.risk_analytics.customer"])
        self.assertEqual(len(spark.created_frames), 1)
        self.assertEqual(len(spark.created_frames[0].rows), 2)

    def test_seed_table_rejects_non_seed_tables(self) -> None:
        module = self._import_module()
        spark = FakeSpark()

        with self.assertRaises(ValueError):
            module.seed_table(spark, "risk_metrics", module.DEFAULT_AS_OF_DATE)

