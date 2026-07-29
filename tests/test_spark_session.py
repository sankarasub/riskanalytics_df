from __future__ import annotations

import importlib
import sys
import unittest
from unittest.mock import patch

from tests.support import build_fake_pyspark


class SparkSessionSelectionTests(unittest.TestCase):
    def _import_module(self):
        fake_modules, builder = build_fake_pyspark()
        sys.modules.pop("risk_analytics.spark", None)

        with patch.dict(sys.modules, fake_modules, clear=False):
            module = importlib.import_module("risk_analytics.spark")

        return module, builder

    def test_remote_session_uses_spark_connect(self) -> None:
        module, builder = self._import_module()

        with patch.object(module, "load_config", return_value={"catalog": {}, "storage": {}}), patch.dict(
            "os.environ",
            {"SPARK_REMOTE": "sc://spark-connect:15002"},
            clear=False,
        ):
            session = module.create_spark_session("risk-analytics-notebook")

        self.assertEqual(builder.app_name, "risk-analytics-notebook")
        self.assertEqual(builder.remote_url, "sc://spark-connect:15002")
        self.assertTrue(builder.get_or_create_called)
        self.assertEqual(session["remote_url"], "sc://spark-connect:15002")
        self.assertEqual(builder.configs, [])

    def test_local_session_sets_iceberg_and_nessie_configuration(self) -> None:
        module, builder = self._import_module()

        config = {
            "catalog": {"nessie_uri": "http://nessie:19120/api/v2", "warehouse": "s3a://warehouse"},
            "storage": {"endpoint": "http://seaweedfs:8333"},
        }

        with patch.object(module, "load_config", return_value=config), patch.dict("os.environ", {}, clear=True):
            session = module.create_spark_session("risk-analytics-job", ref="feature-branch")

        self.assertEqual(builder.app_name, "risk-analytics-job")
        self.assertIsNone(builder.remote_url)
        self.assertTrue(builder.get_or_create_called)
        self.assertEqual(session["app_name"], "risk-analytics-job")
        self.assertNotIn("spark.jars.packages", [key for key, _value in builder.configs])
        self.assertIn(("spark.sql.catalog.nessie.ref", "feature-branch"), builder.configs)
        self.assertIn(("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v2"), builder.configs)
        self.assertIn(("spark.hadoop.fs.s3a.endpoint", "http://seaweedfs:8333"), builder.configs)

