from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from risk_analytics.config import load_config


class LoadConfigTests(unittest.TestCase):
    def test_load_config_applies_environment_overrides(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "NESSIE_URI": "http://override:19120/api/v2",
                "S3_ENDPOINT": "http://override:8333",
            },
            clear=False,
        ):
            config = load_config()

        self.assertEqual(config["catalog"]["nessie_uri"], "http://override:19120/api/v2")
        self.assertEqual(config["storage"]["endpoint"], "http://override:8333")
        self.assertEqual(config["catalog"]["warehouse"], "s3a://risk-analytics-lakehouse/warehouse")
        self.assertEqual(config["risk"]["default_volatility"], 0.15)
        self.assertTrue(Path(config["executor"]["pipeline_path"]).is_absolute())
        self.assertTrue(config["executor"]["pipeline_path"].endswith("transform/risk_metrics_pipeline.yaml"))

    def test_load_config_allows_pipeline_path_override(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "RISK_PIPELINE_YAML": "transform/custom_pipeline.yaml",
            },
            clear=False,
        ):
            config = load_config()

        self.assertTrue(Path(config["executor"]["pipeline_path"]).is_absolute())
        self.assertTrue(config["executor"]["pipeline_path"].endswith("transform/custom_pipeline.yaml"))

