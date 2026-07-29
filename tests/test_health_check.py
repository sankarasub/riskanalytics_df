from __future__ import annotations

import unittest
from unittest.mock import patch

import requests

from scripts.health_check import HttpCheck, run_http_check


class HealthCheckTests(unittest.TestCase):
    def test_optional_http_check_returns_warn_on_connection_error(self) -> None:
        check = HttpCheck(name="developer-ui", url="http://localhost:8502/_stcore/health", expected_statuses={200}, required=False)

        with patch("scripts.health_check.requests.get", side_effect=requests.exceptions.ConnectionError("boom")):
            result = run_http_check(check, timeout=0.01)

        self.assertEqual(result.status, "WARN")
        self.assertIn("optional", result.details.lower())
