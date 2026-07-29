from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from risk_analytics.nessie import NessieClient


class DummyResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self._payload


class NessieClientTests(unittest.TestCase):
    def test_branch_exists_uses_references_endpoint(self) -> None:
        response = DummyResponse({"references": [{"name": "main"}, {"name": "feature"}]})

        with patch("risk_analytics.nessie.requests.get", return_value=response) as get_mock:
            client = NessieClient("http://nessie:19120/api/v2/")

            self.assertTrue(client.branch_exists("feature"))
            self.assertFalse(client.branch_exists("missing"))
            self.assertEqual(get_mock.call_count, 2)
            get_mock.assert_has_calls(
                [
                    unittest.mock.call("http://nessie:19120/api/v2/trees", timeout=10),
                    unittest.mock.call("http://nessie:19120/api/v2/trees", timeout=10),
                ]
            )

    def test_create_branch_and_merge_send_expected_requests(self) -> None:
        source_response = DummyResponse({"reference": {"hash": "abc123"}})
        post_response = DummyResponse({})
        get_mock = Mock(return_value=source_response)
        post_mock = Mock(return_value=post_response)

        with patch("risk_analytics.nessie.requests.get", get_mock), patch("risk_analytics.nessie.requests.post", post_mock):
            client = NessieClient("http://nessie:19120/api/v2")
            client.create_branch("feature")
            client.merge("feature", "main")

        get_mock.assert_called_once_with("http://nessie:19120/api/v2/trees/main", timeout=10)
        post_mock.assert_any_call(
            "http://nessie:19120/api/v2/trees/branch",
            json={"type": "BRANCH", "name": "feature", "hash": "abc123"},
            timeout=10,
        )
        post_mock.assert_any_call(
            "http://nessie:19120/api/v2/trees/branch/main/merge",
            json={"fromRefName": "feature"},
            timeout=20,
        )

