from __future__ import annotations

import json
import unittest

from risk_analytics.kafka_events import build_trigger_payload, match_entity_event


class FakeMessage:
    def __init__(self, value: bytes | None) -> None:
        self._value = value

    def value(self) -> bytes | None:
        return self._value


def _message(payload: dict[str, object]) -> FakeMessage:
    return FakeMessage(json.dumps(payload).encode("utf-8"))


class KafkaEventMatchingTests(unittest.TestCase):
    def test_matching_entity_returns_payload(self) -> None:
        message = _message({"entity": "customer", "as_of_date": "2026-07-18", "source": "kafka-entity-stream"})

        self.assertEqual(
            match_entity_event(message, entity="customer"),
            {"as_of_date": "2026-07-18", "entity": "customer", "source": "kafka-entity-stream"},
        )

    def test_other_entities_are_ignored(self) -> None:
        message = _message({"entity": "asset", "as_of_date": "2026-07-18"})

        self.assertIsNone(match_entity_event(message, entity="customer"))

    def test_event_without_entity_filter_matches_any_entity(self) -> None:
        message = _message({"entity": "deals", "as_of_date": "2026-07-18"})

        matched = match_entity_event(message)

        self.assertIsNotNone(matched)
        self.assertEqual(matched["entity"], "deals")

    def test_unusable_messages_keep_the_sensor_waiting(self) -> None:
        for message in (
            FakeMessage(None),
            FakeMessage(b"not json"),
            FakeMessage(b"[]"),
            _message({"entity": "customer"}),
        ):
            self.assertIsNone(match_entity_event(message, entity="customer"))

    def test_payload_roundtrip(self) -> None:
        payload = json.loads(build_trigger_payload("collateral", "2026-07-18"))

        self.assertEqual(payload["entity"], "collateral")
        self.assertEqual(payload["as_of_date"], "2026-07-18")
        self.assertEqual(payload["source"], "kafka-entity-stream")


if __name__ == "__main__":
    unittest.main()
