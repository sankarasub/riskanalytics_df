"""Kafka trigger payloads shared by the streaming job and the Airflow sensors.

``AwaitMessageSensor`` resolves ``apply_function`` with ``import_string`` inside the
triggerer process, so the matcher has to live in an importable module rather than in
a DAG file.
"""
from __future__ import annotations

import json
from typing import Protocol

TRIGGER_TOPIC = "risk.pipeline.trigger"
STREAM_SOURCE = "kafka-entity-stream"


class KafkaMessage(Protocol):
    """Subset of the confluent-kafka message API used by the matcher."""

    def value(self) -> bytes | None:
        ...


def build_trigger_payload(entity: str, as_of_date: str, source: str = STREAM_SOURCE) -> str:
    """Serialize the micro-batch notification published to ``risk.pipeline.trigger``."""
    return json.dumps({"entity": entity, "as_of_date": as_of_date, "source": source})


def match_entity_event(message: KafkaMessage, entity: str | None = None) -> dict[str, str] | None:
    """Return the payload of a micro-batch event, or ``None`` to keep waiting.

    Returning the payload instead of a Boolean lets downstream tasks reuse the
    business date carried by the Kafka batch rather than an Airflow timestamp.
    Events for other entities are ignored so one topic can drive every entity DAG.
    """
    raw = message.value()
    if raw is None:
        return None
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None

    as_of_date = payload.get("as_of_date")
    if not as_of_date:
        return None

    payload_entity = str(payload.get("entity", "")).lower()
    if entity is not None and payload_entity != entity.lower():
        return None

    return {
        "as_of_date": str(as_of_date),
        "entity": payload_entity or str(entity or ""),
        "source": str(payload.get("source", STREAM_SOURCE)),
    }
