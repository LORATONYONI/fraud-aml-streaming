"""Topic names, client settings, JSON encoding and reliable produce helpers."""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TRANSACTIONS_TOPIC = "transactions"
ALERTS_TOPIC = "alerts"
DLQ_TOPIC = "transactions.dlq"
DETECTOR_GROUP = "fraud-aml-detector"


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    config: dict[str, str]


# Topics as code. Transactions are keyed by origin account, so each account's transactions stay
# in order on one partition, and three partitions let up to three detectors share the work.
TOPICS = (
    TopicSpec(TRANSACTIONS_TOPIC, 3, {"retention.ms": str(7 * 24 * 3600 * 1000)}),
    TopicSpec(ALERTS_TOPIC, 3, {"retention.ms": str(30 * 24 * 3600 * 1000)}),
    TopicSpec(DLQ_TOPIC, 1, {"retention.ms": str(30 * 24 * 3600 * 1000)}),
)


def producer_config(bootstrap: str, client_id: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": bootstrap,
        "client.id": client_id,
        # Idempotent producer: broker-side de-duplication of retries, ordering kept per partition.
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 20,
        "compression.type": "lz4",
    }


def consumer_config(bootstrap: str, group_id: str, client_id: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "client.id": client_id,
        # Offsets are committed by hand, only after a batch's results are safely written.
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        # Fetch small chunks from each partition so partitions interleave finely. When a backlog
        # is replayed this keeps them within minutes of each other in event time, well inside
        # RuleConfig.allowed_lateness_s (the default 1 MB chunks let one run hours ahead).
        "max.partition.fetch.bytes": 64 * 1024,
    }


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")  # money as an exact string, never a float
    raise TypeError(f"cannot serialise {type(value).__name__}")


def encode(record: Mapping[str, Any]) -> bytes:
    return json.dumps(record, default=_json_default, separators=(",", ":"), sort_keys=True).encode()


def decode(value: bytes | None) -> dict[str, Any]:
    if not value:
        raise ValueError("empty message")
    record = json.loads(value)
    if not isinstance(record, dict):
        raise ValueError("message is not a JSON object")
    return record


class DeliveryError(RuntimeError):
    """The broker did not confirm every message."""


class DeliveryTracker:
    """Delivery-report callback that remembers failures so the caller can refuse to commit."""

    def __init__(self) -> None:
        self.failures: list[str] = []

    def __call__(self, err: Any, msg: Any) -> None:
        if err is not None:
            self.failures.append(str(err))

    def raise_if_failed(self) -> None:
        if self.failures:
            summary = f"{len(self.failures)} message(s) not delivered, first error: {self.failures[0]}"
            self.failures.clear()
            raise DeliveryError(summary)


def produce(
    producer: Any,
    topic: str,
    key: bytes | None,
    value: bytes,
    on_delivery: DeliveryTracker,
    headers: list[tuple[str, bytes]] | None = None,
) -> None:
    while True:
        try:
            producer.produce(topic, key=key, value=value, headers=headers, on_delivery=on_delivery)
            break
        except BufferError:  # local queue full: let the client send, then try again
            producer.poll(0.5)
    producer.poll(0)


def flush_or_raise(producer: Any, tracker: DeliveryTracker, timeout_s: float) -> None:
    remaining = producer.flush(timeout_s)
    if remaining:
        raise DeliveryError(f"{remaining} message(s) still unconfirmed after {timeout_s}s")
    tracker.raise_if_failed()
