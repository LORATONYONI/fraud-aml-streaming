"""Where the producer sends transactions."""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Protocol

from fraud_aml.engine import DetectionEngine
from fraud_aml.models import GroundTruth, Transaction


class Sink(Protocol):
    def send(self, txn: Transaction, truth: GroundTruth) -> None: ...

    def close(self) -> None: ...


def to_item(txn: Transaction, truth: GroundTruth, expires_at: int) -> dict[str, Any]:
    return {**txn.to_record(), **truth.to_record(), "expires_at": expires_at}


class RateLimiter:
    """Simple pacing so the producer stays inside DynamoDB's free provisioned write capacity."""

    def __init__(self, per_second: float) -> None:
        self.interval = 1.0 / per_second if per_second > 0 else 0.0
        self.next_at = time.monotonic()

    def wait(self) -> None:
        if not self.interval:
            return
        now = time.monotonic()
        if self.next_at > now:
            time.sleep(self.next_at - now)
        self.next_at = max(self.next_at, now) + self.interval


class LocalPipelineSink:
    """Runs the real detection engine in-process with in-memory stores: no AWS needed."""

    def __init__(self, engine: DetectionEngine, batch_size: int = 100) -> None:
        self.engine = engine
        self.batch_size = batch_size
        self.buffer: list[Transaction] = []
        self.truth: dict[str, GroundTruth] = {}

    def send(self, txn: Transaction, truth: GroundTruth) -> None:
        self.truth[txn.txn_id] = truth
        self.buffer.append(txn)
        if len(self.buffer) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if self.buffer:
            self.engine.process(self.buffer)
            self.buffer = []

    def close(self) -> None:
        self.flush()


class DynamoDBSink:
    def __init__(self, table_name: str, rate: float, ttl_days: int, region: str | None = None) -> None:
        import boto3

        self.table = boto3.resource("dynamodb", region_name=region).Table(table_name)
        self.writer = self.table.batch_writer()
        self.limiter = RateLimiter(rate)
        self.ttl_s = int(timedelta(days=ttl_days).total_seconds())

    def send(self, txn: Transaction, truth: GroundTruth) -> None:
        self.limiter.wait()
        expires_at = int(datetime.now(timezone.utc).timestamp()) + self.ttl_s
        self.writer.put_item(Item=to_item(txn, truth, expires_at))

    def close(self) -> None:
        self.writer.__exit__(None, None, None)  # flush the final partial batch


class StdoutSink:
    def __init__(self, rate: float = 0) -> None:
        self.limiter = RateLimiter(rate)

    def send(self, txn: Transaction, truth: GroundTruth) -> None:
        self.limiter.wait()
        item = to_item(txn, truth, expires_at=0)
        sys.stdout.write(json.dumps(item, default=lambda v: str(v) if isinstance(v, Decimal) else v) + "\n")

    def close(self) -> None:
        sys.stdout.flush()


class KafkaSink:
    """Publishes to a Kafka topic, keyed by origin account so each account's events stay in order."""

    def __init__(self, bootstrap: str, topic: str, rate: float = 0, producer: Any = None) -> None:
        from fraud_aml.kafka.common import DeliveryTracker, producer_config

        if producer is None:
            from confluent_kafka import Producer

            from fraud_aml.kafka.topics import ensure_topics

            ensure_topics(bootstrap)
            producer = Producer(producer_config(bootstrap, "fraud-aml-producer"))
        self.producer = producer
        self.topic = topic
        self.limiter = RateLimiter(rate)
        self.tracker = DeliveryTracker()

    def send(self, txn: Transaction, truth: GroundTruth) -> None:
        from fraud_aml.kafka.common import encode, produce

        self.limiter.wait()
        record = {**txn.to_record(), **truth.to_record()}
        produce(self.producer, self.topic, txn.orig_account.encode(), encode(record), self.tracker)

    def close(self) -> None:
        from fraud_aml.kafka.common import flush_or_raise

        flush_or_raise(self.producer, self.tracker, timeout_s=60)
