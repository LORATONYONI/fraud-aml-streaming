"""The detector as a Kafka consumer: ``python -m fraud_aml.kafka.detector``.

Per batch: consume, run the rules, publish alerts, then commit offsets. This gives at-least-once
processing. The SQLite state store's de-duplication makes the *results* effectively-once: after a
crash or rebalance, re-read transactions are recognised and skipped.

Why not Kafka transactions (exactly-once semantics)? They only cover state kept *inside* Kafka.
Our account state lives in an external store, so idempotent processing is the honest guarantee.
"""

from __future__ import annotations

import argparse
import contextlib
import logging
import signal
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import InvalidOperation
from pathlib import Path
from typing import Any

from fraud_aml.config import RuleConfig
from fraud_aml.engine import AlertSink, BatchResult, DetectionEngine
from fraud_aml.kafka.common import (
    ALERTS_TOPIC,
    DEFAULT_BOOTSTRAP,
    DETECTOR_GROUP,
    DLQ_TOPIC,
    TRANSACTIONS_TOPIC,
    DeliveryTracker,
    consumer_config,
    decode,
    encode,
    flush_or_raise,
    produce,
    producer_config,
)
from fraud_aml.models import Alert, Transaction

logger = logging.getLogger(__name__)


class KafkaAlertSink:
    """Stores alerts in ``inner`` (which de-duplicates) and publishes them to the alerts topic.

    The engine calls this *before* saving account state. Every alert of the batch is published, not
    only new ones: if the process dies after publishing but before the state is saved, the replay
    publishes them again (at-least-once). Consumers de-duplicate on ``alert_key``.
    """

    def __init__(self, inner: AlertSink, producer: Any, topic: str = ALERTS_TOPIC, flush_timeout_s: float = 30):
        self.inner = inner
        self.producer = producer
        self.topic = topic
        self.flush_timeout_s = flush_timeout_s
        self.tracker = DeliveryTracker()

    def put_new(self, alerts: Sequence[Alert], detected_at: datetime, expires_at: int) -> list[Alert]:
        created = self.inner.put_new(alerts, detected_at, expires_at)
        for alert in alerts:
            value = encode(alert.to_record(detected_at))
            produce(self.producer, self.topic, alert.account_id.encode(), value, self.tracker)
        if alerts:
            flush_or_raise(self.producer, self.tracker, self.flush_timeout_s)
        return created


@dataclass
class Totals:
    batches: int = 0
    transactions: int = 0
    duplicates: int = 0
    rejected: int = 0
    new_alerts: int = 0


class KafkaDetector:
    def __init__(
        self,
        consumer: Any,
        producer: Any,
        engine: DetectionEngine,
        *,
        dlq_topic: str = DLQ_TOPIC,
        batch_size: int = 500,
        poll_timeout_s: float = 1.0,
        flush_timeout_s: float = 30,
        batch_lock: Callable[[], contextlib.AbstractContextManager[Any]] = contextlib.nullcontext,
    ) -> None:
        self.consumer = consumer
        self.producer = producer
        self.engine = engine
        self.dlq_topic = dlq_topic
        self.batch_size = batch_size
        self.poll_timeout_s = poll_timeout_s
        self.flush_timeout_s = flush_timeout_s
        self.batch_lock = batch_lock  # serialises batches across processes sharing one state store
        self.tracker = DeliveryTracker()
        self.stopping = False
        self.assigned_at: float | None = None

    def on_assign(self, _consumer: Any, partitions: list[Any]) -> None:
        """Rebalance callback: joining a consumer group takes a few seconds before data flows."""
        self.assigned_at = time.monotonic()
        print(f"assigned partitions: {sorted(p.partition for p in partitions)}", file=sys.stderr)

    def on_revoke(self, _consumer: Any, partitions: list[Any]) -> None:
        print(f"revoked partitions: {sorted(p.partition for p in partitions)}", file=sys.stderr)

    def run_once(self) -> dict[str, Any] | None:
        """Process one batch. Returns its summary, or None if no messages arrived."""
        messages = self.consumer.consume(num_messages=self.batch_size, timeout=self.poll_timeout_s)
        if not messages:
            return None

        txns: list[Transaction] = []
        rejected = 0
        for msg in messages:
            error = msg.error()
            if error is not None:
                if getattr(error, "fatal", lambda: False)():
                    raise RuntimeError(f"fatal consumer error: {error}")
                logger.warning("consumer error: %s", error)
                continue
            try:
                txns.append(Transaction.from_record(decode(msg.value())))
            except (ValueError, KeyError, TypeError, InvalidOperation) as exc:
                # Poison message: park it in the dead-letter topic with its origin, keep going.
                rejected += 1
                headers = [
                    ("error", repr(exc).encode()),
                    ("source", f"{msg.topic()}/{msg.partition()}/{msg.offset()}".encode()),
                ]
                produce(self.producer, self.dlq_topic, msg.key(), msg.value() or b"", self.tracker, headers)

        with self.batch_lock():
            result = self.engine.process(txns) if txns else BatchResult()
        if rejected:
            flush_or_raise(self.producer, self.tracker, self.flush_timeout_s)
        # Only now is everything from this batch safely written: commit the offsets.
        self.consumer.commit(asynchronous=False)
        return {**result.summary(), "rejected": rejected}

    def run(self, max_idle_s: float | None = None, assignment_timeout_s: float = 60) -> Totals:
        """Consume until stopped (Ctrl+C / SIGTERM) or until idle for ``max_idle_s`` seconds.

        Idle time only counts once partitions are assigned, so a slow group join is not mistaken
        for an empty topic.
        """
        totals = Totals()
        started = time.monotonic()
        last_activity: float | None = None
        while not self.stopping:
            summary = self.run_once()
            now = time.monotonic()
            if summary is None:
                if max_idle_s is None:
                    continue
                if self.assigned_at is None:
                    if now - started >= assignment_timeout_s:
                        print(f"no partitions assigned after {assignment_timeout_s:.0f}s, giving up", file=sys.stderr)
                        break
                    continue
                if now - max(self.assigned_at, last_activity or 0) >= max_idle_s:
                    break
                continue
            last_activity = now
            totals.batches += 1
            totals.transactions += summary["processed"]
            totals.duplicates += summary["duplicates"]
            totals.rejected += summary["rejected"]
            totals.new_alerts += summary["new_alerts"]
            rules = ", ".join(f"{k}={v}" for k, v in sorted(summary["new_alerts_by_rule"].items()))
            line = (
                f"batch {totals.batches:>4}: {summary['received']:>4} txns, {summary['duplicates']} duplicates, "
                f"{summary['rejected']} rejected, {summary['new_alerts']} new alerts"
            )
            print(line + (f" ({rules})" if rules else ""), file=sys.stderr)
        return totals


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="python -m fraud_aml.kafka.detector", description=__doc__.split("\n")[0])
    p.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    p.add_argument("--group", default=DETECTOR_GROUP, help="consumer group (run several detectors to share partitions)")
    p.add_argument("--topic", default=TRANSACTIONS_TOPIC)
    p.add_argument("--alerts-topic", default=ALERTS_TOPIC)
    p.add_argument("--dlq-topic", default=DLQ_TOPIC)
    p.add_argument("--state-db", type=Path, default=Path(".local/detector.db"), help="SQLite state store")
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--max-idle-s", type=float, default=None, help="exit after this many idle seconds")
    p.add_argument("--rule-overrides", default=None, help="JSON threshold overrides")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    from confluent_kafka import Consumer, Producer

    from fraud_aml.kafka.topics import ensure_topics
    from fraud_aml.sqlite_store import SqliteAlertSink, SqliteStateStore, connect, transaction

    ensure_topics(args.bootstrap)
    conn = connect(args.state_db)
    producer = Producer(producer_config(args.bootstrap, "fraud-aml-detector"))
    consumer = Consumer(consumer_config(args.bootstrap, args.group, "fraud-aml-detector"))
    engine = DetectionEngine(
        RuleConfig.from_json(args.rule_overrides),
        SqliteStateStore(conn),
        KafkaAlertSink(SqliteAlertSink(conn), producer, args.alerts_topic),
    )
    detector = KafkaDetector(
        consumer,
        producer,
        engine,
        dlq_topic=args.dlq_topic,
        batch_size=args.batch_size,
        batch_lock=lambda: transaction(conn),
    )
    consumer.subscribe([args.topic], on_assign=detector.on_assign, on_revoke=detector.on_revoke)

    def stop(signum: int, _frame: Any) -> None:
        detector.stopping = True

    signal.signal(signal.SIGTERM, stop)
    print(
        f"Detector consuming '{args.topic}' from {args.bootstrap} as group '{args.group}'. "
        f"State: {args.state_db}. Ctrl+C to stop.",
        file=sys.stderr,
    )
    totals = Totals()
    try:
        totals = detector.run(max_idle_s=args.max_idle_s)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()  # leave the group cleanly so partitions are reassigned at once
        producer.flush(10)
        conn.close()
    print(
        f"Stopped. {totals.batches} batches, {totals.transactions} transactions, {totals.duplicates} duplicates, "
        f"{totals.rejected} rejected, {totals.new_alerts} new alerts.",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
