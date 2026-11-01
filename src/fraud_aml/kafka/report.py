"""Score the alerts in Kafka against the ground truth: ``python -m fraud_aml.kafka.report``.

An offline evaluation job: it reads both topics from the beginning, without joining the detector's
consumer group, so it never moves the detector's offsets.
"""

from __future__ import annotations

import argparse
import sys
import time
import uuid
from collections.abc import Iterable
from typing import Any

from fraud_aml.evaluation import Report, evaluate, format_report
from fraud_aml.kafka.common import ALERTS_TOPIC, DEFAULT_BOOTSTRAP, TRANSACTIONS_TOPIC, consumer_config, decode
from fraud_aml.models import Alert, GroundTruth


def build_report(transaction_values: Iterable[bytes], alert_values: Iterable[bytes]) -> Report:
    truth: dict[str, GroundTruth] = {}
    for value in transaction_values:
        try:
            record = decode(value)
            truth[str(record["txn_id"])] = GroundTruth.from_record(record)
        except (ValueError, KeyError):
            continue  # malformed messages are the DLQ's business, not the score's
    alerts: dict[tuple[str, str], Alert] = {}
    for value in alert_values:
        record = decode(value)
        alerts[(record["account_id"], record["alert_key"])] = Alert.from_record(record)  # at-least-once: dedupe
    return evaluate(truth, alerts.values())


def read_topic(consumer: Any, topic: str, timeout_s: float = 60) -> list[bytes]:
    """Read every message currently in ``topic``, from the oldest to the high watermark."""
    from confluent_kafka import TopicPartition

    metadata = consumer.list_topics(topic, timeout=10).topics.get(topic)
    if metadata is None or metadata.error is not None:
        raise RuntimeError(f"topic '{topic}' not found. Did you run the producer?")
    ends: dict[int, int] = {}
    start = []
    for partition in metadata.partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
        if high > low:
            ends[partition] = high
            start.append(TopicPartition(topic, partition, low))
    if not start:
        return []
    consumer.assign(start)
    values: list[bytes] = []
    finished: set[int] = set()
    deadline = time.monotonic() + timeout_s
    while len(finished) < len(ends):
        if time.monotonic() > deadline:
            raise TimeoutError(f"gave up reading '{topic}' after {timeout_s}s")
        for msg in consumer.consume(num_messages=1000, timeout=1.0):
            if msg.error() is not None:
                continue
            values.append(msg.value())
            if msg.offset() + 1 >= ends[msg.partition()]:
                finished.add(msg.partition())
    consumer.unassign()
    return values


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m fraud_aml.kafka.report", description=__doc__.split("\n")[0])
    p.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    p.add_argument("--min-recall", type=float, default=None, help="exit 1 if any scenario's recall is below this")
    args = p.parse_args(argv)

    from confluent_kafka import Consumer

    consumer = Consumer(consumer_config(args.bootstrap, f"fraud-aml-report-{uuid.uuid4()}", "fraud-aml-report"))
    try:
        transactions = read_topic(consumer, TRANSACTIONS_TOPIC)
        alerts = read_topic(consumer, ALERTS_TOPIC)
    finally:
        consumer.close()

    report = build_report(transactions, alerts)
    print(f"Read {len(transactions):,} transaction messages and {len(alerts):,} alert messages.", file=sys.stderr)
    print(format_report(report))
    if args.min_recall is not None:
        failing = {n: s.recall for n, s in report.scenarios.items() if s.recall < args.min_recall}
        if failing or not report.scenarios:
            detail = failing or "no scenarios"
            print(f"\nQuality gate FAILED (min recall {args.min_recall:.0%}): {detail}", file=sys.stderr)
            return 1
        print(f"\nQuality gate passed: every scenario recall >= {args.min_recall:.0%}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
