"""Kafka mode with in-memory fakes of the confluent-kafka Consumer and Producer (no broker needed).

The fakes mimic only what the code uses: consume/commit, produce/poll/flush with delivery reports.
"""

from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest

from fraud_aml.config import RuleConfig
from fraud_aml.engine import ConflictError, DetectionEngine
from fraud_aml.kafka.common import ALERTS_TOPIC, DLQ_TOPIC, DeliveryError, decode, encode
from fraud_aml.kafka.detector import KafkaAlertSink, KafkaDetector
from fraud_aml.kafka.report import build_report
from fraud_aml.models import NOT_SUSPICIOUS, GroundTruth
from fraud_aml.producer.sinks import KafkaSink
from fraud_aml.sqlite_store import SqliteAlertSink, SqliteStateStore, connect, transaction
from helpers import txn


class FakeMessage:
    def __init__(self, value: bytes, key: bytes = b"k", offset: int = 0):
        self._value, self._key, self._offset = value, key, offset

    def error(self):
        return None

    def value(self):
        return self._value

    def key(self):
        return self._key

    def topic(self):
        return "transactions"

    def partition(self):
        return 0

    def offset(self):
        return self._offset


class FakeConsumer:
    def __init__(self, batches):
        self.batches = list(batches)
        self.commits = 0

    def consume(self, num_messages, timeout):
        return self.batches.pop(0) if self.batches else []

    def commit(self, asynchronous=True):
        self.commits += 1


class FakeProducer:
    def __init__(self, fail_topics=()):
        self.messages = []
        self.pending = []
        self.fail_topics = set(fail_topics)

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        msg = SimpleNamespace(topic=topic, key=key, value=value, headers=headers)
        self.messages.append(msg)
        self.pending.append((msg, on_delivery))

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=None):
        for msg, callback in self.pending:
            if callback:
                callback("broker unavailable" if msg.topic in self.fail_topics else None, msg)
        self.pending.clear()
        return 0

    def on(self, topic):
        return [m for m in self.messages if m.topic == topic]


def message_for(t, truth=NOT_SUSPICIOUS, offset=0):
    return FakeMessage(encode({**t.to_record(), **truth.to_record()}), t.orig_account.encode(), offset)


def structuring_messages():
    amounts = ((0, "9500"), (30, "9600"), (60, "9700"))
    deposits = [txn("CASH_IN", a, orig="C1", dest="C9", minutes=m) for m, a in amounts]
    return [message_for(t, GroundTruth(True, "structuring", "s1"), i) for i, t in enumerate(deposits)]


def make_detector(batches, producer=None, conn=None):
    producer = producer or FakeProducer()
    conn = conn or connect(":memory:")
    engine = DetectionEngine(
        RuleConfig(), SqliteStateStore(conn), KafkaAlertSink(SqliteAlertSink(conn), producer, ALERTS_TOPIC)
    )
    consumer = FakeConsumer(batches)
    return KafkaDetector(consumer, producer, engine, batch_lock=lambda: transaction(conn)), consumer, producer


def test_producer_sink_keys_by_account_and_keeps_labels():
    producer = FakeProducer()
    sink = KafkaSink("unused:9092", "transactions", producer=producer)
    sink.send(txn("TRANSFER", "250.50", orig="C7", dest="C8"), GroundTruth(True, "mule_network", "m1"))
    sink.close()
    (msg,) = producer.messages
    record = decode(msg.value)
    assert msg.key == b"C7"
    assert record["amount"] == "250.50"  # exact string, not a float
    assert record["label_scenario"] == "mule_network"


def test_producer_sink_raises_if_broker_rejects_messages():
    sink = KafkaSink("unused:9092", "transactions", producer=FakeProducer(fail_topics={"transactions"}))
    sink.send(txn(), GroundTruth(False))
    with pytest.raises(DeliveryError):
        sink.close()


def test_detector_publishes_alerts_then_commits():
    detector, consumer, producer = make_detector([structuring_messages()])
    summary = detector.run_once()
    assert summary["new_alerts_by_rule"] == {"STRUCTURING": 1}
    (alert,) = producer.on(ALERTS_TOPIC)
    assert alert.key == b"C1"
    assert decode(alert.value)["rule_id"] == "STRUCTURING"
    assert consumer.commits == 1


def test_poison_message_goes_to_dead_letter_topic():
    bad = FakeMessage(b"not json", b"C1", offset=41)
    detector, consumer, producer = make_detector([[bad, *structuring_messages()]])
    summary = detector.run_once()
    assert summary["rejected"] == 1 and summary["processed"] == 3
    (dead,) = producer.on(DLQ_TOPIC)
    assert dead.value == b"not json"
    assert dict(dead.headers)["source"] == b"transactions/0/41"
    assert consumer.commits == 1


def test_offsets_are_not_committed_when_alerts_cannot_be_delivered():
    detector, consumer, _ = make_detector([structuring_messages()], producer=FakeProducer(fail_topics={ALERTS_TOPIC}))
    with pytest.raises(DeliveryError):
        detector.run_once()
    assert consumer.commits == 0  # the batch will be re-read after a restart


def test_redelivery_after_restart_is_deduplicated(tmp_path):
    db = tmp_path / "state.db"
    first = structuring_messages()
    detector, _, _ = make_detector([first[:2]], conn=connect(db))
    detector.run_once()

    # "Restart": new connection to the same file, and the broker re-delivers everything.
    detector, _, producer = make_detector([first], conn=connect(db))
    summary = detector.run_once()
    assert summary["duplicates"] == 2
    assert summary["new_alerts_by_rule"] == {"STRUCTURING": 1}  # state survived the restart


def test_idle_timeout_waits_for_partition_assignment():
    detector, _, _ = make_detector([])
    totals = detector.run(max_idle_s=0, assignment_timeout_s=0.05)
    assert totals.batches == 0  # gave up waiting for an assignment rather than exiting instantly
    detector.on_assign(None, [])
    assert detector.run(max_idle_s=0).batches == 0


def test_sqlite_store_optimistic_locking_and_nesting():
    conn = connect(":memory:")
    store = SqliteStateStore(conn)
    stale = store.load(["C1"])["C1"]
    fresh = store.load(["C1"])["C1"]
    store.save([fresh], expires_at=0)
    with pytest.raises(ConflictError):
        store.save([stale], expires_at=0)
    with transaction(conn):  # a nested save becomes a savepoint inside the batch lock
        store.save([store.load(["C2"])["C2"]], expires_at=0)
    assert store.load(["C2"])["C2"].version == 1


def test_two_connections_to_a_new_file_can_initialise(tmp_path):
    db = tmp_path / "shared.db"
    a, b = connect(db), connect(db)
    assert isinstance(a, sqlite3.Connection) and isinstance(b, sqlite3.Connection)


def test_report_deduplicates_at_least_once_alerts():
    deposits = structuring_messages()
    detector, _, producer = make_detector([deposits])
    detector.run_once()
    alert_values = [m.value for m in producer.on(ALERTS_TOPIC)] * 2  # published twice (replay)
    report = build_report([m.value() for m in deposits] + [b"garbage"], alert_values)
    assert report.rules["STRUCTURING"].alerts == 1
    assert report.scenarios["structuring"].recall == 1.0
