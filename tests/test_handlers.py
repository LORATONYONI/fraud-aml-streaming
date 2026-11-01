import gzip
import json
from datetime import datetime, timezone
from decimal import Decimal

from fraud_aml.config import RuleConfig
from fraud_aml.ddb_stream import deserialize, table_name_from_arn
from fraud_aml.engine import DetectionEngine
from fraud_aml.handlers import archiver, detector, notifier
from fraud_aml.models import Alert, GroundTruth
from fraud_aml.producer.sinks import to_item
from fraud_aml.stores import InMemoryAlertSink, InMemoryStateStore
from helpers import ALERTS_ARN, T0, TRANSACTIONS_ARN, stream_event, to_typed, txn

ENTITIES = {"fraud-aml-transactions": "transactions", "fraud-aml-alerts": "alerts"}


def items_for(*txns):
    return [to_item(t, GroundTruth(True, "structuring", "s1"), expires_at=1800000000) for t in txns]


def test_deserialize_handles_nested_types():
    assert deserialize(to_typed({"a": [Decimal("1.5"), "x", True, None]})) == {"a": [Decimal("1.5"), "x", True, None]}
    assert table_name_from_arn(TRANSACTIONS_ARN) == "fraud-aml-transactions"


def test_detector_handler_end_to_end():
    engine = DetectionEngine(RuleConfig(), InMemoryStateStore(), InMemoryAlertSink())
    deposits = [txn("CASH_IN", a, orig="C1", dest="C9", minutes=m) for m, a in ((0, "9500"), (5, "9600"), (9, "9700"))]
    event = stream_event(items_for(*deposits))
    event["Records"].append({"eventName": "REMOVE", "dynamodb": {}})  # TTL deletes are ignored
    summary = detector.handler(event, engine=engine)
    assert summary["processed"] == 3
    assert summary["new_alerts_by_rule"] == {"STRUCTURING": 1}
    assert summary["rejected"] == 0


def test_detector_skips_malformed_records_instead_of_blocking_the_shard():
    engine = DetectionEngine(RuleConfig(), InMemoryStateStore(), InMemoryAlertSink())
    good = items_for(txn("PAYMENT", "10"))[0]
    event = stream_event([good, {"txn_id": "broken"}])
    summary = detector.handler(event, engine=engine)
    assert summary["processed"] == 1
    assert summary["rejected"] == 1


class FakeWriter:
    def __init__(self):
        self.objects = {}

    def put(self, key, body):
        self.objects[key] = body


def read_rows(body: bytes):
    return [json.loads(line) for line in gzip.decompress(body).splitlines()]


def test_archiver_partitions_by_event_date_and_keeps_money_exact():
    day1 = txn("PAYMENT", "10.10")
    day2 = txn("PAYMENT", "20.20", minutes=24 * 60)
    writer = FakeWriter()
    summary = archiver.archive(stream_event(items_for(day1, day2)), writer, ENTITIES)
    assert summary["rows"] == 2
    keys = sorted(writer.objects)
    assert keys[0].startswith("bronze/transactions/event_date=2026-10-01/")
    assert keys[1].startswith("bronze/transactions/event_date=2026-10-02/")
    row = read_rows(writer.objects[keys[0]])[0]
    assert row["amount"] == "10.10"  # money stays a string, never a float
    assert row["expires_at"] == 1800000000
    assert row["label_suspicious"] is True
    assert row["ingest_source_table"] == "fraud-aml-transactions"


def test_archiver_retry_overwrites_the_same_object():
    event = stream_event(items_for(txn()))
    writer = FakeWriter()
    now = datetime(2026, 10, 1, tzinfo=timezone.utc)
    archiver.archive(event, writer, ENTITIES, now=now)
    archiver.archive(event, writer, ENTITIES, now=now)
    assert len(writer.objects) == 1


def alert_item(severity: str, rule: str = "STRUCTURING"):
    alert = Alert("C1", "t-1", rule, severity, T0, "CASH_IN", Decimal("9500"), "3 deposits under threshold")
    return alert.to_record(T0)


def test_notifier_only_emails_alerts_at_or_above_min_severity(monkeypatch):
    monkeypatch.setenv("ALERT_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789012:alerts")
    monkeypatch.setenv("MIN_SEVERITY", "CRITICAL")
    published = []

    class FakeSns:
        def publish(self, **kwargs):
            published.append(kwargs)

    event = stream_event([alert_item("CRITICAL"), alert_item("MEDIUM", "AMOUNT_SPIKE")], arn=ALERTS_ARN)
    assert notifier.handler(event, sns_client=FakeSns()) == {"notified": 1}
    assert len(published) == 1
    assert "STRUCTURING" in published[0]["Message"]
    assert "AMOUNT_SPIKE" not in published[0]["Message"]

    quiet = stream_event([alert_item("MEDIUM", "AMOUNT_SPIKE")], arn=ALERTS_ARN)
    assert notifier.handler(quiet, sns_client=FakeSns()) == {"notified": 0}
    assert len(published) == 1
