"""DynamoDB adapters against a local AWS mock (moto). Skipped if moto is not installed."""

from decimal import Decimal

import pytest

moto = pytest.importorskip("moto")
boto3 = pytest.importorskip("boto3")

from fraud_aml.config import RuleConfig  # noqa: E402
from fraud_aml.engine import ConflictError, DetectionEngine  # noqa: E402
from fraud_aml.models import GroundTruth  # noqa: E402
from fraud_aml.producer.sinks import DynamoDBSink  # noqa: E402
from fraud_aml.stores import DynamoDBAlertSink, DynamoDBStateStore  # noqa: E402
from helpers import txn  # noqa: E402

REGION = "us-east-1"


def _table(resource, name, hash_key, range_key=None):
    keys = [{"AttributeName": hash_key, "KeyType": "HASH"}]
    attrs = [{"AttributeName": hash_key, "AttributeType": "S"}]
    if range_key:
        keys.append({"AttributeName": range_key, "KeyType": "RANGE"})
        attrs.append({"AttributeName": range_key, "AttributeType": "S"})
    resource.create_table(TableName=name, KeySchema=keys, AttributeDefinitions=attrs, BillingMode="PAY_PER_REQUEST")


@pytest.fixture
def dynamodb(monkeypatch):
    for var, value in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_DEFAULT_REGION": REGION,
    }.items():
        monkeypatch.setenv(var, value)
    with moto.mock_aws():
        resource = boto3.resource("dynamodb", region_name=REGION)
        _table(resource, "state", "account_id")
        _table(resource, "alerts", "account_id", "alert_key")
        _table(resource, "transactions", "txn_id")
        yield resource


def test_engine_with_dynamodb_is_idempotent(dynamodb):
    engine = DetectionEngine(RuleConfig(), DynamoDBStateStore(dynamodb, "state"), DynamoDBAlertSink(dynamodb, "alerts"))
    batch = [txn("CASH_IN", a, orig="C1", dest="C9", minutes=m) for m, a in ((0, "9500"), (30, "9600"), (60, "9700"))]

    first = engine.process(batch)
    replay = engine.process(batch)

    assert [a.rule_id for a in first.new_alerts] == ["STRUCTURING"]
    assert replay.duplicates == 3 and replay.new_alerts == []
    assert dynamodb.Table("alerts").scan()["Count"] == 1
    state = dynamodb.Table("state").get_item(Key={"account_id": "C1"})["Item"]
    assert state["version"] == 1
    assert [e[2] for e in state["events"]] == [Decimal("9500"), Decimal("9600"), Decimal("9700")]


def test_stale_write_raises_conflict(dynamodb):
    store = DynamoDBStateStore(dynamodb, "state")
    stale = store.load(["C2"])["C2"]
    fresh = store.load(["C2"])["C2"]
    store.save([fresh], expires_at=0)
    with pytest.raises(ConflictError):
        store.save([stale], expires_at=0)


def test_load_handles_more_than_one_batch_get_page(dynamodb):
    store = DynamoDBStateStore(dynamodb, "state")
    ids = [f"C{i:010d}" for i in range(150)]
    store.save(store.load(ids).values(), expires_at=0)
    loaded = store.load(ids)
    assert len(loaded) == 150
    assert all(s.version == 1 for s in loaded.values())


def test_producer_sink_writes_items_with_labels_and_ttl(dynamodb):
    sink = DynamoDBSink("transactions", rate=0, ttl_days=7, region=REGION)
    for i in range(30):  # more than one 25-item batch
        sink.send(txn("PAYMENT", "12.34", minutes=i), GroundTruth(i == 0, "velocity_burst" if i == 0 else None))
    sink.close()
    items = dynamodb.Table("transactions").scan()["Items"]
    assert len(items) == 30
    labelled = [i for i in items if i["label_suspicious"]]
    assert len(labelled) == 1 and labelled[0]["label_scenario"] == "velocity_burst"
    assert all(i["amount"] == Decimal("12.34") and i["expires_at"] > 0 for i in items)
