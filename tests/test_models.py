from decimal import Decimal

import pytest

from fraud_aml.config import RuleConfig
from fraud_aml.models import Alert, GroundTruth, Transaction, format_ts, money, parse_ts
from helpers import T0, txn


def test_money_is_exact_and_rounds_half_up():
    assert money(0.1) + money(0.2) == Decimal("0.30")
    assert money("10.005") == Decimal("10.01")
    assert money(7) == Decimal("7.00")


def test_timestamp_round_trip_keeps_milliseconds():
    text = "2026-11-01T13:45:12.123Z"
    assert format_ts(parse_ts(text)) == text


def test_from_record_ignores_ground_truth_labels():
    t = txn("TRANSFER", "500", dest="C2000000001")
    record = {**t.to_record(), **GroundTruth(True, "mule_network", "abc").to_record()}
    parsed = Transaction.from_record(record)
    assert parsed == t
    assert not any(name.startswith("label") for name in vars(parsed))


def test_unknown_transaction_type_is_rejected():
    with pytest.raises(ValueError):
        txn("BITCOIN")


def test_alert_key_is_deterministic_and_round_trips():
    alert = Alert("C1", "t-1", "STRUCTURING", "CRITICAL", T0, "CASH_IN", Decimal("9500.00"), "why", {"count": 3})
    assert alert.alert_key == "2026-10-01T12:00:00.000Z#STRUCTURING#t-1"
    assert Alert.from_record(alert.to_record(T0)) == alert


def test_rule_config_overrides_are_typed_and_validated():
    cfg = RuleConfig.from_json('{"velocity_min_count": "8", "mule_min_inflow": 2500}')
    assert cfg.velocity_min_count == 8
    assert cfg.mule_min_inflow == Decimal("2500")
    with pytest.raises(ValueError, match="unknown"):
        RuleConfig.from_json('{"not_a_setting": 1}')
    with pytest.raises(ValueError, match="state_window_s"):
        RuleConfig(mule_window_s=172800)
