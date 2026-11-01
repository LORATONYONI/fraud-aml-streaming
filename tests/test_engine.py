from decimal import Decimal

import pytest

from fraud_aml.config import RuleConfig
from fraud_aml.engine import ConflictError, DetectionEngine
from fraud_aml.state import EVENT_TRANSFER_IN
from fraud_aml.stores import InMemoryAlertSink, InMemoryStateStore
from helpers import txn


def make_engine(state_store=None, alerts=None) -> DetectionEngine:
    return DetectionEngine(RuleConfig(), state_store or InMemoryStateStore(), alerts or InMemoryAlertSink())


def structuring_batch():
    deposits = ((0, "9500"), (30, "9600"), (60, "9700"))
    return [txn("CASH_IN", amount, orig="C1", dest="C9", minutes=m) for m, amount in deposits]


def test_structuring_detected_across_batches():
    engine = make_engine()
    first, second, third = structuring_batch()
    assert engine.process([first]).new_alerts == []
    assert engine.process([second]).new_alerts == []
    alerts = engine.process([third]).new_alerts
    assert [a.rule_id for a in alerts] == ["STRUCTURING"]
    assert alerts[0].txn_id == third.txn_id


def test_redelivered_batch_is_idempotent():
    engine = make_engine()
    batch = structuring_batch()
    first = engine.process(batch)
    replay = engine.process(batch)
    assert len(first.new_alerts) == 1
    assert replay.duplicates == 3
    assert replay.processed == 0
    assert replay.new_alerts == []
    state = engine.state_store.load(["C1"])["C1"]
    assert len(state.events) == 3


def test_cooldown_prevents_repeat_alerts_in_same_episode():
    engine = make_engine()
    deposits = [txn("CASH_IN", "9500", orig="C1", dest="C9", minutes=m) for m in range(0, 300, 30)]
    result = engine.process(deposits)
    assert [a.rule_id for a in result.new_alerts] == ["STRUCTURING"]


def test_transfers_credit_the_destination_account():
    engine = make_engine()
    engine.process([txn("TRANSFER", "6000", orig="C1", dest="C2")])
    dest = engine.state_store.load(["C2"])["C2"]
    assert [(e.kind, e.amount) for e in dest.events] == [(EVENT_TRANSFER_IN, Decimal("6000.00"))]


def test_batch_is_evaluated_in_event_time_order():
    engine = make_engine()
    inbound = txn("TRANSFER", "8000", orig="C1", dest="C2", minutes=0)
    cash_out = txn("CASH_OUT", "7900", orig="C2", dest="C9", minutes=30, balance_before="8000")
    result = engine.process([cash_out, inbound])  # delivered out of order
    assert "MULE_PASS_THROUGH" in {a.rule_id for a in result.new_alerts}


class FlakyStateStore(InMemoryStateStore):
    """Simulates another writer updating C1 between our read and our first write."""

    def __init__(self):
        super().__init__()
        self.conflicts_left = 1

    def save(self, states, expires_at):
        if self.conflicts_left:
            self.conflicts_left -= 1
            raise ConflictError("C1")
        super().save(states, expires_at)


def test_conflict_is_retried_without_duplicate_alerts():
    alerts = InMemoryAlertSink()
    engine = make_engine(FlakyStateStore(), alerts)
    result = engine.process(structuring_batch())
    assert result.attempts == 2
    assert len(alerts.items) == 1
    assert len(engine.state_store.load(["C1"])["C1"].events) == 3


def test_persistent_conflict_raises_so_lambda_retries_the_batch():
    class AlwaysConflicts(InMemoryStateStore):
        def save(self, states, expires_at):
            raise ConflictError("C1")

    with pytest.raises(ConflictError):
        make_engine(AlwaysConflicts()).process(structuring_batch())


def test_in_memory_store_enforces_optimistic_locking():
    store = InMemoryStateStore()
    stale = store.load(["C1"])["C1"]
    fresh = store.load(["C1"])["C1"]
    store.save([fresh], expires_at=0)
    with pytest.raises(ConflictError):
        store.save([stale], expires_at=0)


def test_mule_detected_when_inbound_transfer_arrives_after_the_cash_out():
    """On a partitioned stream the inflow (keyed by the sender) can arrive after the cash-out."""
    engine = make_engine()
    inbound = txn("TRANSFER", "8000", orig="C1", dest="C2", minutes=0)
    cash_out = txn("CASH_OUT", "7900", orig="C2", dest="C9", minutes=30, balance_before="8000")
    assert engine.process([cash_out]).new_alerts == []  # nothing to pass through yet
    alerts = engine.process([inbound]).new_alerts  # the late inflow completes the picture
    mule = [a for a in alerts if a.rule_id == "MULE_PASS_THROUGH"]
    assert len(mule) == 1
    assert mule[0].account_id == "C2" and mule[0].details["late_inflow"] is True


def test_future_event_from_another_partition_does_not_reset_cooldowns():
    engine = make_engine()
    burst = [txn("PAYMENT", "50", orig="C1", minutes=m) for m in range(6)]
    assert [a.rule_id for a in engine.process(burst).new_alerts] == ["VELOCITY_BURST"]
    # A transfer *to* C1 from a partition that is 5 hours ahead in event time...
    engine.process([txn("TRANSFER", "10", orig="C5", dest="C1", minutes=300)])
    # ...must not let the rest of the same burst raise the alert again.
    late_burst = [txn("PAYMENT", "50", orig="C1", minutes=6 + m) for m in range(3)]
    assert engine.process(late_burst).new_alerts == []
