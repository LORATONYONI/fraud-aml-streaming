import random
from pathlib import Path

from fraud_aml.config import RuleConfig
from fraud_aml.engine import DetectionEngine
from fraud_aml.evaluation import evaluate
from fraud_aml.producer.baseline import SyntheticBaseline, paysim_source
from fraud_aml.producer.scenarios import ALL_SCENARIOS
from fraud_aml.producer.stream import build_stream
from fraud_aml.producer.world import World
from fraud_aml.stores import InMemoryAlertSink, InMemoryStateStore
from helpers import T0

FIXTURE = Path(__file__).parent / "fixtures" / "paysim_sample.csv"


def synthetic_stream(seed: int = 7, limit: int = 3000, scenario_every: int = 100):
    rng = random.Random(seed)
    world = World(rng)
    base = SyntheticBaseline(world, T0, customers=500)
    scenarios = [factory() for factory in ALL_SCENARIOS.values()]
    return list(build_stream(base, world, scenarios, rng, limit=limit, scenario_every=scenario_every))


def test_same_seed_gives_identical_stream():
    a = [(t.txn_id, t.amount) for t, _ in synthetic_stream(seed=1, limit=500)]
    b = [(t.txn_id, t.amount) for t, _ in synthetic_stream(seed=1, limit=500)]
    assert a == b


def test_stream_is_in_event_time_order_and_ledger_is_consistent():
    stream = synthetic_stream()
    times = [t.event_ts for t, _ in stream]
    assert times == sorted(times)
    for t, _ in stream:
        assert t.amount >= 0
        assert t.orig_balance_after >= 0
        expected = t.orig_balance_before + t.amount if t.txn_type == "CASH_IN" else t.orig_balance_before - t.amount
        assert t.orig_balance_after == expected


def test_every_scenario_type_is_injected_and_labelled():
    labels = {truth.scenario for _, truth in synthetic_stream() if truth.suspicious}
    assert labels == set(ALL_SCENARIOS)


def test_paysim_replay_maps_columns_and_fraud_labels():
    rows = [make() for _, make in paysim_source(FIXTURE, T0, random.Random(0))]
    assert len(rows) == 6
    fraud = [(t, g) for t, g in rows if g.suspicious]
    assert len(fraud) == 2
    t, truth = fraud[0]
    assert (t.txn_type, t.source, t.step) == ("TRANSFER", "paysim", 1)
    assert truth.scenario == "paysim_fraud" and truth.scenario_id == t.txn_id
    assert rows[-1][0].step == 2
    assert rows[-1][0].event_ts.hour == 13  # step 2 = second simulated hour after 12:00


def test_detector_catches_injected_scenarios():
    """Regression guard on detection quality for a fixed seed."""
    stream = synthetic_stream(seed=42, limit=6000, scenario_every=150)
    engine = DetectionEngine(RuleConfig(), InMemoryStateStore(), InMemoryAlertSink())
    for start in range(0, len(stream), 100):
        engine.process([t for t, _ in stream[start : start + 100]])
    report = evaluate({t.txn_id: g for t, g in stream}, engine.alert_sink.all_alerts())
    for name, stats in report.scenarios.items():
        assert stats.recall >= 0.9, f"{name} recall dropped to {stats.recall:.0%}"
    assert report.rules["STRUCTURING"].precision == 1.0
    assert report.rules["VELOCITY_BURST"].precision >= 0.9
