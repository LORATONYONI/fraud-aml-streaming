"""Command-line producer.

Examples::

    # Full pipeline on your laptop, no AWS: synthetic data, detector in-process, quality report
    python -m fraud_aml.producer --sink local

    # Replay PaySim instead of synthetic customers
    python -m fraud_aml.producer --source paysim --paysim-path data/paysim.csv --sink local

    # Stream into the deployed DynamoDB table at 5 transactions/second
    python -m fraud_aml.producer --sink dynamodb --table fraud-aml-transactions --limit 3000

    # Stream into a local Kafka topic (make kafka-up first)
    python -m fraud_aml.producer --sink kafka --rate 500
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fraud_aml.config import RuleConfig
from fraud_aml.engine import DetectionEngine
from fraud_aml.evaluation import evaluate, format_report
from fraud_aml.models import parse_ts
from fraud_aml.producer.baseline import SyntheticBaseline, paysim_source
from fraud_aml.producer.scenarios import ALL_SCENARIOS
from fraud_aml.producer.sinks import DynamoDBSink, KafkaSink, LocalPipelineSink, Sink, StdoutSink
from fraud_aml.producer.stream import build_stream
from fraud_aml.producer.world import World
from fraud_aml.stores import InMemoryAlertSink, InMemoryStateStore


def _default_sim_start() -> datetime:
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    return today - timedelta(days=7)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="python -m fraud_aml.producer", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)  # fmt: skip
    p.add_argument("--source", choices=("synthetic", "paysim"), default="synthetic")
    p.add_argument("--paysim-path", type=Path, help="PaySim CSV from Kaggle (needed for --source paysim)")
    p.add_argument("--sink", choices=("local", "dynamodb", "kafka", "stdout"), default="local")
    p.add_argument("--table", default=os.environ.get("TRANSACTIONS_TABLE"), help="DynamoDB table (--sink dynamodb)")
    p.add_argument("--region", default=None, help="AWS region (defaults to your AWS profile's region)")
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                   help="Kafka bootstrap servers (--sink kafka)")  # fmt: skip
    p.add_argument("--topic", default="transactions", help="Kafka topic (--sink kafka)")
    p.add_argument("--limit", type=int, default=20000, help="baseline transactions to send (scenarios add more)")
    p.add_argument("--rate", type=float, default=None,
                   help="max transactions/second (default: 5 for dynamodb, unlimited otherwise)")  # fmt: skip
    p.add_argument("--scenario-every", type=int, default=250, help="start a scenario every N baseline transactions")
    p.add_argument("--scenarios", default=",".join(ALL_SCENARIOS),
                   help=f"comma-separated subset of: {', '.join(ALL_SCENARIOS)} (empty for none)")  # fmt: skip
    p.add_argument("--customers", type=int, default=3000, help="synthetic customer population")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--sim-start", type=parse_ts, default=None, help="simulation start time (ISO-8601, UTC)")
    p.add_argument("--ttl-days", type=int, default=7, help="days before DynamoDB expires a transaction")
    p.add_argument("--batch-size", type=int, default=100, help="detector batch size (--sink local)")
    p.add_argument("--rule-overrides", default=None, help="JSON, e.g. '{\"velocity_min_count\": 8}'")
    args = p.parse_args(argv)
    if args.source == "paysim" and not args.paysim_path:
        p.error("--source paysim needs --paysim-path")
    if args.sink == "dynamodb" and not args.table:
        p.error("--sink dynamodb needs --table (or TRANSACTIONS_TABLE)")
    unknown = [s for s in args.scenarios.split(",") if s and s not in ALL_SCENARIOS]
    if unknown:
        p.error(f"unknown scenarios: {', '.join(unknown)}")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    rng = random.Random(args.seed)
    start = args.sim_start or _default_sim_start()

    world = World(rng, source=args.source)
    if args.source == "paysim":
        base = paysim_source(args.paysim_path, start, rng)
    else:
        base = SyntheticBaseline(world, start, customers=args.customers)
    scenarios = [ALL_SCENARIOS[name]() for name in args.scenarios.split(",") if name]

    engine = None
    sink: Sink
    if args.sink == "local":
        alerts = InMemoryAlertSink()
        engine = DetectionEngine(RuleConfig.from_json(args.rule_overrides), InMemoryStateStore(), alerts)
        sink = LocalPipelineSink(engine, batch_size=args.batch_size)
    elif args.sink == "dynamodb":
        rate = 5.0 if args.rate is None else args.rate
        sink = DynamoDBSink(args.table, rate=rate, ttl_days=args.ttl_days, region=args.region)
    elif args.sink == "kafka":
        sink = KafkaSink(args.bootstrap, args.topic, rate=args.rate or 0)
    else:
        sink = StdoutSink(rate=args.rate or 0)

    stream = build_stream(base, world, scenarios, rng, limit=args.limit, scenario_every=args.scenario_every)
    sent = Counter()
    first_ts = last_ts = None
    started = time.monotonic()
    log = sys.stderr
    progress_every = 5000 if args.sink == "kafka" else 500
    try:
        for txn, truth in stream:
            sink.send(txn, truth)
            sent["total"] += 1
            if truth.suspicious:
                sent[truth.scenario or "suspicious"] += 1
            first_ts = first_ts or txn.event_ts
            last_ts = txn.event_ts
            if args.sink != "local" and sent["total"] % progress_every == 0:
                print(f"... sent {sent['total']:,} transactions", file=log)
    except KeyboardInterrupt:
        print("\nInterrupted, flushing what was sent so far.", file=log)
    finally:
        sink.close()

    elapsed = time.monotonic() - started
    print(f"\nSent {sent['total']:,} transactions in {elapsed:.1f}s", file=log)
    if first_ts and last_ts:
        print(f"Simulated event time: {first_ts:%Y-%m-%d %H:%M} -> {last_ts:%Y-%m-%d %H:%M} UTC", file=log)
    injected = {k: v for k, v in sent.items() if k != "total"}
    if injected:
        print("Suspicious transactions sent: " + ", ".join(f"{k}={v}" for k, v in sorted(injected.items())), file=log)

    if isinstance(sink, LocalPipelineSink) and engine is not None:
        report = evaluate(sink.truth, engine.alert_sink.all_alerts())
        print(format_report(report))
    return 0
