"""Baseline (mostly legitimate) traffic: a synthetic population or a PaySim CSV replay.

Each source yields ``(event_ts, make)`` pairs in event-time order. ``make()`` builds the
transaction at emission time, so balances stay consistent with any scenario transactions that the
stream interleaves in between.
"""

from __future__ import annotations

import csv
import math
import random
from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable

from fraud_aml.models import NOT_SUSPICIOUS, GroundTruth, Transaction, money, new_id
from fraud_aml.producer.world import World

Emitted = tuple[Transaction, GroundTruth]
BaseEvent = tuple[datetime, Callable[[], Emitted]]

# Mix of transaction types, roughly in line with PaySim.
TYPE_WEIGHTS = {"PAYMENT": 0.38, "CASH_OUT": 0.30, "CASH_IN": 0.20, "TRANSFER": 0.10, "DEBIT": 0.02}
# Typical size of each type, as a multiple of the customer's personal spending scale.
TYPE_SCALE = {"PAYMENT": 1.0, "CASH_OUT": 2.0, "CASH_IN": 7.0, "TRANSFER": 4.0, "DEBIT": 1.0}
# Share of legitimate cash-outs/transfers that empty the account (e.g. a customer moving all their
# money). These are the realistic false positives for the ACCOUNT_DRAINED rule.
LEGIT_DRAIN_PROBABILITY = 0.01


class SyntheticBaseline:
    def __init__(
        self,
        world: World,
        start: datetime,
        customers: int = 3000,
        merchants: int = 300,
        agents: int = 150,
        tx_per_customer_per_day: float = 3.0,
    ) -> None:
        self.world = world
        self.start = start
        rng = world.rng
        for _ in range(customers):
            scale = rng.lognormvariate(math.log(200), 0.7)
            world.add_customer(balance=scale * rng.lognormvariate(math.log(15), 0.8), spend_scale=scale)
        for _ in range(merchants):
            world.add_merchant()
        for _ in range(agents):
            world.add_agent()
        self.mean_gap_s = 86400 / (customers * tx_per_customer_per_day)
        self._types = list(TYPE_WEIGHTS)
        self._weights = list(TYPE_WEIGHTS.values())

    def __iter__(self) -> Iterator[BaseEvent]:
        ts = self.start
        rng = self.world.rng
        while True:
            ts = ts + timedelta(seconds=rng.expovariate(1 / self.mean_gap_s))
            yield ts, (lambda ts=ts: (self._make(ts), NOT_SUSPICIOUS))

    def _make(self, ts: datetime) -> Transaction:
        world, rng = self.world, self.world.rng
        customer = rng.choice(world.customers)
        balance = world.balance(customer)
        txn_type = rng.choices(self._types, self._weights)[0]
        if txn_type != "CASH_IN" and balance < 10:
            txn_type = "CASH_IN"  # nothing left to spend: the customer tops up instead

        amount = money(world.spend_scale[customer] * TYPE_SCALE[txn_type] * rng.lognormvariate(0, 0.6))
        if txn_type != "CASH_IN":
            if txn_type in ("CASH_OUT", "TRANSFER") and rng.random() < LEGIT_DRAIN_PROBABILITY:
                amount = balance
            elif amount > balance:
                amount = money(balance * money(rng.uniform(0.1, 0.9)))

        if txn_type in ("CASH_IN", "CASH_OUT"):
            dest = world.random_agent()
        elif txn_type == "TRANSFER":
            dest = rng.choice(world.customers)
            while dest == customer:
                dest = rng.choice(world.customers)
        else:
            dest = world.random_merchant()
        return world.make_txn(ts, txn_type, amount, customer, dest)


PAYSIM_COLUMNS = (
    "step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
    "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud",
)  # fmt: skip


def paysim_source(path: Path, start: datetime, rng: random.Random) -> Iterator[BaseEvent]:
    """Replay the PaySim CSV in order. ``step`` is the simulated hour (1..743).

    Rows inside a step get random, sorted timestamps within that hour, so the stream stays in
    event-time order. PaySim's own fraud rows become labelled ground truth.
    """
    with open(path, newline="") as fh:
        reader = csv.DictReader(fh)
        missing = set(PAYSIM_COLUMNS) - set(reader.fieldnames or ())
        if missing:
            raise ValueError(f"{path} is not a PaySim file, missing columns: {sorted(missing)}")
        buffer: list[dict[str, str]] = []
        current_step: int | None = None
        for row in reader:
            step = int(row["step"])
            if current_step is not None and step != current_step:
                yield from _emit_step(buffer, current_step, start, rng)
                buffer = []
            current_step = step
            buffer.append(row)
        if buffer and current_step is not None:
            yield from _emit_step(buffer, current_step, start, rng)


def _emit_step(rows: list[dict[str, str]], step: int, start: datetime, rng: random.Random) -> Iterator[BaseEvent]:
    hour_start = start + timedelta(hours=step - 1)
    offsets = sorted(rng.uniform(0, 3600) for _ in rows)
    for row, offset in zip(rows, offsets):
        ts = hour_start + timedelta(seconds=offset)
        txn = Transaction(
            txn_id=new_id(rng),
            event_ts=ts,
            txn_type=row["type"],
            amount=money(row["amount"]),
            orig_account=row["nameOrig"],
            dest_account=row["nameDest"],
            orig_balance_before=money(row["oldbalanceOrg"]),
            orig_balance_after=money(row["newbalanceOrig"]),
            dest_balance_before=money(row["oldbalanceDest"]),
            dest_balance_after=money(row["newbalanceDest"]),
            source="paysim",
            step=step,
        )
        truth = GroundTruth(True, "paysim_fraud", txn.txn_id) if row["isFraud"] == "1" else NOT_SUSPICIOUS
        yield ts, (lambda txn=txn, truth=truth: (txn, truth))
