"""Known fraud / AML typologies injected into the stream with ground-truth labels.

Because we know exactly which transactions belong to which scenario, the pipeline's detection
quality (precision and recall) can be measured, which is rarely possible with real data.

Each scenario returns ``ScheduledEvent``s. Their ``build`` callbacks run at emission time so that
amounts depending on a balance (e.g. "empty the account") use the balance at that moment.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Callable, Protocol

from fraud_aml.models import GroundTruth, Transaction, money, new_id
from fraud_aml.producer.world import World

Builder = Callable[[datetime], "Transaction | None"]


@dataclass(frozen=True)
class ScheduledEvent:
    ts: datetime
    build: Builder  # returns None if the transaction is no longer possible (e.g. no money left)
    truth: GroundTruth


class Scenario(Protocol):
    name: str

    def schedule(self, world: World, start: datetime, rng: random.Random) -> list[ScheduledEvent]: ...


def _after(start: datetime, rng: random.Random, low_s: float, high_s: float) -> datetime:
    return start + timedelta(seconds=rng.uniform(low_s, high_s))


def _share_of_balance(world: World, account: str, share: float) -> Decimal:
    return money(world.balance(account) * money(share))


def _victim(world: World, min_balance: Decimal, rng: random.Random, fresh_range: tuple[int, int]) -> str:
    """An existing customer with money (synthetic mode) or a fresh account (PaySim mode)."""
    return world.pick_customer(min_balance) or world.new_account(rng.uniform(*fresh_range))


class AccountTakeover:
    """A fraudster empties a victim's account into a mule, and the mule cashes out."""

    name = "account_takeover"

    def schedule(self, world: World, start: datetime, rng: random.Random) -> list[ScheduledEvent]:
        truth = GroundTruth(True, self.name, new_id(rng)[:13])
        victim = _victim(world, Decimal("3000"), rng, (3000, 25000))
        mule = world.new_account(0)
        t1 = _after(start, rng, 60, 600)
        t2 = _after(t1, rng, 300, 1800)

        def drain_victim(ts: datetime) -> Transaction | None:
            amount = world.balance(victim)
            return world.make_txn(ts, "TRANSFER", amount, victim, mule) if amount > 0 else None

        def cash_out(ts: datetime) -> Transaction | None:
            amount = world.balance(mule)
            return world.make_txn(ts, "CASH_OUT", amount, mule, world.random_agent()) if amount > 0 else None

        return [ScheduledEvent(t1, drain_victim, truth), ScheduledEvent(t2, cash_out, truth)]


class Structuring:
    """Several cash deposits, each just under the 10,000 reporting threshold, within hours."""

    name = "structuring"

    def __init__(self, threshold: Decimal = Decimal("10000")) -> None:
        self.threshold = threshold

    def schedule(self, world: World, start: datetime, rng: random.Random) -> list[ScheduledEvent]:
        truth = GroundTruth(True, self.name, new_id(rng)[:13])
        depositor = world.pick_customer(Decimal("0")) or world.new_account(rng.uniform(100, 2000))
        count = rng.randint(3, 5)
        times = sorted(_after(start, rng, 60, 8 * 3600) for _ in range(count))
        events = []
        for ts in times:
            amount = money(float(self.threshold) * rng.uniform(0.90, 0.999))

            def deposit(at: datetime, amount: Decimal = amount) -> Transaction:
                return world.make_txn(at, "CASH_IN", amount, depositor, world.random_agent())

            events.append(ScheduledEvent(ts, deposit, truth))
        return events


class MuleNetwork:
    """Several senders pay a mule account, which quickly cashes almost all of it out."""

    name = "mule_network"

    def schedule(self, world: World, start: datetime, rng: random.Random) -> list[ScheduledEvent]:
        truth = GroundTruth(True, self.name, new_id(rng)[:13])
        mule = world.new_account(rng.uniform(0, 200))
        senders = rng.randint(2, 4)
        total = rng.uniform(6000, 20000)
        cuts = sorted(rng.uniform(0.2, 0.8) for _ in range(senders - 1))
        shares = [b - a for a, b in zip([0.0, *cuts], [*cuts, 1.0])]
        in_times = sorted(_after(start, rng, 60, 3 * 3600) for _ in range(senders))
        events = []
        for ts, share in zip(in_times, shares):
            amount = money(max(total * share, 500))
            sender = world.new_account(float(amount) * rng.uniform(1.5, 3.0))

            def pay_mule(at: datetime, sender: str = sender, amount: Decimal = amount) -> Transaction:
                return world.make_txn(at, "TRANSFER", amount, sender, mule)

            events.append(ScheduledEvent(ts, pay_mule, truth))

        out_start = _after(in_times[-1], rng, 30 * 60, 120 * 60)
        plan = [rng.uniform(0.93, 1.0)] if rng.random() < 0.5 else [rng.uniform(0.5, 0.6), rng.uniform(0.95, 1.0)]
        for i, share in enumerate(plan):

            def cash_out(at: datetime, share: float = share) -> Transaction | None:
                amount = _share_of_balance(world, mule, share)
                return world.make_txn(at, "CASH_OUT", amount, mule, world.random_agent()) if amount > 0 else None

            events.append(ScheduledEvent(out_start + timedelta(minutes=20 * i), cash_out, truth))
        return events


class VelocityBurst:
    """A compromised account makes a rapid series of payments and cash-outs."""

    name = "velocity_burst"

    def schedule(self, world: World, start: datetime, rng: random.Random) -> list[ScheduledEvent]:
        truth = GroundTruth(True, self.name, new_id(rng)[:13])
        account = _victim(world, Decimal("1000"), rng, (3000, 10000))
        count = rng.randint(6, 10)
        times = sorted(_after(start, rng, 60, 30 * 60) for _ in range(count))
        events = []
        for ts in times:

            def spend(at: datetime) -> Transaction | None:
                amount = _share_of_balance(world, account, rng.uniform(0.05, 0.12))
                if amount <= 0:
                    return None
                if rng.random() < 0.5:
                    return world.make_txn(at, "PAYMENT", amount, account, world.random_merchant())
                return world.make_txn(at, "CASH_OUT", amount, account, world.random_agent())

            events.append(ScheduledEvent(ts, spend, truth))
        return events


ALL_SCENARIOS: dict[str, Callable[[], Scenario]] = {
    AccountTakeover.name: AccountTakeover,
    Structuring.name: Structuring,
    MuleNetwork.name: MuleNetwork,
    VelocityBurst.name: VelocityBurst,
}
