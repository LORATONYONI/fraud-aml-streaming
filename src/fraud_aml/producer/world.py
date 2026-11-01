"""Simulated mobile-money ledger: accounts, balances and transaction construction."""

from __future__ import annotations

import random
from datetime import datetime
from decimal import Decimal

from fraud_aml.models import ZERO, Transaction, money, new_id


class World:
    def __init__(self, rng: random.Random, currency: str = "USD", source: str = "synthetic") -> None:
        self.rng = rng
        self.currency = currency
        self.source = source
        self.balances: dict[str, Decimal] = {}  # accounts whose balance we track
        self.customers: list[str] = []  # baseline population (synthetic mode only)
        self.spend_scale: dict[str, float] = {}
        self.merchants: list[str] = []
        self.agents: list[str] = []

    def _new_id(self, prefix: str) -> str:
        while True:
            account_id = f"{prefix}{self.rng.randrange(10**9, 10**10)}"
            if account_id not in self.balances and account_id not in self.merchants:
                return account_id

    def new_account(self, balance: Decimal | float = 0) -> str:
        account_id = self._new_id("C")
        self.balances[account_id] = money(balance)
        return account_id

    def add_customer(self, balance: Decimal | float, spend_scale: float) -> str:
        account_id = self.new_account(balance)
        self.customers.append(account_id)
        self.spend_scale[account_id] = spend_scale
        return account_id

    def add_merchant(self) -> str:
        merchant = self._new_id("M")
        self.merchants.append(merchant)
        return merchant

    def add_agent(self) -> str:
        # Cash-in/cash-out agents. Their float balance is not simulated.
        agent = self._new_id("C")
        self.agents.append(agent)
        return agent

    def balance(self, account_id: str) -> Decimal:
        return self.balances.get(account_id, ZERO)

    def random_agent(self) -> str:
        if not self.agents:
            self.add_agent()
        return self.rng.choice(self.agents)

    def random_merchant(self) -> str:
        if not self.merchants:
            self.add_merchant()
        return self.rng.choice(self.merchants)

    def pick_customer(self, min_balance: Decimal, exclude: tuple[str, ...] = ()) -> str | None:
        for _ in range(50):
            if not self.customers:
                return None
            candidate = self.rng.choice(self.customers)
            if candidate not in exclude and self.balance(candidate) >= min_balance:
                return candidate
        return None

    def make_txn(self, ts: datetime, txn_type: str, amount: Decimal | float, orig: str, dest: str) -> Transaction:
        """Create a transaction and apply it to the ledger."""
        amount = money(amount)
        orig_before = self.balance(orig)
        if txn_type == "CASH_IN":
            orig_after = orig_before + amount
        else:
            if amount > orig_before:
                raise ValueError(f"{orig} cannot pay {amount}, balance is {orig_before}")
            orig_after = orig_before - amount
        self.balances[orig] = orig_after

        dest_before = dest_after = ZERO
        if dest in self.balances:
            dest_before = self.balances[dest]
            dest_after = dest_before + amount if txn_type == "TRANSFER" else dest_before
            self.balances[dest] = dest_after

        return Transaction(
            txn_id=new_id(self.rng),
            event_ts=ts,
            txn_type=txn_type,
            amount=amount,
            orig_account=orig,
            dest_account=dest,
            orig_balance_before=orig_before,
            orig_balance_after=orig_after,
            dest_balance_before=dest_before,
            dest_balance_after=dest_after,
            currency=self.currency,
            source=self.source,
        )
