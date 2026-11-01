"""Core domain objects shared by the producer, the detector and the archiver.

Money is always ``decimal.Decimal``. Floats never touch an amount, because binary floating point
cannot represent most cent values exactly (0.1 + 0.2 != 0.3), which is unacceptable in finance.
"""

from __future__ import annotations

import json
import random
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

TXN_TYPES = ("CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER")
OUTFLOW_TYPES = frozenset({"CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"})

SEVERITY_ORDER = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}

CENT = Decimal("0.01")
ZERO = Decimal("0")


def money(value: Any) -> Decimal:
    """Convert a number or numeric string to a 2-decimal-place Decimal without using floats."""
    return Decimal(str(value)).quantize(CENT, rounding=ROUND_HALF_UP)


def format_ts(dt: datetime) -> str:
    """ISO-8601 UTC timestamp with millisecond precision, e.g. ``2026-11-01T13:45:12.123Z``."""
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def parse_ts(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def new_id(rng: random.Random | None = None) -> str:
    """UUID4. Pass a seeded ``rng`` to make simulations reproducible."""
    if rng is None:
        return str(uuid.uuid4())
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def is_customer_account(account_id: str) -> bool:
    """PaySim convention: customer accounts start with ``C``, merchants with ``M``."""
    return account_id.startswith("C")


@dataclass(frozen=True)
class Transaction:
    """A single mobile-money transaction, as the detector sees it.

    Balance semantics follow PaySim: for CASH_IN the *origin* account receives the money; for every
    other type the origin account pays it out.
    """

    txn_id: str
    event_ts: datetime
    txn_type: str
    amount: Decimal
    orig_account: str
    dest_account: str
    orig_balance_before: Decimal
    orig_balance_after: Decimal
    dest_balance_before: Decimal = ZERO
    dest_balance_after: Decimal = ZERO
    currency: str = "USD"
    source: str = "synthetic"
    step: int | None = None

    def __post_init__(self) -> None:
        if self.txn_type not in TXN_TYPES:
            raise ValueError(f"unknown txn_type {self.txn_type!r}")
        if self.amount < 0:
            raise ValueError("amount must be >= 0")

    @property
    def is_outflow(self) -> bool:
        return self.txn_type in OUTFLOW_TYPES

    @property
    def short_id(self) -> str:
        """Compact id stored in account state for de-duplication."""
        return self.txn_id.replace("-", "")[:12]

    @property
    def epoch(self) -> int:
        return int(self.event_ts.timestamp())

    def to_record(self) -> dict[str, Any]:
        record: dict[str, Any] = {
            "txn_id": self.txn_id,
            "event_ts": format_ts(self.event_ts),
            "txn_type": self.txn_type,
            "amount": self.amount,
            "orig_account": self.orig_account,
            "dest_account": self.dest_account,
            "orig_balance_before": self.orig_balance_before,
            "orig_balance_after": self.orig_balance_after,
            "dest_balance_before": self.dest_balance_before,
            "dest_balance_after": self.dest_balance_after,
            "currency": self.currency,
            "source": self.source,
        }
        if self.step is not None:
            record["step"] = self.step
        return record

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> Transaction:
        """Build from a DynamoDB item or JSON record.

        ``label_*`` fields (the simulation's answer key) are deliberately ignored, so the detector
        cannot see ground truth even by accident.
        """
        step = record.get("step")
        return cls(
            txn_id=str(record["txn_id"]),
            event_ts=parse_ts(str(record["event_ts"])),
            txn_type=str(record["txn_type"]),
            amount=money(record["amount"]),
            orig_account=str(record["orig_account"]),
            dest_account=str(record["dest_account"]),
            orig_balance_before=money(record.get("orig_balance_before", 0)),
            orig_balance_after=money(record.get("orig_balance_after", 0)),
            dest_balance_before=money(record.get("dest_balance_before", 0)),
            dest_balance_after=money(record.get("dest_balance_after", 0)),
            currency=str(record.get("currency", "USD")),
            source=str(record.get("source", "unknown")),
            step=int(step) if step is not None else None,
        )


@dataclass(frozen=True)
class GroundTruth:
    """The simulation's answer key for one transaction. Used only for evaluation."""

    suspicious: bool
    scenario: str | None = None
    scenario_id: str | None = None

    def to_record(self) -> dict[str, Any]:
        record: dict[str, Any] = {"label_suspicious": self.suspicious}
        if self.scenario:
            record["label_scenario"] = self.scenario
        if self.scenario_id:
            record["label_scenario_id"] = self.scenario_id
        return record

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> GroundTruth:
        """Read the answer key back from a stored transaction (evaluation jobs only)."""
        return cls(
            suspicious=bool(record.get("label_suspicious", False)),
            scenario=record.get("label_scenario"),
            scenario_id=record.get("label_scenario_id"),
        )


NOT_SUSPICIOUS = GroundTruth(suspicious=False)


@dataclass(frozen=True)
class Alert:
    account_id: str
    txn_id: str
    rule_id: str
    severity: str
    event_ts: datetime
    txn_type: str
    amount: Decimal
    reason: str
    details: dict[str, Any] = field(default_factory=dict, compare=False)

    @property
    def alert_key(self) -> str:
        """Deterministic sort key, so re-processing the same transaction never duplicates an alert."""
        return f"{format_ts(self.event_ts)}#{self.rule_id}#{self.txn_id}"

    def to_record(self, detected_at: datetime) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "alert_key": self.alert_key,
            "txn_id": self.txn_id,
            "rule_id": self.rule_id,
            "severity": self.severity,
            "event_ts": format_ts(self.event_ts),
            "detected_at": format_ts(detected_at),
            "txn_type": self.txn_type,
            "amount": self.amount,
            "reason": self.reason,
            "details": json.dumps(self.details, default=str, sort_keys=True),
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> Alert:
        details = record.get("details") or "{}"
        return cls(
            account_id=str(record["account_id"]),
            txn_id=str(record["txn_id"]),
            rule_id=str(record["rule_id"]),
            severity=str(record["severity"]),
            event_ts=parse_ts(str(record["event_ts"])),
            txn_type=str(record["txn_type"]),
            amount=money(record["amount"]),
            reason=str(record.get("reason", "")),
            details=json.loads(details) if isinstance(details, str) else dict(details),
        )
