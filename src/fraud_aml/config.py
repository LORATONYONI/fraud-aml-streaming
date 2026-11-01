"""Detection thresholds. Every value can be overridden without a code change (see ``from_json``)."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, fields
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class RuleConfig:
    # ACCOUNT_DRAINED: an account is emptied in one TRANSFER / CASH_OUT (classic account takeover).
    drain_min_balance: Decimal = Decimal("1000")
    drain_min_ratio: Decimal = Decimal("0.99")

    # HIGH_VALUE_TRANSFER: a single transfer above a reporting-style threshold.
    high_value_threshold: Decimal = Decimal("200000")

    # VELOCITY_BURST: many outgoing transactions from one account in a short window.
    velocity_window_s: int = 3600
    velocity_min_count: int = 6

    # STRUCTURING ("smurfing"): repeated cash deposits kept just under the reporting threshold.
    structuring_threshold: Decimal = Decimal("10000")
    structuring_band_floor: Decimal = Decimal("0.90")
    structuring_min_count: int = 3
    structuring_window_s: int = 86400

    # MULE_PASS_THROUGH: money comes in by transfer and goes straight back out.
    mule_window_s: int = 86400
    mule_min_inflow: Decimal = Decimal("5000")
    mule_min_passthrough: Decimal = Decimal("0.80")

    # AMOUNT_SPIKE: an outgoing amount far above the customer's own average.
    spike_multiplier: Decimal = Decimal("10")
    spike_min_history: int = 5
    spike_min_amount: Decimal = Decimal("1000")

    # Per-account history kept in the state table. Must cover the longest rule window.
    state_window_s: int = 86400
    state_max_events: int = 40
    # How far out of event-time order events may arrive and still be evaluated correctly.
    allowed_lateness_s: int = 6 * 3600

    def __post_init__(self) -> None:
        longest = max(self.velocity_window_s, self.structuring_window_s, self.mule_window_s)
        if self.state_window_s < longest:
            raise ValueError(f"state_window_s ({self.state_window_s}) must be >= the longest rule window ({longest})")

    @classmethod
    def from_overrides(cls, overrides: Mapping[str, Any] | None) -> RuleConfig:
        if not overrides:
            return cls()
        defaults = cls()
        known = {f.name for f in fields(cls)}
        unknown = sorted(set(overrides) - known)
        if unknown:
            raise ValueError(f"unknown rule settings: {', '.join(unknown)}")
        kwargs: dict[str, Any] = {}
        for name, value in overrides.items():
            current = getattr(defaults, name)
            kwargs[name] = Decimal(str(value)) if isinstance(current, Decimal) else int(value)
        return cls(**kwargs)

    @classmethod
    def from_json(cls, raw: str | None) -> RuleConfig:
        return cls.from_overrides(json.loads(raw) if raw else None)
