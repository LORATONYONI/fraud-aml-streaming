"""Detection rules.

Every rule looks at one transaction plus the origin account's state *before* that transaction is
recorded, and returns ``(reason, details)`` when it fires. Stateful rules have a cool-down so they
alert once per episode, not on every later transaction in the same window.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Optional

from fraud_aml.config import RuleConfig
from fraud_aml.models import ZERO, Transaction
from fraud_aml.state import EVENT_CASH_IN, EVENT_OUT, EVENT_TRANSFER_IN, AccountState

Finding = Optional[tuple[str, dict[str, Any]]]


def _pct(value: Decimal) -> str:
    return f"{(value * 100).quantize(Decimal('1'))}%"


def account_drained(txn: Transaction, state: AccountState, cfg: RuleConfig) -> Finding:
    if txn.txn_type not in ("TRANSFER", "CASH_OUT"):
        return None
    before = txn.orig_balance_before
    if before < cfg.drain_min_balance:
        return None
    if txn.amount < before * cfg.drain_min_ratio:
        return None
    if txn.orig_balance_after > before * (1 - cfg.drain_min_ratio):
        return None
    ratio = txn.amount / before
    return (
        f"{txn.txn_type} moved {_pct(ratio)} of a {before} balance in one transaction",
        {"balance_before": before, "ratio": ratio.quantize(Decimal("0.0001"))},
    )


def high_value_transfer(txn: Transaction, state: AccountState, cfg: RuleConfig) -> Finding:
    if txn.txn_type != "TRANSFER" or txn.amount < cfg.high_value_threshold:
        return None
    return (
        f"Transfer of {txn.amount} is at or above the {cfg.high_value_threshold} threshold",
        {"threshold": cfg.high_value_threshold},
    )


def velocity_burst(txn: Transaction, state: AccountState, cfg: RuleConfig) -> Finding:
    if not txn.is_outflow:
        return None
    count = len(state.window(txn.epoch, cfg.velocity_window_s, EVENT_OUT)) + 1
    if count < cfg.velocity_min_count:
        return None
    minutes = cfg.velocity_window_s // 60
    return (
        f"{count} outgoing transactions within {minutes} minutes",
        {"count": count, "window_s": cfg.velocity_window_s},
    )


def structuring(txn: Transaction, state: AccountState, cfg: RuleConfig) -> Finding:
    floor = cfg.structuring_threshold * cfg.structuring_band_floor
    if txn.txn_type != "CASH_IN" or not (floor <= txn.amount < cfg.structuring_threshold):
        return None
    near = [
        e.amount
        for e in state.window(txn.epoch, cfg.structuring_window_s, EVENT_CASH_IN)
        if floor <= e.amount < cfg.structuring_threshold
    ] + [txn.amount]
    total = sum(near, ZERO)
    if len(near) < cfg.structuring_min_count or total < cfg.structuring_threshold:
        return None
    hours = cfg.structuring_window_s // 3600
    return (
        f"{len(near)} cash deposits between {floor} and {cfg.structuring_threshold} within {hours}h (total {total})",
        {"count": len(near), "total": total, "threshold": cfg.structuring_threshold},
    )


def _pass_through(state: AccountState, at_ts: int, cfg: RuleConfig, pending_out: Decimal, late: bool) -> Finding:
    """Money received by transfer in the window ending at ``at_ts``, and how much of it left again."""
    inbound = state.window(at_ts, cfg.mule_window_s, EVENT_TRANSFER_IN)
    inflow = sum((e.amount for e in inbound), ZERO)
    if inflow < cfg.mule_min_inflow:
        return None
    # Only count money that left *after* it arrived; earlier spending is not pass-through.
    first_in = min(e.ts for e in inbound)
    outflow = pending_out + sum(
        (e.amount for e in state.window(at_ts, cfg.mule_window_s, EVENT_OUT) if e.ts >= first_in), ZERO
    )
    ratio = outflow / inflow
    if ratio < cfg.mule_min_passthrough:
        return None
    suffix = " (detected when a late inbound transfer arrived)" if late else ""
    return (
        f"Received {inflow} by transfer and sent {_pct(ratio)} of it back out{suffix}",
        {
            "inflow": inflow,
            "outflow": outflow,
            "ratio": ratio.quantize(Decimal("0.0001")),
            "inbound_transfers": len(inbound),
            "late_inflow": late,
        },
    )


def mule_pass_through(txn: Transaction, state: AccountState, cfg: RuleConfig) -> Finding:
    """Evaluated on the mule's outgoing transaction (state *before* it is recorded)."""
    if not txn.is_outflow:
        return None
    return _pass_through(state, txn.epoch, cfg, pending_out=txn.amount, late=False)


def mule_pass_through_late_inflow(txn: Transaction, state: AccountState, cfg: RuleConfig) -> Finding:
    """Order-insensitive companion, evaluated on the *receiving* account (state *after* recording).

    The inbound transfer is keyed by the sender, so on a partitioned stream it can arrive after the
    mule's own cash-out. Only outflows at or after this transfer's event time are considered; in
    normal order there are none yet and ``mule_pass_through`` decides when the outflow arrives.
    """
    later_out = [e.ts for e in state.events if e.kind == EVENT_OUT and e.ts >= txn.epoch]
    if not later_out:
        return None
    return _pass_through(state, max(later_out), cfg, pending_out=ZERO, late=True)


def amount_spike(txn: Transaction, state: AccountState, cfg: RuleConfig) -> Finding:
    if not txn.is_outflow or txn.amount < cfg.spike_min_amount:
        return None
    if state.lifetime_out_count < cfg.spike_min_history:
        return None
    avg = state.baseline_out_avg()
    if avg is None or avg <= 0 or txn.amount < avg * cfg.spike_multiplier:
        return None
    multiple = (txn.amount / avg).quantize(Decimal("0.1"))
    return (
        f"Amount is {multiple}x this customer's average outgoing transaction ({avg.quantize(Decimal('0.01'))})",
        {"baseline_avg": avg.quantize(Decimal("0.01")), "multiple": multiple},
    )


@dataclass(frozen=True)
class Rule:
    rule_id: str
    severity: str
    evaluate: Callable[[Transaction, AccountState, RuleConfig], Finding]
    # Name of the RuleConfig field holding the cool-down in seconds; None = stateless, no cool-down.
    cooldown_field: str | None = None
    # Optional check on the *receiving* account of a transfer, after the inflow is recorded.
    evaluate_credit: Callable[[Transaction, AccountState, RuleConfig], Finding] | None = None

    def cooldown_s(self, cfg: RuleConfig) -> int:
        return int(getattr(cfg, self.cooldown_field)) if self.cooldown_field else 0


RULES: tuple[Rule, ...] = (
    Rule("ACCOUNT_DRAINED", "HIGH", account_drained),
    Rule("HIGH_VALUE_TRANSFER", "MEDIUM", high_value_transfer),
    Rule("VELOCITY_BURST", "MEDIUM", velocity_burst, cooldown_field="velocity_window_s"),
    Rule("STRUCTURING", "CRITICAL", structuring, cooldown_field="structuring_window_s"),
    Rule(
        "MULE_PASS_THROUGH",
        "CRITICAL",
        mule_pass_through,
        cooldown_field="mule_window_s",
        evaluate_credit=mule_pass_through_late_inflow,
    ),
    Rule("AMOUNT_SPIKE", "MEDIUM", amount_spike),
)
