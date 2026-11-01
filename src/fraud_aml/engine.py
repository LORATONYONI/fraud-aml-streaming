"""Batch detection engine: the heart of the detector Lambda, also used by the local demo.

Processing guarantees (the stream delivers *at least once*; the effects are *effectively once*):

1. Each transaction's short id is stored in the account's state, so a re-delivered transaction is
   recognised and skipped.
2. Alerts have deterministic keys and are written with a conditional put, so retries never
   duplicate them.
3. Alerts are written *before* account state. If the state write fails half-way, the retry
   re-creates the same alerts (no-ops) and finishes the state writes.
4. State writes use optimistic locking (a version number). If another invocation updated the same
   account concurrently, the whole batch is re-evaluated against fresh state.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Protocol

from fraud_aml.config import RuleConfig
from fraud_aml.models import Alert, Transaction, is_customer_account
from fraud_aml.rules import RULES, Rule
from fraud_aml.state import EVENT_CASH_IN, EVENT_OUT, EVENT_TRANSFER_IN, AccountEvent, AccountState

logger = logging.getLogger(__name__)


class ConflictError(Exception):
    """Another writer updated an account between our read and our write."""


class StateStore(Protocol):
    def load(self, account_ids: Iterable[str]) -> dict[str, AccountState]: ...

    def save(self, states: Iterable[AccountState], expires_at: int) -> None: ...


class AlertSink(Protocol):
    def put_new(self, alerts: Sequence[Alert], detected_at: datetime, expires_at: int) -> list[Alert]:
        """Store alerts that do not exist yet and return only those."""
        ...


@dataclass
class BatchResult:
    received: int = 0
    processed: int = 0
    duplicates: int = 0
    alerts: list[Alert] = field(default_factory=list)
    new_alerts: list[Alert] = field(default_factory=list)
    attempts: int = 1

    def summary(self) -> dict[str, object]:
        by_rule: dict[str, int] = {}
        for alert in self.new_alerts:
            by_rule[alert.rule_id] = by_rule.get(alert.rule_id, 0) + 1
        return {
            "received": self.received,
            "processed": self.processed,
            "duplicates": self.duplicates,
            "alerts": len(self.alerts),
            "new_alerts": len(self.new_alerts),
            "new_alerts_by_rule": by_rule,
            "attempts": self.attempts,
        }


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class DetectionEngine:
    def __init__(
        self,
        cfg: RuleConfig,
        state_store: StateStore,
        alert_sink: AlertSink,
        *,
        rules: Sequence[Rule] = RULES,
        state_ttl: timedelta = timedelta(days=30),
        alert_ttl: timedelta = timedelta(days=30),
        max_attempts: int = 3,
        clock: Callable[[], datetime] = _utcnow,
    ) -> None:
        self.cfg = cfg
        self.state_store = state_store
        self.alert_sink = alert_sink
        self.rules = tuple(rules)
        self.state_ttl = state_ttl
        self.alert_ttl = alert_ttl
        self.max_attempts = max_attempts
        self.clock = clock

    def process(self, txns: Sequence[Transaction]) -> BatchResult:
        for attempt in range(1, self.max_attempts + 1):
            try:
                result = self._process_once(txns)
                result.attempts = attempt
                return result
            except ConflictError:
                if attempt == self.max_attempts:
                    raise
                logger.warning("state conflict, re-evaluating batch (attempt %d)", attempt)
        raise AssertionError("unreachable")

    def _process_once(self, txns: Sequence[Transaction]) -> BatchResult:
        result = BatchResult(received=len(txns))
        # Records from different shards are not globally ordered; evaluate in event-time order.
        ordered = sorted(txns, key=lambda t: (t.event_ts, t.txn_id))

        accounts: dict[str, None] = {}
        for txn in ordered:
            accounts[txn.orig_account] = None
            if self._credits_destination(txn):
                accounts[txn.dest_account] = None
        states = self.state_store.load(accounts.keys())

        changed: dict[str, AccountState] = {}
        for txn in ordered:
            orig = states[txn.orig_account]
            if orig.has_seen(txn.short_id):
                result.duplicates += 1
            else:
                result.alerts.extend(self._evaluate(txn, orig))
                kind = EVENT_CASH_IN if txn.txn_type == "CASH_IN" else EVENT_OUT
                orig.record(AccountEvent(txn.epoch, kind, txn.amount, txn.short_id), self.cfg)
                changed[orig.account_id] = orig
                result.processed += 1

            if self._credits_destination(txn):
                dest = states[txn.dest_account]
                if not dest.has_seen(txn.short_id):
                    dest.record(AccountEvent(txn.epoch, EVENT_TRANSFER_IN, txn.amount, txn.short_id), self.cfg)
                    changed[dest.account_id] = dest
                    # Cross-account rules must not depend on arrival order (see rules.py).
                    result.alerts.extend(self._evaluate(txn, dest, on_credit=True))

        now = self.clock()
        result.new_alerts = self.alert_sink.put_new(
            result.alerts, detected_at=now, expires_at=int((now + self.alert_ttl).timestamp())
        )
        self.state_store.save(changed.values(), expires_at=int((now + self.state_ttl).timestamp()))
        return result

    @staticmethod
    def _credits_destination(txn: Transaction) -> bool:
        return (
            txn.txn_type == "TRANSFER"
            and is_customer_account(txn.dest_account)
            and txn.dest_account != txn.orig_account
        )

    def _evaluate(self, txn: Transaction, state: AccountState, on_credit: bool = False) -> list[Alert]:
        """Run the rules for ``state``'s account: the payer (default) or the receiver of a transfer."""
        alerts = []
        for rule in self.rules:
            evaluate = rule.evaluate_credit if on_credit else rule.evaluate
            if evaluate is None:
                continue
            if rule.cooldown_field and state.in_cooldown(rule.rule_id, txn.epoch):
                continue
            finding = evaluate(txn, state, self.cfg)
            if finding is None:
                continue
            reason, details = finding
            alerts.append(
                Alert(
                    account_id=state.account_id,
                    txn_id=txn.txn_id,
                    rule_id=rule.rule_id,
                    severity=rule.severity,
                    event_ts=txn.event_ts,
                    txn_type=txn.txn_type,
                    amount=txn.amount,
                    reason=reason,
                    details=details,
                )
            )
            if rule.cooldown_field:
                state.start_cooldown(rule.rule_id, txn.epoch + rule.cooldown_s(self.cfg))
        return alerts
