"""Per-account state used by the stateful rules.

Each account keeps a short, bounded history of recent events (sliding window, by *event time*),
lifetime spending aggregates for its baseline, and rule cool-downs so an alert fires once per
episode instead of on every following transaction.

Stored as one small DynamoDB item per account (usually well under 1 KB = 1 write unit).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from fraud_aml.config import RuleConfig
from fraud_aml.models import ZERO

EVENT_OUT = "o"  # money left the account (CASH_OUT, PAYMENT, TRANSFER, DEBIT)
EVENT_CASH_IN = "c"  # cash deposited into the account
EVENT_TRANSFER_IN = "t"  # transfer received from another customer


@dataclass
class AccountEvent:
    ts: int  # epoch seconds, event time
    kind: str
    amount: Decimal
    sid: str  # short transaction id, used for de-duplication


@dataclass
class AccountState:
    account_id: str
    events: list[AccountEvent] = field(default_factory=list)
    lifetime_out_count: int = 0
    lifetime_out_sum: Decimal = ZERO
    cooldowns: dict[str, int] = field(default_factory=dict)
    version: int = 0  # optimistic-locking version; 0 means "not stored yet"

    def has_seen(self, sid: str) -> bool:
        return any(e.sid == sid for e in self.events)

    def window(self, now_ts: int, window_s: int, kind: str) -> list[AccountEvent]:
        """Events of ``kind`` in the sliding window ``(now - window, now]``."""
        return [e for e in self.events if e.kind == kind and now_ts - window_s < e.ts <= now_ts]

    def baseline_out_avg(self) -> Decimal | None:
        if self.lifetime_out_count == 0:
            return None
        return self.lifetime_out_sum / self.lifetime_out_count

    def in_cooldown(self, rule_id: str, now_ts: int) -> bool:
        return self.cooldowns.get(rule_id, 0) > now_ts

    def start_cooldown(self, rule_id: str, until_ts: int) -> None:
        self.cooldowns[rule_id] = max(until_ts, self.cooldowns.get(rule_id, 0))

    def record(self, event: AccountEvent, cfg: RuleConfig) -> None:
        self.events.append(event)
        if event.kind == EVENT_OUT:
            self.lifetime_out_count += 1
            self.lifetime_out_sum += event.amount
        self.events.sort(key=lambda e: e.ts)
        # Prune relative to the newest event *minus the allowed lateness*: on a partitioned stream
        # an account can see a "future" event (e.g. an inbound transfer from a partition that is
        # ahead) before its own older events, and those must still find their window and cool-downs.
        retain_after = self.events[-1].ts - cfg.allowed_lateness_s - cfg.state_window_s
        kept = [e for e in self.events if e.ts > retain_after]
        self.events = kept[-cfg.state_max_events :]
        # Cool-downs are tiny (at most one per rule), so keep them as long as the events.
        self.cooldowns = {r: until for r, until in self.cooldowns.items() if until > retain_after}

    def to_item(self, expires_at: int) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "version": self.version,
            "events": [[e.ts, e.kind, e.amount, e.sid] for e in self.events],
            "lifetime_out_count": self.lifetime_out_count,
            "lifetime_out_sum": self.lifetime_out_sum,
            "cooldowns": dict(self.cooldowns),
            "expires_at": expires_at,
        }

    @classmethod
    def from_item(cls, item: Mapping[str, Any]) -> AccountState:
        return cls(
            account_id=str(item["account_id"]),
            events=[
                AccountEvent(ts=int(ts), kind=str(kind), amount=Decimal(str(amount)), sid=str(sid))
                for ts, kind, amount, sid in item.get("events", [])
            ],
            lifetime_out_count=int(item.get("lifetime_out_count", 0)),
            lifetime_out_sum=Decimal(str(item.get("lifetime_out_sum", 0))),
            cooldowns={str(k): int(v) for k, v in (item.get("cooldowns") or {}).items()},
            version=int(item.get("version", 0)),
        )
