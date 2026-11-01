"""Test helpers: transaction factory and DynamoDB Streams event builder."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from fraud_aml.models import Transaction, money, new_id

T0 = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
TRANSACTIONS_ARN = "arn:aws:dynamodb:us-east-1:123456789012:table/fraud-aml-transactions/stream/2026-10-01T00:00:00.000"
ALERTS_ARN = "arn:aws:dynamodb:us-east-1:123456789012:table/fraud-aml-alerts/stream/2026-10-01T00:00:00.000"


def txn(
    txn_type: str = "PAYMENT",
    amount: str | int = "100",
    orig: str = "C1000000001",
    dest: str = "M2000000001",
    *,
    minutes: float = 0,
    balance_before: str | int | None = None,
    balance_after: str | int | None = None,
    txn_id: str | None = None,
) -> Transaction:
    amount_d = money(amount)
    before = money(balance_before) if balance_before is not None else money(100000)
    if balance_after is not None:
        after = money(balance_after)
    else:
        after = before + amount_d if txn_type == "CASH_IN" else before - amount_d
    return Transaction(
        txn_id=txn_id or new_id(),
        event_ts=T0 + timedelta(minutes=minutes),
        txn_type=txn_type,
        amount=amount_d,
        orig_account=orig,
        dest_account=dest,
        orig_balance_before=before,
        orig_balance_after=after,
    )


def to_typed(value: Any) -> dict[str, Any]:
    """Python value -> DynamoDB typed JSON, as found in stream records."""
    if value is None:
        return {"NULL": True}
    if isinstance(value, bool):
        return {"BOOL": value}
    if isinstance(value, (int, Decimal)):
        return {"N": str(value)}
    if isinstance(value, str):
        return {"S": value}
    if isinstance(value, dict):
        return {"M": {k: to_typed(v) for k, v in value.items()}}
    if isinstance(value, list):
        return {"L": [to_typed(v) for v in value]}
    raise TypeError(type(value))


def stream_event(items: list[dict[str, Any]], arn: str = TRANSACTIONS_ARN, event_name: str = "INSERT") -> dict:
    return {
        "Records": [
            {
                "eventName": event_name,
                "eventSourceARN": arn,
                "dynamodb": {
                    "SequenceNumber": str(100000000000000000000 + i),
                    "NewImage": {k: to_typed(v) for k, v in item.items()},
                },
            }
            for i, item in enumerate(items)
        ]
    }
