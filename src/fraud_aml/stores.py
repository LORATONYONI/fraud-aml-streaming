"""State and alert storage: DynamoDB implementations for AWS, in-memory ones for tests and demos.

The in-memory versions copy DynamoDB's semantics (serialised items, conditional writes,
optimistic locking) so tests exercise the same code paths as production.
"""

from __future__ import annotations

import copy
import logging
import time
from collections.abc import Iterable, Sequence
from datetime import datetime
from typing import Any

from fraud_aml.engine import ConflictError
from fraud_aml.models import Alert
from fraud_aml.state import AccountState

logger = logging.getLogger(__name__)

_BATCH_GET_LIMIT = 100


class InMemoryStateStore:
    def __init__(self) -> None:
        self.items: dict[str, dict[str, Any]] = {}

    def load(self, account_ids: Iterable[str]) -> dict[str, AccountState]:
        return {
            a: AccountState.from_item(copy.deepcopy(self.items[a])) if a in self.items else AccountState(a)
            for a in account_ids
        }

    def save(self, states: Iterable[AccountState], expires_at: int) -> None:
        for state in states:
            stored = self.items.get(state.account_id)
            stored_version = int(stored["version"]) if stored else 0
            if stored_version != state.version:
                raise ConflictError(state.account_id)
            item = state.to_item(expires_at)
            item["version"] = state.version + 1
            self.items[state.account_id] = copy.deepcopy(item)


class InMemoryAlertSink:
    def __init__(self) -> None:
        self.items: dict[tuple[str, str], dict[str, Any]] = {}

    def put_new(self, alerts: Sequence[Alert], detected_at: datetime, expires_at: int) -> list[Alert]:
        created = []
        for alert in alerts:
            key = (alert.account_id, alert.alert_key)
            if key in self.items:
                continue
            record = alert.to_record(detected_at)
            record["expires_at"] = expires_at
            self.items[key] = record
            created.append(alert)
        return created

    def all_alerts(self) -> list[Alert]:
        return [Alert.from_record(r) for r in self.items.values()]


class DynamoDBStateStore:
    """One item per account. Reads are strongly consistent; writes use optimistic locking."""

    def __init__(self, dynamodb_resource: Any, table_name: str) -> None:
        self.resource = dynamodb_resource
        self.table_name = table_name
        self.table = dynamodb_resource.Table(table_name)

    def load(self, account_ids: Iterable[str]) -> dict[str, AccountState]:
        ids = list(dict.fromkeys(account_ids))
        found: dict[str, AccountState] = {}
        for start in range(0, len(ids), _BATCH_GET_LIMIT):
            keys = [{"account_id": a} for a in ids[start : start + _BATCH_GET_LIMIT]]
            request = {self.table_name: {"Keys": keys, "ConsistentRead": True}}
            for attempt in range(6):
                response = self.resource.batch_get_item(RequestItems=request)
                for item in response.get("Responses", {}).get(self.table_name, []):
                    state = AccountState.from_item(item)
                    found[state.account_id] = state
                request = response.get("UnprocessedKeys") or {}
                if not request:
                    break
                time.sleep(min(0.05 * 2**attempt, 1.0))
            else:
                raise RuntimeError("DynamoDB kept returning unprocessed keys (throttled?)")
        return {a: found.get(a, AccountState(a)) for a in ids}

    def save(self, states: Iterable[AccountState], expires_at: int) -> None:
        client_errors = self.resource.meta.client.exceptions
        for state in states:
            item = state.to_item(expires_at)
            item["version"] = state.version + 1
            if state.version == 0:
                condition = {"ConditionExpression": "attribute_not_exists(account_id)"}
            else:
                condition = {
                    "ConditionExpression": "version = :expected",
                    "ExpressionAttributeValues": {":expected": state.version},
                }
            try:
                self.table.put_item(Item=item, **condition)
            except client_errors.ConditionalCheckFailedException as exc:
                raise ConflictError(state.account_id) from exc


class DynamoDBAlertSink:
    def __init__(self, dynamodb_resource: Any, table_name: str) -> None:
        self.table = dynamodb_resource.Table(table_name)
        self.client_errors = dynamodb_resource.meta.client.exceptions

    def put_new(self, alerts: Sequence[Alert], detected_at: datetime, expires_at: int) -> list[Alert]:
        created = []
        for alert in alerts:
            item = alert.to_record(detected_at)
            item["expires_at"] = expires_at
            try:
                self.table.put_item(Item=item, ConditionExpression="attribute_not_exists(account_id)")
            except self.client_errors.ConditionalCheckFailedException:
                logger.info("alert already exists, skipping: %s", alert.alert_key)
                continue
            created.append(alert)
        return created
