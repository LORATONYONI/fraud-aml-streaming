"""SQLite-backed state and alert stores for the local Kafka mode.

This plays the role of a Kafka Streams state store: the detector keeps each account's history
across restarts. It has the same semantics as the DynamoDB stores (versioned writes, idempotent
alerts).

Several detector processes in one consumer group can share the file. Each wraps a whole batch in
``transaction(conn)``, which holds SQLite's write lock from read to write. Optimistic locking alone
would livelock here: busy detectors update the same accounts (the receivers of transfers) all the
time, so their retries keep colliding.
"""

from __future__ import annotations

import itertools
import json
import sqlite3
import time
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from fraud_aml.engine import ConflictError
from fraud_aml.models import Alert
from fraud_aml.state import AccountState

_IN_CLAUSE_CHUNK = 500

SCHEMA = """
CREATE TABLE IF NOT EXISTS account_state (
    account_id TEXT PRIMARY KEY,
    version    INTEGER NOT NULL,
    item       TEXT NOT NULL,
    expires_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS alerts (
    account_id TEXT NOT NULL,
    alert_key  TEXT NOT NULL,
    txn_id     TEXT NOT NULL,
    rule_id    TEXT NOT NULL,
    severity   TEXT NOT NULL,
    event_ts   TEXT NOT NULL,
    record     TEXT NOT NULL,
    PRIMARY KEY (account_id, alert_key)
);
"""


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    raise TypeError(f"cannot serialise {type(value).__name__}")


_savepoints = itertools.count()


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[None]:
    """Write transaction that nests: inside an open transaction it becomes a savepoint."""
    if conn.in_transaction:
        name = f"sp_{next(_savepoints)}"
        conn.execute(f"SAVEPOINT {name}")
        try:
            yield
        except BaseException:
            conn.execute(f"ROLLBACK TO {name}")
            conn.execute(f"RELEASE {name}")
            raise
        conn.execute(f"RELEASE {name}")
        return
    conn.execute("BEGIN IMMEDIATE")  # takes the write lock now; other processes wait (busy timeout)
    try:
        yield
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")


def connect(path: str | Path, init_timeout_s: float = 30) -> sqlite3.Connection:
    if str(path) != ":memory:":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30, isolation_level=None)  # explicit transactions
    # Several detectors may start at once on a new file. Switching to WAL mode does not wait on
    # SQLite's busy handler, so retry initialisation until the other process is done with it.
    deadline = time.monotonic() + init_timeout_s
    while True:
        try:
            conn.execute("PRAGMA journal_mode=WAL")  # readers don't block the writer
            conn.executescript(SCHEMA)
            return conn
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc) or time.monotonic() > deadline:
                conn.close()
                raise
            time.sleep(0.1)


class SqliteStateStore:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def load(self, account_ids: Iterable[str]) -> dict[str, AccountState]:
        ids = list(dict.fromkeys(account_ids))
        found: dict[str, AccountState] = {}
        for start in range(0, len(ids), _IN_CLAUSE_CHUNK):
            chunk = ids[start : start + _IN_CLAUSE_CHUNK]
            placeholders = ",".join("?" * len(chunk))
            rows = self.conn.execute(
                f"SELECT account_id, version, item FROM account_state WHERE account_id IN ({placeholders})", chunk
            )
            for account_id, version, item in rows:
                state = AccountState.from_item(json.loads(item))
                state.version = version
                found[account_id] = state
        return {a: found.get(a, AccountState(a)) for a in ids}

    def save(self, states: Iterable[AccountState], expires_at: int) -> None:
        """Write all states atomically; any version mismatch rolls back the lot."""
        with transaction(self.conn):
            for state in states:
                item = json.dumps(state.to_item(expires_at), default=_json_default)
                if state.version == 0:
                    try:
                        self.conn.execute(
                            "INSERT INTO account_state (account_id, version, item, expires_at) VALUES (?, 1, ?, ?)",
                            (state.account_id, item, expires_at),
                        )
                    except sqlite3.IntegrityError as exc:
                        raise ConflictError(state.account_id) from exc
                else:
                    cursor = self.conn.execute(
                        "UPDATE account_state SET version = version + 1, item = ?, expires_at = ? "
                        "WHERE account_id = ? AND version = ?",
                        (item, expires_at, state.account_id, state.version),
                    )
                    if cursor.rowcount != 1:
                        raise ConflictError(state.account_id)


class SqliteAlertSink:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def put_new(self, alerts: Sequence[Alert], detected_at: datetime, expires_at: int) -> list[Alert]:
        created = []
        with transaction(self.conn):
            for alert in alerts:
                record = alert.to_record(detected_at)
                cursor = self.conn.execute(
                    "INSERT OR IGNORE INTO alerts (account_id, alert_key, txn_id, rule_id, severity, event_ts, record) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        alert.account_id,
                        alert.alert_key,
                        alert.txn_id,
                        alert.rule_id,
                        alert.severity,
                        record["event_ts"],
                        json.dumps(record, default=_json_default),
                    ),
                )
                if cursor.rowcount == 1:
                    created.append(alert)
        return created

    def all_alerts(self) -> list[Alert]:
        rows = self.conn.execute("SELECT record FROM alerts ORDER BY event_ts")
        return [Alert.from_record(json.loads(record)) for (record,) in rows]
