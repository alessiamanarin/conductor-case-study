"""SQLite: the audit log (system of record), disbursements and billing events.

Activities can run more than once (a retry after a worker crash), so every write is
``INSERT OR IGNORE`` on a fixed key: running it again has no extra effect.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS audit_log (
    application_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    state TEXT NOT NULL,
    actor TEXT NOT NULL,
    at TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (application_id, seq)
);
CREATE TABLE IF NOT EXISTS disbursements (
    idempotency_key TEXT PRIMARY KEY,
    application_id TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    payouts TEXT NOT NULL,
    receipt_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS billing_events (
    application_id TEXT PRIMARY KEY,
    product TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    at TEXT NOT NULL
);
"""


class Store:
    def __init__(self, path: str) -> None:
        self.path = path
        with self.conn() as c:
            c.executescript(SCHEMA)

    @contextmanager
    def conn(self) -> Iterator[sqlite3.Connection]:
        # One connection per call keeps it safe for the activity thread pool.
        c = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        try:
            yield c
        finally:
            c.close()

    def append_audit(self, application_id: str, seq: int, event_type: str, state: str,
                     actor: str, at: str, data: dict[str, Any]) -> bool:
        with self.conn() as c:
            cur = c.execute("INSERT OR IGNORE INTO audit_log VALUES (?,?,?,?,?,?,?)",
                            (application_id, seq, event_type, state, actor, at, _j(data)))
            return cur.rowcount == 1

    def audit_trail(self, application_id: str) -> list[dict[str, Any]]:
        with self.conn() as c:
            rows = c.execute("SELECT * FROM audit_log WHERE application_id=? ORDER BY seq",
                             (application_id,)).fetchall()
        return [{**dict(r), "data": json.loads(r["data"])} for r in rows]

    def get_disbursement(self, key: str) -> dict[str, Any] | None:
        with self.conn() as c:
            r = c.execute("SELECT * FROM disbursements WHERE idempotency_key=?", (key,)).fetchone()
        return dict(r) if r else None

    def add_disbursement(self, key: str, application_id: str, amount_cents: int,
                         payouts: list[dict[str, Any]], receipt_id: str) -> bool:
        with self.conn() as c:
            cur = c.execute("INSERT OR IGNORE INTO disbursements VALUES (?,?,?,?,?)",
                            (key, application_id, amount_cents, _j(payouts), receipt_id))
            return cur.rowcount == 1

    def disbursements(self, application_id: str) -> list[dict[str, Any]]:
        with self.conn() as c:
            rows = c.execute("SELECT * FROM disbursements WHERE application_id=?",
                             (application_id,)).fetchall()
        return [{**dict(r), "payouts": json.loads(r["payouts"])} for r in rows]

    def add_billing_event(self, application_id: str, product: str, amount: int, at: str) -> bool:
        with self.conn() as c:
            cur = c.execute("INSERT OR IGNORE INTO billing_events VALUES (?,?,?,?)",
                            (application_id, product, amount, at))
            return cur.rowcount == 1

    def billing_events(self, application_id: str) -> list[dict[str, Any]]:
        with self.conn() as c:
            rows = c.execute("SELECT * FROM billing_events WHERE application_id=?",
                             (application_id,)).fetchall()
        return [dict(r) for r in rows]


def _j(v: Any) -> str:
    return json.dumps(v, sort_keys=True, default=str)


_stores: dict[str, Store] = {}
_lock = threading.Lock()


def get_store() -> Store:
    """Activities read the DB path at call time (never inside workflow code)."""
    path = os.environ.get("LOAN_DB_PATH", "loan.db")
    with _lock:
        if path not in _stores:
            _stores[path] = Store(path)
        return _stores[path]
