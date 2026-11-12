from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))


@pytest.fixture
def db(tmp_path, monkeypatch):
    """Fresh SQLite DB per test; activities resolve the path from env at call time."""
    path = tmp_path / "loan.db"
    monkeypatch.setenv("LOAN_DB_PATH", str(path))
    from loan_origination.store import get_store

    return get_store()
