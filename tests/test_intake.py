"""The three channel payloads become applications with predictable ids."""

from __future__ import annotations

import json
from pathlib import Path

from loan_origination.intake import parse_payload

SAMPLES = Path(__file__).resolve().parent.parent / "samples"


def ids(name: str) -> list[str]:
    return [a.application_id for a in parse_payload(json.loads((SAMPLES / name).read_text()))]


def test_each_channel_gives_one_id_per_application() -> None:
    assert ids("portal.json") == ["loan-portal-P-1001"]
    assert ids("broker_email.json") == ["loan-broker-BRK-2001"]
    assert ids("aggregator_batch.json") == ["loan-aggregator-AGG-3001", "loan-aggregator-AGG-3002"]


def test_same_payload_gives_the_same_ids() -> None:
    assert ids("aggregator_batch.json") == ids("aggregator_batch.json")  # so a resend is a no-op
