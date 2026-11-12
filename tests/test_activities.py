"""Activities called directly (no Temporal server): running one twice has no extra effect."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from factories import app_input
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from loan_origination.activities import funding, ops
from loan_origination.models import AppState, AuditEvent, DisbursementRequest, Payout

NOW = datetime(2026, 11, 11, 9, 0, tzinfo=UTC)
APP = "loan-portal-act"


def run(fn, *args):
    return ActivityEnvironment().run(fn, *args)


def test_validation() -> None:
    assert run(ops.validate_application, app_input("v1")).valid
    missing = run(ops.validate_application, app_input("v2", docs=["proof_of_income"]))
    assert missing.valid and missing.missing_documents == ["government_id"]
    assert not run(ops.validate_application, app_input("v3", amount=1)).valid


def test_audit_write_is_idempotent(db) -> None:
    ev = AuditEvent(APP, 1, "state_changed", AppState.VALIDATING, NOW)
    run(ops.record_event, ev)
    run(ops.record_event, ev)  # a retry
    assert len(db.audit_trail(APP)) == 1


def disbursement(profile: list[str]) -> DisbursementRequest:
    return DisbursementRequest(APP, f"{APP}-disburse", 15_000_00, [Payout("applicant", 15_000_00)],
                               profile)


def test_disbursement_never_pays_twice(db) -> None:
    first = run(funding.disburse_funds, disbursement([]))
    again = run(funding.disburse_funds, disbursement([]))  # e.g. a retry after a crash
    assert first == again and len(db.disbursements(APP)) == 1


def test_bank_rejection_is_not_retried(db) -> None:
    with pytest.raises(ApplicationError) as e:
        run(funding.disburse_funds, disbursement(["funding_reject"]))
    assert e.value.type == "AccountRejected" and e.value.non_retryable
