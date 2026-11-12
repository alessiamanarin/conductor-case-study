"""Disbursement. The idempotency key (``<application_id>-disburse``) means a retry can never
pay twice: if the money already moved, the existing receipt is returned.
"""

from __future__ import annotations

from dataclasses import asdict

from temporalio import activity
from temporalio.exceptions import ApplicationError

from loan_origination import fake_providers as fp
from loan_origination.models import Disbursement, DisbursementRequest
from loan_origination.store import get_store


@activity.defn
def disburse_funds(req: DisbursementRequest) -> Disbursement:
    s = get_store()
    existing = s.get_disbursement(req.idempotency_key)
    if existing:
        # Already paid, e.g. the worker crashed after the transfer but before reporting back.
        return Disbursement(existing["receipt_id"], existing["amount_cents"])
    try:
        fp.bank_transfer(req.demo_profile)
    except fp.AccountRejected as e:
        # A definite "no" from the bank: retrying will not help, so a human decides.
        raise ApplicationError(str(e), type="AccountRejected", non_retryable=True) from e
    receipt_id = f"dsb-{req.idempotency_key}"
    s.add_disbursement(req.idempotency_key, req.application_id, req.amount_cents,
                       [asdict(p) for p in req.payouts], receipt_id)
    return Disbursement(receipt_id, req.amount_cents)
