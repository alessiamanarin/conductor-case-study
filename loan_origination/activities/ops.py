"""Internal activities: validation, the audit log and the billing event.

All idempotent on a fixed key, so the workflow can retry them without limit: a database
outage delays an application but never loses or duplicates a record.
"""

from __future__ import annotations

import re

from temporalio import activity

from loan_origination.models import ApplicationInput, AuditEvent, BillingEvent, ValidationResult
from loan_origination.policy import POLICIES
from loan_origination.store import get_store

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


@activity.defn
def validate_application(app: ApplicationInput) -> ValidationResult:
    """Invalid data -> decline. Valid but missing documents -> a human follows up."""
    pol = POLICIES.get(app.product)
    if pol is None:
        return ValidationResult(False, [f"product {app.product.value} is not offered"])
    reasons = []
    if not pol.min_amount_cents <= app.amount_cents <= pol.max_amount_cents:
        reasons.append(f"amount {app.amount_cents} outside "
                       f"[{pol.min_amount_cents}, {pol.max_amount_cents}]")
    if not pol.min_term_months <= app.term_months <= pol.max_term_months:
        reasons.append(f"term {app.term_months} outside [{pol.min_term_months}, {pol.max_term_months}]")
    if app.applicant.annual_income_cents <= 0:
        reasons.append("income missing")
    if not EMAIL_RE.match(app.applicant.email or ""):
        reasons.append("email invalid")
    missing = sorted(set(pol.required_documents) - set(app.documents))
    return ValidationResult(valid=not reasons, reasons=reasons, missing_documents=missing)


@activity.defn
def record_event(ev: AuditEvent) -> bool:
    """Append-only audit insert keyed (application_id, seq)."""
    return get_store().append_audit(ev.application_id, ev.seq, ev.event_type, ev.state.value,
                                    ev.actor, ev.at.isoformat(), ev.data)


@activity.defn
def emit_billing_event(ev: BillingEvent) -> bool:
    """The lending team bills on disbursed loans: one billing event per application, ever."""
    return get_store().add_billing_event(ev.application_id, ev.product, ev.amount_cents,
                                         ev.at.isoformat())


ALL = [validate_application, record_event, emit_billing_event]
