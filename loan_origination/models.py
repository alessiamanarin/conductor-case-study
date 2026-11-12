"""Plain dataclass models shared by the workflow, activities and products.

Money is in integer cents, rates in basis points (1 bps = 0.01%), datetimes are tz-aware.
No pydantic: these types must be importable inside the Temporal workflow sandbox.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class Channel(str, Enum):
    PORTAL = "portal"  # self-serve online portal
    BROKER = "broker"  # broker referral via email
    AGGREGATOR = "aggregator"  # batch feed from a financial aggregator


class Product(str, Enum):
    PERSONAL = "personal"
    DEBT_CONSOLIDATION = "debt_consolidation"
    AUTO = "auto"  # documented only: no rules module is registered for it


class TimingProfile(str, Enum):
    PROD = "prod"
    DEMO = "demo"  # shrinks provider/funding deadlines to seconds


class AppState(str, Enum):
    INGESTED = "INGESTED"
    VALIDATING = "VALIDATING"
    ENRICHING = "ENRICHING"
    UNDERWRITING = "UNDERWRITING"
    FUNDING = "FUNDING"
    FUNDED = "FUNDED"
    DECLINED = "DECLINED"
    ESCALATED = "ESCALATED"


class DeclineReason(str, Enum):
    INVALID_APPLICATION = "INVALID_APPLICATION"
    UNDERWRITING_DECLINED = "UNDERWRITING_DECLINED"
    REVIEWER_DECLINED = "REVIEWER_DECLINED"


class EscalationReason(str, Enum):
    MISSING_DOCUMENTS = "MISSING_DOCUMENTS"
    PROVIDER_UNAVAILABLE = "PROVIDER_UNAVAILABLE"
    UNDERWRITING_REFERRAL = "UNDERWRITING_REFERRAL"
    FUNDING_REJECTED = "FUNDING_REJECTED"
    UNEXPECTED_FAILURE = "UNEXPECTED_FAILURE"


class ReviewAction(str, Enum):
    APPROVE = "APPROVE"
    DECLINE = "DECLINE"
    RETRY = "RETRY"


class Decision(str, Enum):
    """Underwriting result: approve, decline, or refer to human review."""

    APPROVE = "APPROVE"
    DECLINE = "DECLINE"
    REFER = "REFER"


# --------------------------------------------------------------------------- ingestion


@dataclass
class Applicant:
    full_name: str
    email: str
    date_of_birth: str
    annual_income_cents: int
    # DEMO ONLY: tokens that steer the fake providers (see fake_providers.py).
    demo_profile: list[str] = field(default_factory=list)


@dataclass
class ApplicationInput:
    application_id: str
    channel: Channel
    external_ref: str
    product: Product
    applicant: Applicant
    amount_cents: int
    term_months: int
    received_at: datetime  # when the channel received it: the 48h SLA starts here
    documents: list[str] = field(default_factory=list)
    # Product-specific facts, e.g. debt consolidation: {"debts": [...]}.
    product_data: dict[str, Any] = field(default_factory=dict)
    # Part of the input (never read from env) so the workflow replays identically.
    timing_profile: TimingProfile = TimingProfile.PROD


def application_id_for(channel: Channel, external_ref: str) -> str:
    """Deterministic id: duplicates are ignored, and operators can look it up."""
    return f"loan-{channel.value}-{external_ref}"


@dataclass
class ValidationResult:
    valid: bool
    reasons: list[str] = field(default_factory=list)
    missing_documents: list[str] = field(default_factory=list)


# --------------------------------------------------------------------------- third-party providers


@dataclass
class ProviderRequest:
    application_id: str
    request_id: str  # idempotency key we would forward to the real provider
    attempt_round: int  # DEMO ONLY: lets the fake be "down" until a reviewer retries
    demo_profile: list[str]
    full_name: str
    date_of_birth: str


@dataclass
class CreditReport:
    score: int


@dataclass
class IdentityResult:
    verified: bool


@dataclass
class FraudResult:
    score_bps: int  # probability of fraud (9000 == 90%)


# --------------------------------------------------------------------------- underwriting


@dataclass
class Payout:
    payee: str
    amount_cents: int


@dataclass
class UnderwritingRequest:
    application_id: str
    product: Product
    amount_cents: int
    credit: CreditReport
    identity: IdentityResult
    fraud: FraudResult
    product_data: dict[str, Any] = field(default_factory=dict)


@dataclass
class UnderwritingResult:
    decision: Decision
    reasons: list[str]
    rules_version: str  # which rules decided, e.g. personal@2026.11.0 (for the audit)
    payouts: list[Payout] = field(default_factory=list)  # who gets the money; empty on decline


# --------------------------------------------------------------------------- funding


@dataclass
class DisbursementRequest:
    application_id: str
    idempotency_key: str
    amount_cents: int
    payouts: list[Payout]
    demo_profile: list[str] = field(default_factory=list)


@dataclass
class Disbursement:
    receipt_id: str
    amount_cents: int


@dataclass
class BillingEvent:
    application_id: str
    product: str
    amount_cents: int
    at: datetime


# --------------------------------------------------------------------------- human review


@dataclass
class ReviewDecision:
    task_id: str
    reviewer_id: str
    action: ReviewAction
    rationale: str


@dataclass
class ReviewAck:
    task_id: str
    message: str


@dataclass
class PendingReview:
    task_id: str
    reason: EscalationReason
    allowed_actions: list[ReviewAction]
    context: dict[str, Any]
    created_at: datetime
    decision: ReviewDecision | None = None


# --------------------------------------------------------------------------- audit, query, result


@dataclass
class AuditEvent:
    application_id: str
    seq: int  # from a counter in the workflow, so a retried write reuses the same key
    event_type: str
    state: AppState
    at: datetime
    actor: str = "system"
    data: dict[str, Any] = field(default_factory=dict)


@dataclass
class ApplicationStatus:
    application_id: str
    state: AppState
    product: Product
    channel: Channel
    sla_deadline: datetime
    pending_review: PendingReview | None
    decline_reason: str = ""


@dataclass
class ApplicationResult:
    application_id: str
    final_state: AppState
    reason: str = ""
    receipt_id: str = ""
