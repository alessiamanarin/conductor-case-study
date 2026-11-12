"""Per-product data (documents, limits) plus task queues and timings. No I/O, workflow-safe."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from loan_origination.models import Product, TimingProfile

SLA_BUDGET = timedelta(hours=48)  # every application should be funded within 48h of receipt

# Task queues. Each provider gets its own queue so Temporal can rate-limit it on the server
# (the limit holds across every worker) and an outage at one provider only backs up its queue.
ORCHESTRATION_QUEUE = "loan-orchestration"
CREDIT_QUEUE = "provider-credit"
IDENTITY_QUEUE = "provider-identity"
FRAUD_QUEUE = "provider-fraud"
PROVIDER_RATE_LIMITS: dict[str, float] = {CREDIT_QUEUE: 5.0, IDENTITY_QUEUE: 10.0, FRAUD_QUEUE: 10.0}


@dataclass(frozen=True)
class ProductPolicy:
    """Document requirements and limits for one product. The rules live in products/."""

    product: Product
    required_documents: tuple[str, ...]
    min_amount_cents: int
    max_amount_cents: int
    min_term_months: int
    max_term_months: int


POLICIES: dict[Product, ProductPolicy] = {
    Product.PERSONAL: ProductPolicy(
        Product.PERSONAL, ("government_id", "proof_of_income"), 1_000_00, 50_000_00, 12, 60,
    ),
    Product.DEBT_CONSOLIDATION: ProductPolicy(
        Product.DEBT_CONSOLIDATION, ("government_id", "proof_of_income", "creditor_statements"),
        2_000_00, 75_000_00, 24, 84,
    ),
}


@dataclass(frozen=True)
class Timings:
    provider_deadline: timedelta  # total retry budget for one provider call
    funding_deadline: timedelta  # total retry budget for one disbursement


TIMINGS: dict[TimingProfile, Timings] = {
    TimingProfile.PROD: Timings(timedelta(minutes=10), timedelta(hours=2)),
    TimingProfile.DEMO: Timings(timedelta(seconds=3), timedelta(seconds=10)),
}
