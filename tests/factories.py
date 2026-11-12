"""Shared builders for tests."""

from __future__ import annotations

from datetime import UTC, datetime

from loan_origination.models import (
    Applicant,
    ApplicationInput,
    Channel,
    CreditReport,
    FraudResult,
    IdentityResult,
    Product,
    TimingProfile,
    UnderwritingRequest,
    application_id_for,
)

DEBTS = [
    {"creditor": "card_a", "balance_cents": 8_000_00},
    {"creditor": "card_b", "balance_cents": 4_000_00},
]


def uw_request(product: Product = Product.PERSONAL, *, score: int = 780, fraud_bps: int = 200,
               verified: bool = True, amount: int = 15_000_00) -> UnderwritingRequest:
    data = {"debts": DEBTS} if product == Product.DEBT_CONSOLIDATION else {}
    return UnderwritingRequest("loan-portal-t1", product, amount, CreditReport(score),
                               IdentityResult(verified), FraudResult(fraud_bps), data)


def app_input(ref: str, *, product: Product = Product.PERSONAL, profile: list[str] | None = None,
              docs: list[str] | None = None, amount: int = 15_000_00) -> ApplicationInput:
    if docs is None:
        docs = ["government_id", "proof_of_income"]
        if product == Product.DEBT_CONSOLIDATION:
            docs.append("creditor_statements")
    return ApplicationInput(
        application_id=application_id_for(Channel.PORTAL, ref),
        channel=Channel.PORTAL,
        external_ref=ref,
        product=product,
        applicant=Applicant("Dana Demo", "dana@example.test", "1990-04-01", 9_000_000,
                            profile if profile is not None else ["prime"]),
        amount_cents=amount,
        term_months=36,
        received_at=datetime(2026, 11, 11, 9, 0, tzinfo=UTC),
        documents=docs,
        product_data={"debts": DEBTS} if product == Product.DEBT_CONSOLIDATION else {},
        timing_profile=TimingProfile.DEMO,
    )
