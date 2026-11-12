"""DEMO ONLY: fake third parties, steered by ``Applicant.demo_profile`` tokens.

| token          | behaviour                                                          |
|----------------|--------------------------------------------------------------------|
| prime          | credit score 780, identity verified, fraud 2%                      |
| near_prime     | credit score 660 (personal loans refer to a human below 700)       |
| outage_credit  | credit provider down until a reviewer chooses RETRY                |
| id_unverified  | identity check returns verified=False                              |
| funding_reject | the receiving bank rejects the account                             |
"""

from __future__ import annotations

from loan_origination.models import CreditReport, FraudResult, IdentityResult, ProviderRequest


class ProviderUnavailable(Exception):
    pass


class AccountRejected(Exception):
    pass


def credit_report(req: ProviderRequest) -> CreditReport:
    if "outage_credit" in req.demo_profile and req.attempt_round == 1:
        raise ProviderUnavailable("credit bureau returned 503")
    score = 660 if "near_prime" in req.demo_profile else 780
    return CreditReport(score=score)


def identity_check(req: ProviderRequest) -> IdentityResult:
    return IdentityResult(verified="id_unverified" not in req.demo_profile)


def fraud_score(req: ProviderRequest) -> FraudResult:
    return FraudResult(score_bps=200)


def bank_transfer(demo_profile: list[str]) -> None:
    if "funding_reject" in demo_profile:
        raise AccountRejected("receiving bank rejected the account")
