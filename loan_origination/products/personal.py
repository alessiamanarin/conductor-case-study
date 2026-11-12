"""Personal loan rules: the money goes to the applicant."""

from __future__ import annotations

from loan_origination.models import Decision, Payout, Product, UnderwritingRequest, UnderwritingResult
from loan_origination.products.base import RulesError

RULES_VERSION = "personal@2026.11.0"

DECLINE_SCORE_BELOW = 580
DECLINE_FRAUD_AT_OR_ABOVE_BPS = 9000
REFER_SCORE_BELOW = 700
REFER_FRAUD_AT_OR_ABOVE_BPS = 3000
REFER_AMOUNT_ABOVE_CENTS = 25_000_00


class PersonalUnderwriter:
    product = Product.PERSONAL
    rules_version = RULES_VERSION

    def underwrite(self, req: UnderwritingRequest) -> UnderwritingResult:
        if req.product != Product.PERSONAL:
            raise RulesError(f"personal rules got a {req.product.value} application")
        score, fraud = req.credit.score, req.fraud.score_bps

        declines = []
        if score < DECLINE_SCORE_BELOW:
            declines.append(f"credit score {score} < {DECLINE_SCORE_BELOW}")
        if fraud >= DECLINE_FRAUD_AT_OR_ABOVE_BPS:
            declines.append(f"fraud score {fraud}bps >= {DECLINE_FRAUD_AT_OR_ABOVE_BPS}bps")
        if declines:
            return UnderwritingResult(Decision.DECLINE, declines, RULES_VERSION)

        payouts = [Payout("applicant", req.amount_cents)]
        refers = []
        if score < REFER_SCORE_BELOW:
            refers.append(f"credit score {score} < {REFER_SCORE_BELOW}")
        if not req.identity.verified:
            refers.append("identity not verified")
        if fraud >= REFER_FRAUD_AT_OR_ABOVE_BPS:
            refers.append(f"fraud score {fraud}bps >= {REFER_FRAUD_AT_OR_ABOVE_BPS}bps")
        if req.amount_cents > REFER_AMOUNT_ABOVE_CENTS:
            refers.append(f"amount {req.amount_cents} > {REFER_AMOUNT_ABOVE_CENTS} cents")
        if refers:
            return UnderwritingResult(Decision.REFER, refers, RULES_VERSION, payouts)
        return UnderwritingResult(Decision.APPROVE, ["all checks passed"], RULES_VERSION, payouts)
