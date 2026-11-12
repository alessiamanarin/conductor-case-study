"""Debt consolidation rules: the loan pays the applicant's creditors directly."""

from __future__ import annotations

from typing import Any

from loan_origination.models import Decision, Payout, Product, UnderwritingRequest, UnderwritingResult
from loan_origination.products.base import RulesError

RULES_VERSION = "debt_consolidation@2026.11.0"

DECLINE_SCORE_BELOW = 600
DECLINE_FRAUD_AT_OR_ABOVE_BPS = 9000
REFER_SCORE_BELOW = 680
REFER_FRAUD_AT_OR_ABOVE_BPS = 3000


def payouts_for(amount_cents: int, debts: list[dict[str, Any]]) -> list[Payout]:
    """Each creditor receives exactly its balance; any remainder goes to the applicant."""
    payouts = [Payout(str(d["creditor"]), int(d["balance_cents"])) for d in debts]
    remainder = amount_cents - sum(p.amount_cents for p in payouts)
    if remainder < 0:
        raise RulesError("loan amount is lower than the balances it must pay off")
    if remainder:
        payouts.append(Payout("applicant", remainder))
    return payouts


class DebtConsolidationUnderwriter:
    product = Product.DEBT_CONSOLIDATION
    rules_version = RULES_VERSION

    def underwrite(self, req: UnderwritingRequest) -> UnderwritingResult:
        if req.product != Product.DEBT_CONSOLIDATION:
            raise RulesError(f"debt_consolidation rules got a {req.product.value} application")
        debts = req.product_data.get("debts", [])
        score, fraud = req.credit.score, req.fraud.score_bps

        declines = []
        if not debts:
            declines.append("no debts to consolidate")
        if score < DECLINE_SCORE_BELOW:
            declines.append(f"credit score {score} < {DECLINE_SCORE_BELOW}")
        if fraud >= DECLINE_FRAUD_AT_OR_ABOVE_BPS:
            declines.append(f"fraud score {fraud}bps >= {DECLINE_FRAUD_AT_OR_ABOVE_BPS}bps")
        if declines:
            return UnderwritingResult(Decision.DECLINE, declines, RULES_VERSION)

        payouts = payouts_for(req.amount_cents, debts)
        refers = []
        if score < REFER_SCORE_BELOW:
            refers.append(f"credit score {score} < {REFER_SCORE_BELOW}")
        if not req.identity.verified:
            refers.append("identity not verified")
        if fraud >= REFER_FRAUD_AT_OR_ABOVE_BPS:
            refers.append(f"fraud score {fraud}bps >= {REFER_FRAUD_AT_OR_ABOVE_BPS}bps")
        if refers:
            return UnderwritingResult(Decision.REFER, refers, RULES_VERSION, payouts)
        return UnderwritingResult(Decision.APPROVE, ["all checks passed"], RULES_VERSION, payouts)
