"""Product rules: each product decides with its own thresholds, and they stay separate."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest
from factories import uw_request

from loan_origination.models import Decision, Product
from loan_origination.products import personal
from loan_origination.products.base import RulesError
from loan_origination.products.registry import underwriter_for

DC = Product.DEBT_CONSOLIDATION


@pytest.mark.parametrize("req,expected", [
    (uw_request(), Decision.APPROVE),
    (uw_request(score=579), Decision.DECLINE),
    (uw_request(score=699), Decision.REFER),
    (uw_request(fraud_bps=9000), Decision.DECLINE),
    (uw_request(verified=False), Decision.REFER),
    (uw_request(amount=25_000_01), Decision.REFER),
])
def test_personal_rules(req, expected) -> None:
    assert underwriter_for(Product.PERSONAL).underwrite(req).decision == expected


@pytest.mark.parametrize("req,expected", [
    (uw_request(DC), Decision.APPROVE),
    (uw_request(DC, score=599), Decision.DECLINE),
    (uw_request(DC, score=679), Decision.REFER),  # 679 approves a personal loan but refers here
    (uw_request(DC, verified=False), Decision.REFER),
])
def test_debt_consolidation_rules(req, expected) -> None:
    assert underwriter_for(DC).underwrite(req).decision == expected


def test_debt_consolidation_pays_creditors_directly() -> None:
    res = underwriter_for(DC).underwrite(uw_request(DC, amount=15_000_00))
    assert [(p.payee, p.amount_cents) for p in res.payouts] == [
        ("card_a", 8_000_00), ("card_b", 4_000_00), ("applicant", 3_000_00)]


def test_auto_is_not_coded() -> None:
    with pytest.raises(RulesError):
        underwriter_for(Product.AUTO)


def test_products_do_not_import_each_other() -> None:
    pkg = Path(personal.__file__).parent
    for name, other in [("personal", "debt_consolidation"), ("debt_consolidation", "personal")]:
        tree = ast.parse((pkg / f"{name}.py").read_text())
        modules = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
        assert not any(m.endswith(other) for m in modules)
