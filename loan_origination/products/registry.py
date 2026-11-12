"""Product -> rules. Adding a product = one rules module, one entry here, one policy entry."""

from __future__ import annotations

from loan_origination.models import Product
from loan_origination.products.base import RulesError, Underwriter
from loan_origination.products.debt_consolidation import DebtConsolidationUnderwriter
from loan_origination.products.personal import PersonalUnderwriter

REGISTRY: dict[Product, Underwriter] = {
    Product.PERSONAL: PersonalUnderwriter(),
    Product.DEBT_CONSOLIDATION: DebtConsolidationUnderwriter(),
}


def underwriter_for(product: Product) -> Underwriter:
    try:
        return REGISTRY[product]
    except KeyError:
        raise RulesError(f"no rules registered for {product.value}") from None
