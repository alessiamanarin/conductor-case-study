"""Search attributes: the fields operators can search on in the Temporal UI."""

from __future__ import annotations

from datetime import datetime

from temporalio.common import SearchAttributeKey, SearchAttributePair, TypedSearchAttributes

APP_STATE = SearchAttributeKey.for_keyword("AppState")
LOAN_PRODUCT = SearchAttributeKey.for_keyword("LoanProduct")
SLA_DEADLINE = SearchAttributeKey.for_datetime("SlaDeadline")  # received_at + 48h


def initial(state: str, product: str, sla_deadline: datetime) -> TypedSearchAttributes:
    return TypedSearchAttributes([
        SearchAttributePair(APP_STATE, state),
        SearchAttributePair(LOAN_PRODUCT, product),
        SearchAttributePair(SLA_DEADLINE, sla_deadline),
    ])
