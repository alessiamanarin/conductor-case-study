"""Underwriting activity: runs the rules module for the application's product.

The workflow never imports product rules, so changing a product's rules never touches the
workflow or any application already in progress.
"""

from __future__ import annotations

from temporalio import activity
from temporalio.exceptions import ApplicationError

from loan_origination.models import UnderwritingRequest, UnderwritingResult
from loan_origination.products.base import RulesError
from loan_origination.products.registry import underwriter_for


@activity.defn
def underwrite_application(req: UnderwritingRequest) -> UnderwritingResult:
    try:
        return underwriter_for(req.product).underwrite(req)
    except RulesError as e:
        # Retrying cannot fix bad data or a missing product: go straight to a human.
        raise ApplicationError(str(e), type="RulesError", non_retryable=True) from e
