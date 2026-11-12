"""Third-party provider activities: credit, identity, fraud. Each runs on its own task queue.

A provider outage is raised as a retryable ``ApplicationError``; Temporal retries it with
backoff until the workflow's deadline, then the workflow asks a human.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from temporalio import activity
from temporalio.exceptions import ApplicationError

from loan_origination import fake_providers as fp
from loan_origination.models import CreditReport, FraudResult, IdentityResult, ProviderRequest

T = TypeVar("T")


def _call(fn: Callable[[ProviderRequest], T], req: ProviderRequest) -> T:
    try:
        return fn(req)
    except fp.ProviderUnavailable as e:
        raise ApplicationError(str(e), type="ProviderUnavailable") from e


@activity.defn
def pull_credit_report(req: ProviderRequest) -> CreditReport:
    return _call(fp.credit_report, req)


@activity.defn
def verify_identity(req: ProviderRequest) -> IdentityResult:
    return _call(fp.identity_check, req)


@activity.defn
def score_fraud(req: ProviderRequest) -> FraudResult:
    return _call(fp.fraud_score, req)
