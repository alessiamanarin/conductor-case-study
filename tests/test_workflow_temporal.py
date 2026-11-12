"""The whole workflow against Temporal's time-skipping test server (timers run instantly).

Skipped, with the reason, if the test server can't be started (e.g. offline).
"""

from __future__ import annotations

import asyncio
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import AsyncExitStack, asynccontextmanager

import pytest
import pytest_asyncio
from factories import app_input
from temporalio import activity
from temporalio.api.enums.v1 import IndexedValueType
from temporalio.api.operatorservice.v1 import AddSearchAttributesRequest
from temporalio.client import WorkflowExecutionStatus
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from loan_origination.activities import funding, ops
from loan_origination.intake import start_application
from loan_origination.models import (
    AppState,
    EscalationReason,
    ReviewAction,
    UnderwritingRequest,
    UnderwritingResult,
)
from loan_origination.review_cli import ReviewRejected, get_status, submit_decision
from loan_origination.worker import worker_specs

pytestmark = pytest.mark.temporal


@pytest_asyncio.fixture(scope="session")
async def env():
    try:
        e = await WorkflowEnvironment.start_time_skipping()
    except Exception as ex:
        pytest.skip(f"Temporal test server unavailable ({ex!r}); these tests were not run")
    # The same search attributes the dev server is started with (see Makefile).
    await e.client.operator_service.add_search_attributes(AddSearchAttributesRequest(
        namespace=e.client.namespace,
        search_attributes={
            "AppState": IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD,
            "LoanProduct": IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD,
            "SlaDeadline": IndexedValueType.INDEXED_VALUE_TYPE_DATETIME,
        },
    ))
    yield e
    await e.shutdown()


@asynccontextmanager
async def workers(env, replace_orchestration_activities=None):
    pool = ThreadPoolExecutor(max_workers=16)
    async with AsyncExitStack() as stack:
        for s in worker_specs():
            acts = s.activities
            if replace_orchestration_activities and s.task_queue == "loan-orchestration":
                acts = replace_orchestration_activities
            await stack.enter_async_context(Worker(env.client, task_queue=s.task_queue,
                                                   workflows=s.workflows, activities=acts,
                                                   activity_executor=pool, **s.options))
        yield


def new_ref(name: str) -> str:
    return f"{name}-{uuid.uuid4().hex[:8]}"


async def wait_for(env, app_id: str, state: AppState):
    for _ in range(600):
        st = await get_status(env.client, app_id)
        if st.state == state:
            return st
        await asyncio.sleep(0.1)
    raise AssertionError(f"never reached {state}; stuck in {st.state}")


async def test_happy_path_funds_once_and_ignores_duplicates(env, db) -> None:
    app = app_input(new_ref("happy"))
    async with workers(env):
        h = await start_application(env.client, app)
        h2 = await start_application(env.client, app)  # the same application again
        assert (await h.describe()).run_id == (await h2.describe()).run_id
        result = await h.result()
    assert result.final_state == AppState.FUNDED
    assert len(db.disbursements(app.application_id)) == 1
    assert len(db.billing_events(app.application_id)) == 1


async def test_referral_waits_for_a_human_and_rejects_bad_decisions(env, db) -> None:
    app = app_input(new_ref("review"), profile=["near_prime"])
    c = env.client
    async with workers(env):
        h = await start_application(c, app)
        st = await wait_for(env, app.application_id, AppState.ESCALATED)
        assert st.pending_review.reason == EscalationReason.UNDERWRITING_REFERRAL
        with pytest.raises(ReviewRejected, match="not allowed"):
            await submit_decision(c, app.application_id, ReviewAction.RETRY, "r.lee", "again")
        await submit_decision(c, app.application_id, ReviewAction.APPROVE, "r.lee", "stable income")
        with pytest.raises(ReviewRejected):  # double-click
            await submit_decision(c, app.application_id, ReviewAction.APPROVE, "r.lee", "again",
                                  task_id=st.pending_review.task_id)
        result = await h.result()
    assert result.final_state == AppState.FUNDED
    decisions = [r for r in db.audit_trail(app.application_id) if r["event_type"] == "human_decision"]
    assert len(decisions) == 1 and decisions[0]["actor"] == "r.lee"


async def test_provider_outage_goes_to_a_human_and_retry_calls_only_the_failed_one(env, db) -> None:
    app = app_input(new_ref("outage"), profile=["prime", "outage_credit"])
    async with workers(env):
        h = await start_application(env.client, app)
        st = await wait_for(env, app.application_id, AppState.ESCALATED)
        assert st.pending_review.context["failed"] == {"credit": "ProviderUnavailable"}
        await submit_decision(env.client, app.application_id, ReviewAction.RETRY, "r.lee", "back up")
        result = await h.result()
    assert result.final_state == AppState.FUNDED
    fetched = [r["data"]["provider"] for r in db.audit_trail(app.application_id)
               if r["event_type"] == "provider_result"]
    assert sorted(fetched) == ["credit", "fraud", "identity"]  # each fetched exactly once


async def test_a_bug_goes_to_a_human_instead_of_failing(env, db) -> None:
    @activity.defn(name="underwrite_application")
    def broken(req: UnderwritingRequest) -> UnderwritingResult:
        raise RuntimeError("bug in the rules")

    app = app_input(new_ref("bug"))
    async with workers(env, [*ops.ALL, broken, funding.disburse_funds]):
        h = await start_application(env.client, app)
        st = await wait_for(env, app.application_id, AppState.ESCALATED)
        assert st.pending_review.reason == EscalationReason.UNEXPECTED_FAILURE
        await submit_decision(env.client, app.application_id, ReviewAction.DECLINE, "r.lee", "bug")
        result = await h.result()
    assert result.final_state == AppState.DECLINED
    assert (await h.describe()).status == WorkflowExecutionStatus.COMPLETED  # not Failed
