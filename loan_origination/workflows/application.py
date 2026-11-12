"""``LoanApplicationWorkflow``: one Temporal workflow per loan application.

Read ``run`` top to bottom: validate -> enrich -> underwrite -> fund. Whenever a person is
needed, the workflow calls ``_ask_human``, which moves to ESCALATED and waits for a decision
sent as a Temporal Update. If the person declines, the application ends DECLINED.

The workflow only orchestrates. Every side effect (database, providers, bank) is an activity.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import asdict
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from loan_origination import search_attrs as sa
    from loan_origination.activities import funding, ops, providers, underwriting
    from loan_origination.models import (
        ApplicationInput,
        ApplicationResult,
        ApplicationStatus,
        AppState,
        AuditEvent,
        BillingEvent,
        CreditReport,
        Decision,
        DeclineReason,
        DisbursementRequest,
        EscalationReason,
        FraudResult,
        IdentityResult,
        PendingReview,
        ProviderRequest,
        ReviewAck,
        ReviewAction,
        ReviewDecision,
        UnderwritingRequest,
        UnderwritingResult,
    )
    from loan_origination.policy import CREDIT_QUEUE, FRAUD_QUEUE, IDENTITY_QUEUE, SLA_BUDGET, TIMINGS
    from loan_origination.state_machine import assert_transition

A = ReviewAction

# Our own database writes: retry forever. They are safe to repeat, and a database outage
# should delay an application, never lose its audit record.
INTERNAL: dict[str, Any] = dict(
    start_to_close_timeout=timedelta(seconds=30),
    retry_policy=RetryPolicy(maximum_attempts=0, maximum_interval=timedelta(seconds=60)),
)


class Declined(Exception):
    """Raised anywhere to end the application as DECLINED."""

    def __init__(self, reason: DeclineReason) -> None:
        super().__init__(reason.value)
        self.reason = reason


def error_type(e: ActivityError) -> str:
    """The error's name, e.g. "ProviderUnavailable" or "AccountRejected"."""
    if isinstance(e.cause, ApplicationError):
        return e.cause.type or "ApplicationError"
    return type(e.cause).__name__


@workflow.defn
class LoanApplicationWorkflow:
    @workflow.init
    def __init__(self, app: ApplicationInput) -> None:
        self.app = app
        self.timings = TIMINGS[app.timing_profile]
        self.state = AppState.INGESTED
        self.seq = 0  # audit event counter
        self.credit: CreditReport | None = None
        self.identity: IdentityResult | None = None
        self.fraud: FraudResult | None = None
        self.uw: UnderwritingResult | None = None
        self.pending: PendingReview | None = None  # set while waiting for a human
        self.reviews = 0
        self.enrich_round = 0
        self.decline_reason = ""
        self.receipt_id = ""

    @workflow.run
    async def run(self, app: ApplicationInput) -> ApplicationResult:
        try:
            await self.validate()
            await self.step(self.enrich)
            await self.step(self.underwrite)
            await self.step(self.fund)
            await self.end(AppState.FUNDED)
        except Declined as d:
            self.decline_reason = d.reason.value
            await self.end(AppState.DECLINED)
        await workflow.wait_condition(workflow.all_handlers_finished)
        return ApplicationResult(app.application_id, self.state, self.decline_reason, self.receipt_id)

    async def step(self, fn: Callable[[], Awaitable[None]]) -> None:
        """Run a stage. If it fails unexpectedly (a bug, an outage past every retry), ask a
        person to retry it or decline. So no application ever ends as Temporal `Failed`."""
        while True:
            try:
                return await fn()
            except ActivityError as e:
                await self.ask_human(EscalationReason.UNEXPECTED_FAILURE, [A.RETRY, A.DECLINE],
                                     {"stage": fn.__name__, "error": error_type(e)})

    # ================================================================== stages

    async def validate(self) -> None:
        await self.set_state(AppState.VALIDATING)
        v = await workflow.execute_activity(ops.validate_application, self.app, **INTERNAL)
        await self.audit("validated", asdict(v))
        if not v.valid:
            raise Declined(DeclineReason.INVALID_APPLICATION)
        if v.missing_documents:
            # A person follows up with the applicant. APPROVE = the documents are sorted.
            await self.ask_human(EscalationReason.MISSING_DOCUMENTS, [A.APPROVE, A.DECLINE],
                                 {"missing_documents": v.missing_documents})

    async def enrich(self) -> None:
        """Credit, identity and fraud in parallel. Results we already have are kept."""
        while True:
            await self.set_state(AppState.ENRICHING)
            self.enrich_round += 1
            calls: dict[str, Awaitable[Any]] = {}
            if self.credit is None:
                calls["credit"] = self.call_provider(providers.pull_credit_report, CREDIT_QUEUE, "credit")
            if self.identity is None:
                calls["identity"] = self.call_provider(providers.verify_identity, IDENTITY_QUEUE, "identity")
            if self.fraud is None:
                calls["fraud"] = self.call_provider(providers.score_fraud, FRAUD_QUEUE, "fraud")
            results = await asyncio.gather(*calls.values(), return_exceptions=True)

            failed = {}
            for name, result in zip(calls, results, strict=True):
                if isinstance(result, ActivityError):
                    failed[name] = error_type(result)
                    await self.audit("provider_failed", {"provider": name, "error": failed[name]})
                elif isinstance(result, BaseException):
                    raise result  # not a provider failure (e.g. cancellation): don't hide it
                else:
                    setattr(self, name, result)
                    await self.audit("provider_result", {"provider": name, **asdict(result)})
            if not failed:
                return
            # Temporal already retried until the deadline. RETRY re-runs only the failed ones.
            await self.ask_human(EscalationReason.PROVIDER_UNAVAILABLE, [A.RETRY, A.DECLINE],
                                 {"failed": failed})

    async def call_provider(self, activity_fn: Any, queue: str, name: str) -> Any:
        a = self.app
        req = ProviderRequest(a.application_id, f"{a.application_id}-{name}", self.enrich_round,
                              a.applicant.demo_profile, a.applicant.full_name, a.applicant.date_of_birth)
        return await workflow.execute_activity(
            activity_fn, req,
            task_queue=queue,  # one queue per provider, rate-limited by the Temporal server
            start_to_close_timeout=timedelta(seconds=20),
            schedule_to_close_timeout=self.timings.provider_deadline,  # total time for retries
            retry_policy=RetryPolicy(initial_interval=timedelta(seconds=1),
                                     maximum_interval=timedelta(seconds=30)),
        )

    async def underwrite(self) -> None:
        await self.set_state(AppState.UNDERWRITING)
        assert self.credit and self.identity and self.fraud
        a = self.app
        req = UnderwritingRequest(a.application_id, a.product, a.amount_cents, self.credit,
                                  self.identity, self.fraud, a.product_data)
        self.uw = await workflow.execute_activity(
            underwriting.underwrite_application, req,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=5, non_retryable_error_types=["RulesError"]))
        await self.audit("underwriting_decision", asdict(self.uw))
        if self.uw.decision == Decision.DECLINE:
            raise Declined(DeclineReason.UNDERWRITING_DECLINED)
        if self.uw.decision == Decision.REFER:
            await self.ask_human(EscalationReason.UNDERWRITING_REFERRAL, [A.APPROVE, A.DECLINE],
                                 {"reasons": self.uw.reasons, "rules_version": self.uw.rules_version})

    async def fund(self) -> None:
        assert self.uw
        a = self.app
        req = DisbursementRequest(a.application_id, f"{a.application_id}-disburse", a.amount_cents,
                                  self.uw.payouts, a.applicant.demo_profile)
        while True:
            await self.set_state(AppState.FUNDING)
            try:
                # A timeout is retried with the same idempotency key, so it can never pay twice.
                receipt = await workflow.execute_activity(
                    funding.disburse_funds, req,
                    start_to_close_timeout=timedelta(seconds=60),
                    schedule_to_close_timeout=self.timings.funding_deadline,
                    retry_policy=RetryPolicy(non_retryable_error_types=["AccountRejected"]))
                break
            except ActivityError as e:
                if error_type(e) != "AccountRejected":
                    raise  # `step` asks a human
                await self.ask_human(EscalationReason.FUNDING_REJECTED, [A.RETRY, A.DECLINE],
                                     {"error": str(e.cause)})
        self.receipt_id = receipt.receipt_id
        await self.audit("disbursed", {"receipt_id": receipt.receipt_id,
                                       "payouts": [asdict(p) for p in req.payouts]})
        # Only after the money moved: the lending team bills on disbursed loans.
        await workflow.execute_activity(
            ops.emit_billing_event,
            BillingEvent(a.application_id, a.product.value, receipt.amount_cents, workflow.now()),
            **INTERNAL)

    # ================================================================== human in the loop

    async def ask_human(self, reason: EscalationReason, allowed: list[ReviewAction],
                        context: dict[str, Any]) -> ReviewAction:
        """Move to ESCALATED and wait, as long as it takes, for a person's decision.
        Returns the action (APPROVE or RETRY); a DECLINE ends the application."""
        self.reviews += 1
        self.pending = PendingReview(f"{self.app.application_id}-review-{self.reviews}", reason,
                                     allowed, context, workflow.now())
        await self.set_state(AppState.ESCALATED)
        await self.audit("review_requested", {"task_id": self.pending.task_id, "reason": reason.value,
                                              "allowed_actions": [x.value for x in allowed],
                                              "context": context})
        pending = self.pending
        await workflow.wait_condition(lambda: pending.decision is not None)
        d = pending.decision
        assert d is not None
        await self.audit("human_decision", {"task_id": d.task_id, "action": d.action.value,
                                            "rationale": d.rationale}, actor=d.reviewer_id)
        self.pending = None
        if d.action == A.DECLINE:
            raise Declined(DeclineReason.REVIEWER_DECLINED)
        return d.action

    @workflow.update
    def submit_review(self, decision: ReviewDecision) -> ReviewAck:
        assert self.pending is not None  # the validator guarantees it
        self.pending.decision = decision
        return ReviewAck(decision.task_id, f"{decision.action.value} accepted")

    @submit_review.validator
    def validate_review(self, d: ReviewDecision) -> None:
        # Runs before anything is recorded. A rejected decision leaves no trace in history,
        # and the reviewer sees the reason straight away.
        p = self.pending
        if p is None:
            raise ValueError("no review is pending for this application")
        if d.task_id != p.task_id:
            raise ValueError(f"old task {d.task_id!r}; the current task is {p.task_id!r}")
        if p.decision is not None:
            raise ValueError(f"a decision was already submitted for {p.task_id!r}")
        if d.action not in p.allowed_actions:
            allowed = ", ".join(x.value for x in p.allowed_actions)
            raise ValueError(f"{d.action.value} is not allowed here (allowed: {allowed})")
        if not d.reviewer_id.strip() or not d.rationale.strip():
            raise ValueError("reviewer and rationale are required")

    @workflow.query
    def status(self) -> ApplicationStatus:
        a = self.app
        return ApplicationStatus(a.application_id, self.state, a.product, a.channel,
                                 a.received_at + SLA_BUDGET, self.pending, self.decline_reason)

    # ================================================================== helpers

    async def set_state(self, new: AppState) -> None:
        if self.state == new:
            return
        assert_transition(self.state, new)  # an illegal move is a bug: fails the task, not the app
        old, self.state = self.state, new
        workflow.upsert_search_attributes([sa.APP_STATE.value_set(new.value)])
        await self.audit("state_changed", {"from": old.value, "to": new.value})

    async def audit(self, event_type: str, data: dict[str, Any], actor: str = "system") -> None:
        self.seq += 1  # same number on replay, so a retried write never duplicates
        await workflow.execute_activity(
            ops.record_event,
            AuditEvent(self.app.application_id, self.seq, event_type, self.state, workflow.now(),
                       actor, data),
            **INTERNAL)

    async def end(self, final: AppState) -> None:
        await self.set_state(final)
        await self.audit("application_finished", {
            "final_state": final.value, "reason": self.decline_reason, "receipt_id": self.receipt_id,
            "rules_version": self.uw.rules_version if self.uw else ""})
