"""Worker setup and the workflow sandbox check, without a Temporal server."""

from __future__ import annotations

from temporalio import workflow
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

from loan_origination.worker import worker_specs
from loan_origination.workflows.application import LoanApplicationWorkflow


def test_provider_queues_are_rate_limited() -> None:
    for s in worker_specs():
        if s.task_queue.startswith("provider-"):
            assert s.options["max_task_queue_activities_per_second"] > 0


async def test_workflow_passes_the_sandbox_check() -> None:
    # Fails if the workflow imports something non-deterministic.
    SandboxedWorkflowRunner().prepare_workflow(workflow._Definition.must_from_class(LoanApplicationWorkflow))
