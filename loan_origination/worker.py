"""Workers: one for the workflow, one per third-party provider.

    python -m loan_origination.worker

The provider queues carry a rate limit that the Temporal *server* enforces, so it holds
across every worker process, not just this one.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

from temporalio.client import Client
from temporalio.worker import Worker

from loan_origination.activities import funding, ops, providers, underwriting
from loan_origination.policy import (
    CREDIT_QUEUE,
    FRAUD_QUEUE,
    IDENTITY_QUEUE,
    ORCHESTRATION_QUEUE,
    PROVIDER_RATE_LIMITS,
)
from loan_origination.workflows.application import LoanApplicationWorkflow


@dataclass
class WorkerSpec:
    """What one worker registers, so it can be checked in tests without a server."""

    task_queue: str
    workflows: list[type] = field(default_factory=list)
    activities: list[Callable[..., Any]] = field(default_factory=list)
    options: dict[str, Any] = field(default_factory=dict)


def worker_specs() -> list[WorkerSpec]:
    def provider(queue: str, fn: Callable[..., Any]) -> WorkerSpec:
        return WorkerSpec(queue, [], [fn],
                          {"max_task_queue_activities_per_second": PROVIDER_RATE_LIMITS[queue]})

    return [
        WorkerSpec(ORCHESTRATION_QUEUE, [LoanApplicationWorkflow],
                   [*ops.ALL, underwriting.underwrite_application, funding.disburse_funds]),
        provider(CREDIT_QUEUE, providers.pull_credit_report),
        provider(IDENTITY_QUEUE, providers.verify_identity),
        provider(FRAUD_QUEUE, providers.score_fraud),
    ]


def build_workers(client: Client) -> list[Worker]:
    executor = ThreadPoolExecutor(max_workers=32)  # activities are sync (SQLite)
    return [Worker(client, task_queue=s.task_queue, workflows=s.workflows,
                   activities=s.activities, activity_executor=executor, **s.options)
            for s in worker_specs()]


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    client = await Client.connect(os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"))
    workers = build_workers(client)
    logging.info("polling %s (db=%s)", ", ".join(w.task_queue for w in workers),
                 os.environ.get("LOAN_DB_PATH", "loan.db"))
    await asyncio.gather(*(w.run() for w in workers))


if __name__ == "__main__":
    asyncio.run(main())
