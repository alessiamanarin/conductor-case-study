"""Reviewer CLI for human review.

    python -m loan_origination.review_cli list
    python -m loan_origination.review_cli show <app_id>
    python -m loan_origination.review_cli approve|decline|retry <app_id> --reviewer r.lee --rationale "..."

``list`` searches Temporal for escalated applications; the decision is sent to the workflow
as a Temporal Update, so an invalid decision is rejected immediately with the reason.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import asdict
from typing import Any

from temporalio.client import Client, WorkflowUpdateFailedError

from loan_origination.models import ApplicationStatus, ReviewAck, ReviewAction, ReviewDecision
from loan_origination.workflows.application import LoanApplicationWorkflow

ESCALATED_QUERY = 'AppState = "ESCALATED" AND ExecutionStatus = "Running"'


class ReviewRejected(Exception):
    """The workflow refused the decision; nothing was recorded."""


async def get_status(client: Client, app_id: str) -> ApplicationStatus:
    return await client.get_workflow_handle(app_id).query(
        LoanApplicationWorkflow.status, result_type=ApplicationStatus)


async def submit_decision(client: Client, app_id: str, action: ReviewAction, reviewer: str,
                          rationale: str, task_id: str | None = None) -> ReviewAck:
    if task_id is None:  # default to the task currently waiting
        st = await get_status(client, app_id)
        task_id = st.pending_review.task_id if st.pending_review else ""
    try:
        return await client.get_workflow_handle(app_id).execute_update(
            LoanApplicationWorkflow.submit_review,
            ReviewDecision(task_id, reviewer, action, rationale))
    except WorkflowUpdateFailedError as e:
        cause: Any = e.cause
        raise ReviewRejected(getattr(cause, "message", None) or str(cause or e)) from e


async def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="review_cli")
    p.add_argument("--address", default=os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"))
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list", help="applications waiting for human review")
    sub.add_parser("show").add_argument("app_id")
    for name in ("approve", "decline", "retry"):
        a = sub.add_parser(name)
        a.add_argument("app_id")
        a.add_argument("--reviewer", required=True)
        a.add_argument("--rationale", required=True)
    args = p.parse_args(argv)
    client = await Client.connect(args.address)

    if args.cmd == "list":
        found = False
        async for wf in client.list_workflows(ESCALATED_QUERY):
            st = await get_status(client, wf.id)
            if st.pending_review:
                found = True
                pr = st.pending_review
                print(f"{wf.id:<36} {pr.reason.value:<24} "
                      f"allowed={','.join(a.value for a in pr.allowed_actions)}")
        if not found:
            print("nothing waiting for human review")
        return 0
    if args.cmd == "show":
        print(json.dumps(asdict(await get_status(client, args.app_id)), indent=2, default=str))
        return 0
    try:
        ack = await submit_decision(client, args.app_id, ReviewAction(args.cmd.upper()),
                                    args.reviewer, args.rationale)
    except ReviewRejected as e:
        print(f"REJECTED: {e}", file=sys.stderr)
        return 2
    print(f"ACCEPTED: {ack.message} ({ack.task_id})")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
