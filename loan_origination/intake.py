"""Ingestion: turn a payload from any of the three channels into workflows.

    python -m loan_origination.intake samples/portal.json

The three channels look different (one application, an email, a batch of many), so each
payload is first normalised into the same ``ApplicationInput``. Each application then starts
one workflow whose id is ``loan-<channel>-<reference>``: delivering the same payload twice
is a no-op, which is what makes ingestion reliable.

The samples stand in for the real channels. In production the portal would call
``start_application`` directly, a scheduled job would read the broker inbox, and the
aggregator batch would be read from a file drop; all three end in the same call.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import UTC, datetime
from typing import Any

from temporalio.client import Client, WorkflowHandle
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from loan_origination import search_attrs as sa
from loan_origination.models import (
    Applicant,
    ApplicationInput,
    ApplicationResult,
    AppState,
    Channel,
    Product,
    TimingProfile,
    application_id_for,
)
from loan_origination.policy import ORCHESTRATION_QUEUE, SLA_BUDGET
from loan_origination.workflows.application import LoanApplicationWorkflow

Handle = WorkflowHandle[LoanApplicationWorkflow, ApplicationResult]


def parse_payload(payload: dict[str, Any], *, now: datetime | None = None,
                  timing: TimingProfile = TimingProfile.PROD) -> list[ApplicationInput]:
    """Normalise a portal, broker email or aggregator batch payload into applications."""
    channel = Channel(payload["channel"])
    # received_at is when the *channel* got it, so delays before ingestion count against the SLA.
    received = _ts(payload.get("received_at")) or now or datetime.now(UTC)
    if channel == Channel.AGGREGATOR:
        records = [(r["reference"], r["application"]) for r in payload["applications"]]
    else:
        records = [(payload["reference"], payload["application"])]
    return [_application(channel, ref, body, received, timing) for ref, body in records]


def _application(channel: Channel, ref: str, body: dict[str, Any], received: datetime,
                 timing: TimingProfile) -> ApplicationInput:
    p = body["applicant"]
    return ApplicationInput(
        application_id=application_id_for(channel, ref),
        channel=channel,
        external_ref=ref,
        product=Product(body["product"]),
        applicant=Applicant(p["full_name"], p["email"], p["date_of_birth"],
                            int(p["annual_income_cents"]), list(p.get("demo_profile", []))),
        amount_cents=int(body["amount_cents"]),
        term_months=int(body["term_months"]),
        received_at=received,
        documents=list(body.get("documents", [])),
        product_data=dict(body.get("product_data", {})),
        timing_profile=timing,
    )


def _ts(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


async def start_application(client: Client, app: ApplicationInput) -> Handle:
    """Start one workflow per application. A duplicate returns the existing workflow."""
    try:
        return await client.start_workflow(
            LoanApplicationWorkflow.run, app,
            id=app.application_id,
            task_queue=ORCHESTRATION_QUEUE,
            # Even a finished application with this id is never started again.
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
            search_attributes=sa.initial(AppState.INGESTED.value, app.product.value,
                                         app.received_at + SLA_BUDGET),
        )
    except WorkflowAlreadyStartedError:
        return client.get_workflow_handle_for(LoanApplicationWorkflow.run, app.application_id)


async def ingest(client: Client, payload: dict[str, Any], **kw: Any) -> list[Handle]:
    return [await start_application(client, app) for app in parse_payload(payload, **kw)]


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("payload", help="path to a sample payload, e.g. samples/portal.json")
    p.add_argument("--address", default=os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"))
    args = p.parse_args()
    with open(args.payload) as f:
        payload = json.load(f)
    client = await Client.connect(args.address)
    for h in await ingest(client, payload, timing=TimingProfile.DEMO):
        print(f"started {h.id}")


if __name__ == "__main__":
    asyncio.run(main())
