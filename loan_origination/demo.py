"""Three demo scenarios, one per input channel (demo timings: seconds, not hours).

    python -m loan_origination.demo happy     # portal: auto-approved, funded, billed once
    python -m loan_origination.demo review    # broker email: referred -> human approves -> funded
    python -m loan_origination.demo outage    # aggregator batch: credit provider down -> human retries

Needs the dev server and ``make worker`` running against the same ``LOAN_DB_PATH``.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from temporalio.client import Client

from loan_origination.intake import Handle, ingest
from loan_origination.models import AppState, ReviewAction, TimingProfile
from loan_origination.review_cli import ReviewRejected, get_status, submit_decision
from loan_origination.store import get_store

SAMPLES = Path(__file__).resolve().parent.parent / "samples"


def say(msg: str) -> None:
    print(f"\n\033[1m== {msg}\033[0m")


def load_sample(name: str, run: str) -> dict[str, Any]:
    """Load a sample payload. Each run gets fresh references, because a reference can
    only ever start one workflow (that is the duplicate protection)."""
    payload = json.loads((SAMPLES / name).read_text())
    if "reference" in payload:
        payload["reference"] += f"-{run}"
    for rec in payload.get("applications", []):
        rec["reference"] += f"-{run}"
    return payload


_printed: dict[str, int] = {}  # audit events already shown, per application


async def watch(client: Client, app_id: str, *states: AppState, timeout: float = 60) -> None:
    """Print each new audit event as it is written, until the application reaches a state."""
    printed = _printed.get(app_id, 0)
    deadline = time.monotonic() + timeout
    while True:
        trail = get_store().audit_trail(app_id)
        for r in trail[printed:]:
            d = {k: v for k, v in r["data"].items() if k in (
                "to", "provider", "score", "verified", "error", "decision", "reasons", "reason",
                "action", "rationale", "receipt_id", "missing_documents", "failed")}
            print(f"   #{r['seq']:<3} {r['event_type']:<22} actor={r['actor']:<8} "
                  f"{json.dumps(d, default=str)[:150]}")
        printed = _printed[app_id] = len(trail)
        st = await get_status(client, app_id)
        if st.state in states:
            return
        if time.monotonic() > deadline:
            raise SystemExit(f"timed out waiting for {states}; state is {st.state.value}")
        await asyncio.sleep(0.3)


async def show_result(h: Handle) -> None:
    res = await h.result()
    desc = await h.describe()
    print(f"\n   RESULT {res.final_state.value} {res.reason}  "
          f"(Temporal status: {desc.status.name if desc.status else '?'})")
    s = get_store()
    print(f"   disbursements: {[(d['amount_cents'], d['payouts']) for d in s.disbursements(h.id)]}")
    print(f"   billing events: {[b['amount_cents'] for b in s.billing_events(h.id)]}")


async def operator_search(client: Client, query: str) -> None:
    await asyncio.sleep(1.2)  # Temporal search is eventually consistent
    print(f"   operator searches the Temporal UI:  {query}")
    async for wf in client.list_workflows(query):
        attrs = {k: v[0] for k, v in wf.search_attributes.items()}
        print(f"     {wf.id}  {attrs}")


# ------------------------------------------------------------------------- scenarios


async def happy(client: Client, run: str) -> None:
    payload = load_sample("portal.json", run)
    say("self-serve online portal: same payload delivered twice")
    [h] = await ingest(client, payload, timing=TimingProfile.DEMO)
    [h2] = await ingest(client, payload, timing=TimingProfile.DEMO)
    same = (await h.describe()).run_id == (await h2.describe()).run_id
    print(f"   {h.id}: one workflow for both deliveries: {same}")
    await watch(client, h.id, AppState.FUNDED, AppState.DECLINED)
    await show_result(h)


async def review(client: Client, run: str) -> None:
    say("broker referral via email: near-prime applicant, the rules refer it to a human")
    [h] = await ingest(client, load_sample("broker_email.json", run), timing=TimingProfile.DEMO)
    await watch(client, h.id, AppState.ESCALATED)
    await operator_search(client, 'AppState = "ESCALATED" AND ExecutionStatus = "Running"')
    st = await get_status(client, h.id)
    assert st.pending_review
    say("reviewer sends RETRY, which is not allowed for a referral")
    try:
        await submit_decision(client, h.id, ReviewAction.RETRY, "r.lee", "try again")
    except ReviewRejected as e:
        print(f"   REJECTED immediately: {e}")
    say("reviewer approves")
    ack = await submit_decision(client, h.id, ReviewAction.APPROVE, "r.lee",
                                "Stable income; score within tolerance")
    print(f"   ACCEPTED: {ack.message}")
    say("the same approval again (double-click)")
    try:
        await submit_decision(client, h.id, ReviewAction.APPROVE, "r.lee", "double click",
                              task_id=st.pending_review.task_id)
    except ReviewRejected as e:
        print(f"   REJECTED immediately: {e}")
    await watch(client, h.id, AppState.FUNDED, AppState.DECLINED)
    await show_result(h)


async def outage(client: Client, run: str) -> None:
    say("aggregator batch feed: two applications; the credit provider is down for the second")
    good, flaky = await ingest(client, load_sample("aggregator_batch.json", run),
                               timing=TimingProfile.DEMO)
    print(f"   started {good.id} and {flaky.id}")
    await watch(client, flaky.id, AppState.ESCALATED)
    say("Temporal retried the credit check until its deadline; a human now decides")
    say("reviewer chooses RETRY: only the missing credit check runs again")
    await submit_decision(client, flaky.id, ReviewAction.RETRY, "r.lee", "Bureau is back up")
    await watch(client, flaky.id, AppState.FUNDED, AppState.DECLINED)
    await show_result(flaky)
    calls = [r["data"]["provider"] for r in get_store().audit_trail(flaky.id)
             if r["event_type"] == "provider_result"]
    print(f"   provider results recorded: {calls} (identity and fraud were not called again)")
    say(f"the other application in the batch: {good.id} (debt consolidation, pays creditors)")
    await watch(client, good.id, AppState.FUNDED, AppState.DECLINED)
    await show_result(good)


SCENARIOS: dict[str, Callable[[Client, str], Awaitable[None]]] = {
    "happy": happy, "review": review, "outage": outage,
}


async def main() -> None:
    name = sys.argv[1] if len(sys.argv) > 1 else "happy"
    if name not in SCENARIOS:
        raise SystemExit(f"unknown scenario {name!r}; choose from {', '.join(SCENARIOS)}")
    client = await Client.connect(os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"))
    await SCENARIOS[name](client, str(int(time.time())))


if __name__ == "__main__":
    asyncio.run(main())
