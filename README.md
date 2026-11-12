# Loan disbursement on Temporal

One Temporal workflow per loan application:

**ingest → validate → enrich (credit, identity, fraud in parallel) → underwrite → fund**

Every application ends **funded**, **declined** or **escalated** to a human, who decides and lets it resume. The three input channels are sample payloads, third parties are faked, storage is SQLite, and Temporal runs locally.

- Design (the brief's questions, trade-offs, what I'd add next): [docs/DESIGN.md](docs/DESIGN.md)
- Four diagrams: [docs/DIAGRAMS.md](docs/DIAGRAMS.md) (Mermaid) and [docs/loan-diagrams.excalidraw](docs/loan-diagrams.excalidraw) — **[open in Excalidraw](https://excalidraw.com/#json=1tKHe5OvCcvMSdrCy3ia8,L1nHmBih2Pwedt8qThNEgw)**
- [docs/CLAUDE_PLAN.md](docs/CLAUDE_PLAN.md) is the Claude plan file: the earlier, fuller plan; this is the deliberately bare-bones version, scoped to the 2-hour timebox. DESIGN.md §7 lists what was left out.

## Setup

1. Install the Temporal CLI with `brew install temporal`, or use Docker: `docker run -p 7233:7233 -p 8233:8233 temporalio/temporal server start-dev`.
2. Install Python 3.11+ and the dependencies:
   ```bash
   make venv
   ```
3. Terminal 1, the Temporal dev server (Web UI at http://localhost:8233):
   ```bash
   make dev-server
   ```
4. Terminal 2, the workers:
   ```bash
   make worker
   ```
5. Terminal 3, a demo:
   ```bash
   make demo SCENARIO=happy
   ```

The worker and the demo share `./loan.db` (override with `LOAN_DB_PATH`). `make clean` deletes it.

## Demos (one per channel)

| Scenario | Channel | What it shows | Ends |
|---|---|---|---|
| `happy` | self-serve online portal | the same payload sent twice → one workflow; auto-approved; disbursed once; one billing event | FUNDED |
| `review` | broker referral via email | near-prime applicant → rules refer → ESCALATED; a disallowed decision and a double-click are rejected; reviewer approves → resumes | FUNDED |
| `outage` | aggregator batch feed | two applications; the credit provider is down for one → Temporal retries → ESCALATED → reviewer retries → only credit is called again. The other is debt consolidation and pays creditors directly. | FUNDED |

To ingest a payload yourself (each reference can only start one workflow, ever):

```bash
make ingest PAYLOAD=samples/broker_email.json
```

## Human review

```bash
.venv/bin/python -m loan_origination.review_cli list
```

```bash
.venv/bin/python -m loan_origination.review_cli show <app_id>
```

```bash
.venv/bin/python -m loan_origination.review_cli approve <app_id> --reviewer r.lee --rationale "stable income"
```

The other actions are `decline` and `retry`. A rejected decision prints `REJECTED: <reason>` and exits with code 2.

## Operator view

Paste these into the search box at http://localhost:8233:

```
AppState = "ESCALATED" AND ExecutionStatus = "Running"
SlaDeadline < "2026-11-12T12:00:00Z" AND ExecutionStatus = "Running"
ExecutionStatus = "Failed"
```

In the second query, use the current time. The last query should always be empty.

## Tests

```bash
make test
```

29 tests:
- **No server needed:** product rules, the state machine, activities being safe to repeat, channel payloads, the provider rate limits and the workflow sandbox check.
- **With Temporal's time-skipping test server** (downloaded on first run; skipped with a reason if unavailable):
  - happy path with a duplicate;
  - referral with rejected bad decisions;
  - provider outage with a retry;
  - a bug going to a human instead of `Failed`.

`make test-unit` runs only the tests that need no server. `make lint` runs ruff.

## What you are looking at

1. `workflows/application.py`: the whole lifecycle; `run()` reads top to bottom. `ask_human()` is the human-in-the-loop.
2. `state_machine.py`: the transition table, checked before every state change.
3. `intake.py` + `samples/`: three channel payloads normalised into one shape; id `loan-<channel>-<reference>` makes duplicates no-ops.
4. `activities/`: validation, audit log, providers, underwriting, disbursement and billing. All safe to retry.
5. `products/`: one rules module per product; `policy.py` has per-product documents and limits.
6. `worker.py`: one worker for the workflow, one per provider with a server-side rate limit.
7. `review_cli.py`: sends the human decision as a Temporal Update; its validator rejects bad decisions.
8. `demo.py`: the three scenarios above.
