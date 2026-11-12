# Design: loan disbursement on Temporal

Diagrams: [DIAGRAMS.md](DIAGRAMS.md) (Mermaid) and [Excalidraw](https://excalidraw.com/#json=1tKHe5OvCcvMSdrCy3ia8,L1nHmBih2Pwedt8qThNEgw) (file: [loan-diagrams.excalidraw](loan-diagrams.excalidraw)).

## 1. Overview

Every application gets **one Temporal workflow**, with id `loan-<channel>-<reference>`. The workflow runs the steps in order (validate → enrich → underwrite → fund) and calls **activities** for anything that touches the outside world (database, providers, bank). Temporal handles retries, waiting and crash recovery. Whenever a person is needed, the workflow moves to `ESCALATED` and **waits for a decision sent as a Temporal Update**, then carries on. Every application ends **funded**, **declined** or **escalated**.

## 2. Design Criteria

| Question | Answer |
|---|---|
| **Ingest from three channels reliably?** | Each channel's payload (portal, broker email, aggregator batch) is turned into the same `ApplicationInput`, then one workflow is started per application. The id is `loan-<channel>-<reference>` with `REJECT_DUPLICATE`, so sending the same application twice does nothing. In this repo the channels are sample payloads in `samples/`. |
| **Lifecycle as a state machine?** | `INGESTED → VALIDATING → ENRICHING → UNDERWRITING → FUNDING → FUNDED`, plus `DECLINED` and `ESCALATED`. `state_machine.py` lists the allowed moves, and the workflow checks it before every change. |
| **Third-party failures, rate limits, partial failures?** | **Rate limits:** each provider has its own task queue with a limit the Temporal server enforces across all workers. **Outages:** Temporal retries with backoff until a deadline. **Partial failures:** the three checks run in parallel and results we already have are kept. If one still fails, a human chooses retry (only the missing check runs again) or decline. |
| **Isolate product logic?** | Each product's rules are a separate module (`products/personal.py`, `products/debt_consolidation.py`) that never imports another product. Required documents and limits are per-product data in `policy.py`. The workflow is the same for every product. Auto would be one more module plus a registry entry. |
| **Know when something is broken?** | In the Temporal UI, `ExecutionStatus = "Failed"` should always be empty, because unexpected errors go to a human instead. Escalated and late applications are one search away (§5). To investigate one application, open its history in the Temporal UI and read its audit trail. |
| **How does a human enter, and how does it resume?** | Triggers: missing documents, a referral from the rules, a provider still down, the bank rejecting the account, or any unexpected error. The workflow waits with no automatic decision. The reviewer sends approve, decline or retry as an **Update**. The Update's validator rejects bad decisions immediately (an old task, an action that isn't allowed, a double-click). A valid decision is written to the audit log and the workflow continues. |

## 3. Workflow and activities

`workflows/application.py` reads top to bottom:

```python
await self.validate()               # invalid -> DECLINED; missing documents -> ask a human
await self.step(self.enrich)        # credit, identity, fraud in parallel
await self.step(self.underwrite)    # approve / decline / refer to a human
await self.step(self.fund)          # disburse once, then the billing event
await self.end(AppState.FUNDED)
```

- `ask_human(...)` moves the application to `ESCALATED` and waits. It returns approve or retry; a decline ends the application.
- `step(...)` catches unexpected activity failures and asks a human to retry or decline, so nothing ends as a failed workflow.

| Activity | Where | Retries | Why repeating it is safe |
|---|---|---|---|
| `validate_application` | orchestration queue | forever | it only checks |
| `record_event` (audit) | orchestration queue | forever | one row per `(application_id, seq)` |
| `pull_credit_report`, `verify_identity`, `score_fraud` | one queue per provider | backoff, until a deadline (10 min) | read-only |
| `underwrite_application` | orchestration queue | 5 attempts | same inputs, same answer |
| `disburse_funds` | orchestration queue | until a deadline (2h); a bank rejection isn't retried | keyed on `<application_id>-disburse`: never pays twice |
| `emit_billing_event` | orchestration queue | forever | one row per application |

The demo uses the same code with shorter deadlines (seconds instead of minutes).

## 4. Errors and exceptions

| Problem | What happens |
|---|---|
| The same application arrives twice | Same workflow id, so the second start is ignored |
| A provider is down | Temporal retries; after the deadline, a human decides; results already fetched are kept |
| A provider's rate limit | The queue's server-side limit keeps all workers under it |
| A worker crashes | Temporal replays the workflow on another worker; activities are safe to repeat |
| The database is down | Writes retry until it's back; nothing is lost |
| The disbursement times out | Retried with the same key, so it can never pay twice |
| The bank rejects the account | A human retries (after fixing the account) or declines |
| A reviewer double-clicks | The validator rejects the second decision |
| A bug in an activity | Goes to a human as `UNEXPECTED_FAILURE`, not `Failed` |

## 5. Operators and the 48-hour SLA

The clock starts when the channel received the application. Each workflow has three search attributes: `AppState`, `LoanProduct` and `SlaDeadline` (received + 48h).

```
AppState = "ESCALATED" AND ExecutionStatus = "Running"                    # needs a human
SlaDeadline < "2026-11-12T12:00:00Z" AND ExecutionStatus = "Running"      # past 48h (use the current time)
ExecutionStatus = "Failed"                                                # broken; should be empty
```

The audit log in SQLite is the permanent record: every state change, provider result, underwriting decision (with its `rules_version`), and human decision (with reviewer and rationale).

## 6. Trade-offs

| Decision | Gained | Gave up |
|---|---|---|
| One workflow per application | One place for state; the id stops duplicates | Changing workflow code later needs versioning |
| Rules in product modules, run as an activity | Rules change without touching running workflows | One more step per decision |
| A human decides when unsure, never a timeout | Accountable, auditable decisions | Needs reviewers; some applications wait longer |
| Update instead of Signal for decisions | The reviewer sees accepted or rejected immediately | Slightly newer Temporal feature |
| Missing documents go to a human, no reminders | One simple path | No automatic chasing of slow applicants |
| Audit table as the record, not Temporal history | Long retention, easy to query | Two places that must agree (handled by idempotent writes) |
| Sample payloads instead of real channels | Shows normalising and duplicate protection quickly | No inbox polling or file parsing |
| SQLite, fake providers, no login | Runs on a laptop | Not production-ready |

## 7. What I'd add next

- Reminders to applicants for missing documents.
- Paging a supervisor when a review waits too long.
- An SLA monitor and alerts.
- UI for customer reporting