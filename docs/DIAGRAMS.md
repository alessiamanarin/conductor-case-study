# Diagrams

The same four diagrams are in [loan-diagrams.excalidraw](loan-diagrams.excalidraw) (open it at excalidraw.com with Menu → Open). These Mermaid versions render on GitHub and in VS Code's Markdown preview.

## 1. Architecture

```mermaid
flowchart LR
    P["Self-serve online portal<br/>(sample payload)"] --> I
    B["Broker referral via email<br/>(sample payload)"] --> I
    A["Aggregator batch feed<br/>(sample payload)"] --> I
    I["Ingest<br/>id = loan-channel-reference<br/>SLA deadline = received + 48h"] -->|start| T
    T["Temporal<br/>LoanApplicationWorkflow"] --> PR["Third-party providers<br/>credit, identity, fraud<br/>(rate limited)"]
    T --> U["Underwriting: rules per product<br/>personal, debt consolidation<br/>(auto: documented only)"]
    T --> F["Funding<br/>disburse once (idempotency key)<br/>+ billing event"]
    PR --> L[("Audit log")]
    U --> L
    F --> L
    H["Human review<br/>(review CLI)"] -->|decision| T
    O["Operators<br/>Temporal UI"] -.->|search| T
```

## 2. Application lifecycle (state machine)

```mermaid
stateDiagram-v2
    [*] --> INGESTED
    INGESTED --> VALIDATING
    VALIDATING --> ENRICHING
    VALIDATING --> DECLINED: invalid
    VALIDATING --> ESCALATED: missing documents
    ENRICHING --> UNDERWRITING
    ENRICHING --> ESCALATED: provider down
    UNDERWRITING --> FUNDING: approve
    UNDERWRITING --> DECLINED: auto-decline
    UNDERWRITING --> ESCALATED: refer
    FUNDING --> FUNDED
    FUNDING --> ESCALATED: bank rejected
    ESCALATED --> ENRICHING: human decision
    ESCALATED --> UNDERWRITING: human decision
    ESCALATED --> FUNDING: human decision
    ESCALATED --> DECLINED: human declines
    FUNDED --> [*]
    DECLINED --> [*]
```

Any unexpected error also goes to ESCALATED, so no application ends as a failed workflow.

## 3. Third-party providers: rate limits, partial failures, outages

```mermaid
flowchart LR
    E[ENRICHING] --> C[Credit check]
    E --> ID[Identity check]
    E --> FR[Fraud check]
    C --> J{all three<br/>results in?}
    ID --> J
    FR --> J
    J -->|yes| U[UNDERWRITING]
    J -->|no| X["ESCALATED<br/>human: retry or decline"]
    X -->|retry: only the missing checks| E
```

Each check retries with backoff on its own rate-limited task queue before the workflow escalates.

## 4. Human-in-the-loop

```mermaid
sequenceDiagram
    participant WF as Workflow
    participant R as Reviewer (CLI)
    WF->>WF: ESCALATED, pause (no automatic decision)
    R->>WF: Update submit_review(decision)
    alt invalid (old task, not allowed, already decided)
        WF-->>R: rejected immediately, nothing recorded
    else valid
        WF-->>R: accepted
        WF->>WF: audit the decision, then resume (or decline)
    end
```
