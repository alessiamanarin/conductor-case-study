"""The application lifecycle as an explicit transition table.

The workflow calls ``assert_transition`` before every state change. An illegal move raises a
plain ``RuntimeError``: in Temporal's Python SDK that fails the workflow *task*, which is
retried once a fix is deployed, instead of failing the whole application.
"""

from __future__ import annotations

from loan_origination.models import AppState as S

TERMINAL: frozenset[S] = frozenset({S.FUNDED, S.DECLINED})

TRANSITIONS: dict[S, frozenset[S]] = {
    S.INGESTED: frozenset({S.VALIDATING}),
    S.VALIDATING: frozenset({S.ENRICHING, S.ESCALATED, S.DECLINED}),
    S.ENRICHING: frozenset({S.UNDERWRITING, S.ESCALATED}),
    S.UNDERWRITING: frozenset({S.FUNDING, S.ESCALATED, S.DECLINED}),
    S.FUNDING: frozenset({S.FUNDED, S.ESCALATED}),
    # A human decision resumes the stage it came from, or declines.
    S.ESCALATED: frozenset({S.ENRICHING, S.UNDERWRITING, S.FUNDING, S.DECLINED}),
    S.FUNDED: frozenset(),
    S.DECLINED: frozenset(),
}


def is_legal(src: S, dst: S) -> bool:
    return dst in TRANSITIONS[src]


def assert_transition(src: S, dst: S) -> None:
    if not is_legal(src, dst):
        raise RuntimeError(f"illegal transition {src.value} -> {dst.value}")
