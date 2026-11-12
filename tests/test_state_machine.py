"""The lifecycle: only the transitions in the diagram are allowed."""

from __future__ import annotations

import itertools

import pytest

from loan_origination.models import AppState as S
from loan_origination.state_machine import TERMINAL, TRANSITIONS, assert_transition


def test_happy_path_is_allowed() -> None:
    path = [S.INGESTED, S.VALIDATING, S.ENRICHING, S.UNDERWRITING, S.FUNDING, S.FUNDED]
    for old, new in itertools.pairwise(path):
        assert_transition(old, new)


def test_escalated_can_resume_or_decline() -> None:
    for new in (S.ENRICHING, S.UNDERWRITING, S.FUNDING, S.DECLINED):
        assert_transition(S.ESCALATED, new)


def test_illegal_moves_are_rejected() -> None:
    with pytest.raises(RuntimeError, match="illegal transition"):
        assert_transition(S.ENRICHING, S.FUNDED)  # cannot skip underwriting
    with pytest.raises(RuntimeError):
        assert_transition(S.FUNDED, S.DECLINED)  # a funded loan is final


def test_only_funded_and_declined_are_final() -> None:
    assert {S.FUNDED, S.DECLINED} == TERMINAL
    assert all(not TRANSITIONS[t] for t in TERMINAL)
