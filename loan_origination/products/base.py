"""Shared types for product rules. No product thresholds live here.

Product modules import this file and never import each other.
"""

from __future__ import annotations

from typing import Protocol

from loan_origination.models import Product, UnderwritingRequest, UnderwritingResult


class RulesError(Exception):
    """The rules cannot be evaluated (bad product data, unknown product). Never retried."""


class Underwriter(Protocol):
    product: Product
    rules_version: str

    def underwrite(self, req: UnderwritingRequest) -> UnderwritingResult: ...
