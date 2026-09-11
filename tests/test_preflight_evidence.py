from datetime import UTC, datetime

import pytest

from qs_ai.domain.evaluation.preflight import AssertionReceipt, PreflightEvidence


def test_passing_preflight_requires_both_assertions_and_zero_model_calls():
    receipt = AssertionReceipt(
        "provider_call_count", "case", 1, True, "preflight", "passed", "zero"
    )
    with pytest.raises(ValueError, match="required assertions"):
        PreflightEvidence(
            "case:1", "passed", datetime.now(UTC), 0, "insufficient_dimensions", (receipt,)
        )
    with pytest.raises(ValueError, match="must not call"):
        PreflightEvidence(
            "case:1", "failed", datetime.now(UTC), 1, "insufficient_dimensions", (receipt,)
        )
