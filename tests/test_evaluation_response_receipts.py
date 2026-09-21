import hashlib
from datetime import UTC, datetime

import pytest

from qs_ai.application.interpretation.provider import ModelResponse
from qs_ai.infrastructure.persistence.mysql.evaluation_response_receipts import (
    EvaluationResponse,
    decode,
)


def test_response_round_trip_preserves_unknown_usage_and_original_output():
    response = ModelResponse("inv:1", "req:1", "model", '{"x":1}', '{"x":1}', "", None, None, 10)
    result = EvaluationResponse(response, None, datetime(2026, 9, 21, tzinfo=UTC))
    raw = result.encode()
    digest = hashlib.sha256(raw.encode()).hexdigest()
    assert decode(raw, digest) == result
    with pytest.raises(ValueError, match="digest"):
        decode(raw + " ", digest)


def test_missing_response_and_failure_rejected():
    with pytest.raises(ValueError):
        EvaluationResponse(None, None, datetime(2026, 9, 21, tzinfo=UTC))
