from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from qs_ai.infrastructure.persistence.mysql.observation import observe, summarize


def test_missing_samples_are_not_zero_latency_or_success():
    assert summarize([None, None]) == {
        "n": 0,
        "missing": 2,
        "invalid": 0,
        "known_total": None,
        "p50": None,
        "p95": None,
        "max": None,
    }
    result = summarize([None, -1, float("nan"), 0, 2, 100])
    assert result == {
        "n": 3,
        "missing": 1,
        "invalid": 2,
        "known_total": 102,
        "p50": 2,
        "p95": 100,
        "max": 100,
    }


@pytest.mark.parametrize("mode", ["profile", "naive", "empty", "too_long"])
async def test_invalid_observation_scope_never_opens_database(mode):
    tx = AsyncMock()
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=1)
    profile = "sha256:" + "a" * 64
    if mode == "profile":
        profile = "anything' OR 1=1"
    elif mode == "naive":
        start = start.replace(tzinfo=None)
    elif mode == "empty":
        end = start
    else:
        end = start + timedelta(days=32)
    with pytest.raises(ValueError):
        await observe(tx, profile, start, end)
    tx.open.assert_not_called()
