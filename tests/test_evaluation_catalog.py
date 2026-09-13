import base64
import json
from dataclasses import replace
from datetime import UTC, datetime
from uuid import UUID

import pytest

from qs_ai.application.evaluation.catalog import EvaluationCatalogQuery


def test_cursor_roundtrip_is_bound_to_organization_and_status():
    query = EvaluationCatalogQuery(1, 42, "requested")
    at, run_id = "2026-09-13T16:00:00+08:00", str(UUID(int=42))
    cursor = query.next_cursor(at, run_id)
    paged = replace(query, cursor=cursor)
    assert paged.after() == (datetime(2026, 9, 13, 8), run_id)
    assert replace(paged, operator_user_id=43).after() == paged.after()
    for overrides in ({"organization_id": 2}, {"status": "approved"}, {"status": ""}):
        with pytest.raises(ValueError):
            replace(paged, **overrides)


@pytest.mark.parametrize(
    "overrides",
    [
        {"organization_id": 0},
        {"organization_id": True},
        {"operator_user_id": 0},
        {"operator_user_id": 2**63},
        {"limit": 0},
        {"limit": 101},
        {"limit": True},
        {"status": "failed"},
        {"cursor": "*"},
        {"cursor": "a" * 1025},
    ],
)
def test_invalid_queries_rejected(overrides):
    with pytest.raises(ValueError):
        replace(EvaluationCatalogQuery(1, 42), **overrides)


@pytest.mark.parametrize(
    "position",
    [
        ["not-a-date", str(UUID(int=1))],
        ["2026-09-13T08:00:00", str(UUID(int=1))],
        [datetime.now(UTC).isoformat(), "bad-id"],
        [datetime.now(UTC).isoformat(), str(UUID(int=0))],
    ],
)
def test_invalid_cursor_position_rejected(position):
    cursor = base64.urlsafe_b64encode(
        json.dumps([1, "evaluation", 1, "", *position]).encode()
    ).decode()
    with pytest.raises(ValueError):
        EvaluationCatalogQuery(1, 42, cursor=cursor)
