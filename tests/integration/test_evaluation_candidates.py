import json
from dataclasses import replace

import pytest
from sqlalchemy import update

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_generation_completions,
    evaluation_semantic_completions,
)
from tests.integration.test_evaluation_completions import dispatched as dispatched
from tests.integration.test_evaluation_reviews import outputs
from tests.integration.test_evaluation_reviews import reviewable as reviewable
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_semantic_completions import judge as judge

pytestmark = pytest.mark.integration


async def test_read_original_bytes_receipts_release_and_only_selected_reviews(reviewable):
    tx, scope, version, review = reviewable
    store = MySQLEvaluationManagement(tx)
    before = await outputs(tx, scope.run_id)
    index = await store.list_candidates(scope)
    assert index.version == version and len(index.candidates) == 35
    assert {s.candidate_id for s in index.candidates} == {f"candidate:{i}" for i in range(1, 36)}
    view = await store.get_candidate(scope, "candidate:1", index.version)
    evidence = json.loads(view.evidence_json)
    generation = next(r for r in before[0] if r["candidate_id"] == "candidate:1")
    semantic = next(r for r in before[1] if r["candidate_id"] == "candidate:1")
    assert view.normalized_output == generation["normalized_output"]
    assert view.semantic_output == semantic["normalized_output"]
    assert evidence["candidate"] == generation["candidate_json"]
    assert evidence["semantic"]["result"] == semantic["result_json"]
    assert len(evidence["release"]) == 11 and evidence["reviews"] == []
    assert evidence["generation"]["receipt"] == generation["evidence_json"]["receipt"]
    assert "raw_output" not in evidence and "raw_output" not in evidence["generation"]
    updated = await store.review(
        scope, version, (review, replace(review, candidate_id="candidate:2"))
    )
    with pytest.raises(CheckpointConflict):
        await store.get_candidate(scope, "candidate:1", index.version)
    current = await store.get_candidate(scope, "candidate:1", updated.version)
    assert [r["candidate_id"] for r in json.loads(current.evidence_json)["reviews"]] == [
        "candidate:1"
    ]
    assert current.normalized_output == view.normalized_output
    assert await outputs(tx, scope.run_id) == before


async def test_foreign_scope_missing_candidate_and_invalid_query_do_not_reveal_evidence(reviewable):
    tx, scope, version, _ = reviewable
    store = MySQLEvaluationManagement(tx)
    foreign = replace(scope, organization_id=2)
    with pytest.raises(NotFound):
        await store.list_candidates(foreign)
    with pytest.raises(NotFound):
        await store.get_candidate(foreign, "candidate:1", version)
    with pytest.raises(NotFound):
        await store.get_candidate(scope, "candidate:missing", version)
    for candidate_id, expected_version in ((" ", version), ("candidate:1", 0)):
        with pytest.raises(ValueError):
            await store.get_candidate(scope, candidate_id, expected_version)


async def test_concurrent_review_does_not_mix_snapshot_version_and_audit(reviewable, monkeypatch):
    from qs_ai.infrastructure.persistence.mysql import evaluation_candidates

    tx, scope, version, review = reviewable
    store = MySQLEvaluationManagement(tx)
    original_header = evaluation_candidates.header

    async def concurrent_header(db, trusted):
        row = await original_header(db, trusted)
        await store.review(scope, version, (review,))
        return row

    monkeypatch.setattr(evaluation_candidates, "header", concurrent_header)
    view = await store.get_candidate(scope, "candidate:1", version)
    assert view.version == version and json.loads(view.evidence_json)["reviews"] == []
    monkeypatch.setattr(evaluation_candidates, "header", original_header)
    current = await store.get_candidate(scope, "candidate:1", version + 1)
    assert len(json.loads(current.evidence_json)["reviews"]) == 1


@pytest.mark.parametrize("kind", ["generation", "semantic", "projection"])
async def test_corrupt_evidence_never_becomes_a_review_view(reviewable, kind):
    tx, scope, version, _ = reviewable
    before = await outputs(tx, scope.run_id)
    table = (
        evaluation_generation_completions if kind != "semantic" else evaluation_semantic_completions
    )
    values = {"normalized_output": b"{}"}
    if kind == "projection":
        row = next(r for r in before[0] if r["candidate_id"] == "candidate:1")
        values = {
            "candidate_json": {**row["candidate_json"], "accepted_semantic_execution_id": "other"}
        }
    async with tx.open() as db:
        await db.execute(
            update(table)
            .where(table.c.run_id == str(scope.run_id), table.c.candidate_id == "candidate:1")
            .values(**values)
        )
        await db.commit()
    with pytest.raises((CheckpointConflict, ValueError)):
        await MySQLEvaluationManagement(tx).get_candidate(scope, "candidate:1", version)
