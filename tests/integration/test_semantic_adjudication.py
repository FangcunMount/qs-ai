import json
from dataclasses import replace

import pytest
from sqlalchemy import update

from qs_ai.domain.evaluation.review import CONTRADICTION_POLICY, SemanticContradictionReview
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_runs
from tests.integration.test_evaluation_completions import dispatched as dispatched
from tests.integration.test_evaluation_reviews import outputs
from tests.integration.test_evaluation_reviews import reviewable as reviewable
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_semantic_completions import judge as judge

pytestmark = pytest.mark.integration


async def test_read_derived_adjudication_only_after_two_evidence_bound_signatures(reviewable):
    tx, scope, version, first = reviewable
    store = MySQLEvaluationManagement(tx)
    original = await outputs(tx, scope.run_id)
    generation = next(r for r in original[0] if r["candidate_id"] == first.candidate_id)
    candidate = generation["candidate_json"]
    semantic = next(
        r for r in original[1] if r["execution_id"] == candidate["accepted_semantic_execution_id"]
    )
    assertion = next(
        a
        for a in candidate["assertions"]
        if a["type"] == "forbidden_claims_absent" and a["scope"] == "default"
    )
    note = SemanticContradictionReview(
        CONTRADICTION_POLICY,
        semantic["execution_id"],
        semantic["evidence_json"]["output_fingerprint"],
        assertion["ordinal"],
        assertion["detail"],
        generation["normalized_output"].decode()[:50],
        "核对原文后确认误判",
    )
    first = replace(first, semantic_review=note)
    state = await store.review(scope, version, (first,))
    view = await store.get_candidate(scope, first.candidate_id, state.version)
    evidence = json.loads(view.evidence_json)
    assert evidence["semantic_adjudication"] is None
    assert evidence["effective_assertions"] == candidate["assertions"]
    second_scope = replace(scope, operator_user_id=43)
    second = replace(first, reviewer=second_scope.actor, role="safety_product")
    state = await store.review(second_scope, state.version, (second,))
    view = await store.get_candidate(scope, first.candidate_id, state.version)
    evidence = json.loads(view.evidence_json)
    assert evidence["candidate"] == candidate and state.status == "awaiting_review"
    assert evidence["semantic_adjudication"]["reviewers"] == ["user:42", "user:43"]
    assert evidence["semantic_adjudication"]["original_status"] == "failed"
    changed = [
        (old, new)
        for old, new in zip(candidate["assertions"], evidence["effective_assertions"], strict=True)
        if old != new
    ]
    assert len(changed) == 1 and changed[0][0] == assertion
    assert changed[0][1] == {**assertion, "status": "passed"}
    assert await outputs(tx, scope.run_id) == original
    # Corrupted audit records are not treated as valid approvals during reads.
    run, _, _ = await rows(tx, scope.run_id)
    progress = run["progress_json"]
    progress["human_reviews"][1]["reviewer"] = scope.actor
    async with tx.open() as db:
        await db.execute(
            update(evaluation_runs)
            .where(evaluation_runs.c.run_id == str(scope.run_id))
            .values(progress_json=progress)
        )
        await db.commit()
    with pytest.raises(ValueError):
        await store.get_candidate(scope, first.candidate_id, state.version)
