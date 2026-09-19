"""Versioned gate changes use real disposable MySQL evidence, never production approvals."""

import json
from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import update

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.domain.evaluation.acceptance import RULE_VERSION, rule_document
from qs_ai.domain.evaluation.review import CandidateHumanReview
from qs_ai.infrastructure.persistence.mysql.evaluation_finalization import save_progress
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_runs
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import AT, Gateway
from tests.integration.test_evaluation_step import ready as ready
from tests.integration.test_semantic_contract_recovery import accept, blocked, originals, resume

pytestmark = pytest.mark.integration


async def seed_legacy_creation(ready):
    """Only in the disposable fixture: reproduce the pre-upgrade creation document."""
    tx, run_id, *_ = ready
    before, _, _ = await rows(tx, run_id)
    definition = json.loads(before["definition_json"])
    assert definition.pop("acceptance_rule")["version"] == RULE_VERSION
    async with tx.open() as db:
        await db.execute(
            update(evaluation_runs)
            .where(evaluation_runs.c.run_id == str(run_id))
            .values(definition_json=json.dumps(definition))
        )
        await db.commit()


async def seed_retained_adoption(ready, version):
    """Isolated database fixture for the already-retained production audit format.

    The retired write command is intentionally absent from the runtime package.
    This fixture does not authorize a live policy change or sign a human review.
    """
    tx, run_id, *_ = ready
    before, _, _ = await rows(tx, run_id)
    adoption = {
        "rule": rule_document(),
        "actor": "operator:root",
        "reason": "Historical fixture only",
        "adopted_at": (AT + timedelta(seconds=4)).isoformat(),
        "source_version": version,
        "version": version + 1,
        "release_fingerprint": json.loads(before["definition_json"])["release_fingerprint"],
    }
    async with tx.open() as db:
        await save_progress(
            db,
            ManagementScope(run_id, 1, 42),
            version,
            {**before["progress_json"], "acceptance_rule_adoption": adoption},
        )
        await db.commit()


async def finish(ready, state):
    for _ in range(68):
        state = await resume(ready, state, Gateway(ready))
    return state


async def test_retained_adoption_reconstructs_gates_and_preserves_every_record(ready):
    await seed_legacy_creation(ready)
    state, value = await blocked(ready)
    state = await accept(ready, state, value)
    state = await resume(ready, state, Gateway(ready))
    state = await finish(ready, state)
    tx, run_id, *_ = ready
    store = MySQLEvaluationManagement(tx)
    scope = ManagementScope(run_id, 1, 42)
    before = await rows(tx, run_id)
    evidence = await originals(ready)
    legacy = await store.preview_gates(scope, state.version, AT + timedelta(seconds=4))
    assert dict(legacy.gate_passes)["G3"] is False
    await seed_retained_adoption(ready, state.version)
    after = await rows(tx, run_id)
    assert after[0]["definition_json"] == before[0]["definition_json"]
    assert {
        k: v for k, v in after[0]["progress_json"].items() if k != "acceptance_rule_adoption"
    } == before[0]["progress_json"]
    assert after[2]["version"] == state.version + 1
    assert await originals(ready) == evidence
    # Fresh adapter reconstructs the new rule from audit after a process restart.
    current = await MySQLEvaluationManagement(tx).preview_gates(
        scope, state.version + 1, AT + timedelta(seconds=5)
    )
    assert dict(current.gate_passes)["G3"] is True
    assert dict(current.gate_passes)["G5"] is False
    assert {m.name: m for m in current.quality.metrics}[
        "observed_semantic_execution_success_rate"
    ].value == 35 / 36
    # Synthetic reviews exercise finalization readback; they are never production approvals.
    batch = tuple(
        CandidateHumanReview(
            r["candidate_id"],
            "assessment_semantics",
            scope.actor,
            "approve",
            AT + timedelta(seconds=5),
            "隔离测试审核",
        )
        for r in evidence[0]
    )
    reviewed = await store.review(scope, state.version + 1, batch)
    other = ManagementScope(run_id, 1, 43)
    reviewed = await store.review(
        other,
        reviewed.version,
        tuple(replace(r, role="safety_product", reviewer=other.actor) for r in batch),
    )
    # The reused fixture output is deliberately inadequate for some cases: G4 stays blocking.
    final = await store.finalize(
        scope, reviewed.version, False, "案例质量未达标", AT + timedelta(seconds=6), confirm=True
    )
    assert final.status == "rejected"
    assert await MySQLEvaluationManagement(tx).get(scope) == final
    assert json.loads(final.finalization_json)["gate_result"]["gate_passes"]["G3"] is True


async def test_new_creation_freezes_rule_and_no_adoption_is_needed(ready):
    run, _, _ = await rows(ready[0], ready[1])
    assert json.loads(run["definition_json"])["acceptance_rule"]["version"] == RULE_VERSION
    state, value = await blocked(ready)
    state = await accept(ready, state, value)
    state = await resume(ready, state, Gateway(ready))
    state = await finish(ready, state)
    store = MySQLEvaluationManagement(ready[0])
    preview = await store.preview_gates(
        ManagementScope(ready[1], 1, 42), state.version, AT + timedelta(seconds=4)
    )
    assert dict(preview.gate_passes)["G3"] is True
