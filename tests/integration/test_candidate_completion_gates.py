"""Versioned gate changes use real disposable MySQL evidence, never production approvals."""

import asyncio
import json
from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import update

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.evaluation.acceptance import RULE_VERSION
from qs_ai.domain.evaluation.review import CandidateHumanReview
from qs_ai.infrastructure.persistence.mysql.evaluation_acceptance import adopt_acceptance_rule
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


async def adopt(ready, version, *, commit=True, org=1, fingerprint=None, confirm=True):
    tx, run_id, *_ = ready
    before, _, _ = await rows(tx, run_id)
    release = json.loads(before["definition_json"])["release_fingerprint"]
    async with tx.open() as db:
        value = await adopt_acceptance_rule(
            db,
            ManagementScope(run_id, org, 42),
            version,
            fingerprint or release,
            "operator:root",
            "采用最终候选完成率，保留全部调用记录",
            AT + timedelta(seconds=4),
            confirm=confirm,
        )
        if commit:
            await db.commit()
        return value


async def finish(ready, state):
    for _ in range(68):
        state = await resume(ready, state, Gateway(ready))
    return state


async def test_recovered_legacy_run_changes_only_gate_basis_and_preserves_every_record(ready):
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
    preview = await adopt(ready, state.version, commit=False)
    assert preview["before_gates"]["G3"] is False and preview["after_gates"]["G3"] is True
    assert await rows(tx, run_id) == before and await originals(ready) == evidence
    for options in ({"org": 2}, {"fingerprint": "sha256:" + "0" * 64}, {"confirm": False}):
        with pytest.raises((ValueError, CheckpointConflict, NotFound)):
            await adopt(ready, state.version, **options)
        assert await rows(tx, run_id) == before
    concurrent = await asyncio.gather(
        adopt(ready, state.version), adopt(ready, state.version), return_exceptions=True
    )
    assert sum(isinstance(r, CheckpointConflict) for r in concurrent) == 1
    result = next(r for r in concurrent if isinstance(r, dict))
    assert result["adoption"]["rule"]["version"] == RULE_VERSION
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
    with pytest.raises(CheckpointConflict):
        await adopt(ready, state.version)
    with pytest.raises(ValueError):
        await adopt(ready, state.version + 1)
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
    with pytest.raises(ValueError):
        await adopt(ready, final.version)


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
    with pytest.raises(ValueError):
        await adopt(ready, state.version)


@pytest.mark.parametrize(
    "case", ["wrong_org", "stale", "unconfirmed", "wrong_release", "collecting"]
)
async def test_rejects_invalid_changes_without_mutation(ready, case):
    await seed_legacy_creation(ready)
    before = await rows(ready[0], ready[1])
    options = {}
    if case == "wrong_org":
        options["org"] = 2
    if case == "wrong_release":
        options["fingerprint"] = "sha256:" + "0" * 64
    if case == "unconfirmed":
        options["confirm"] = False
    with pytest.raises((ValueError, CheckpointConflict, NotFound)):
        await adopt(ready, 2 if case == "stale" else 3, **options)
    assert await rows(ready[0], ready[1]) == before
