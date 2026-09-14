"""Recover real persisted failed evidence with a synthetic provider; no production approvals."""

import asyncio
import json
from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import select

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.domain.evaluation.contract_recovery import RECOVERY_INSTRUCTION, ContractRecovery
from qs_ai.domain.evaluation.review import CandidateHumanReview
from qs_ai.infrastructure.persistence.mysql.evaluation_contract_recovery import (
    authorize_contract_recovery,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.evaluation_projection import decode_semantic_completion
from qs_ai.infrastructure.persistence.mysql.evaluation_step import execute_step
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_generation_completions,
    evaluation_semantic_completions,
)
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import AT, Gateway, step
from tests.integration.test_evaluation_step import ready as ready

pytestmark = pytest.mark.integration


class MissingDecision(Gateway):
    async def generate_messages(self, messages, route, schema, invocation_id):
        response = await super().generate_messages(messages, route, schema, invocation_id)
        data = json.loads(response.validation_output)
        data["decisions"] = data["decisions"][:-1]
        raw = json.dumps(data)
        return replace(response, raw_output=raw, validation_output=raw)


async def originals(ready):
    tx, run_id, *_ = ready
    async with tx.open() as db:
        return [
            list((await db.execute(select(table).where(table.c.run_id == str(run_id)))).mappings())
            for table in (evaluation_generation_completions, evaluation_semantic_completions)
        ]


async def blocked(ready):
    state = await step(ready, Gateway(ready))
    state = await step(ready, MissingDecision(ready), state.version)
    target = decode_semantic_completion((await originals(ready))[1][0])
    assert target.status == "failed"
    assert target.failure.code == "semantic_decision_contract_invalid"
    value = ContractRecovery(
        target.execution_id,
        target.candidate_id,
        target.candidate_output_fingerprint,
        target.output_fingerprint,
        "operator:root",
        "修复缺项，允许原候选一次额外评判，保留原策略和失败证据",
        AT + timedelta(seconds=1),
        True,
    )
    return state, value


async def accept(ready, state, value, *, commit=True, org=1, fingerprint=None, confirm=True):
    tx, run_id, *_ = ready
    run, _, _ = await rows(tx, run_id)
    release = json.loads(run["definition_json"])["release_fingerprint"]
    async with tx.open() as db:
        result = await authorize_contract_recovery(
            db, run_id, state.version, org, fingerprint or release, value, confirm=confirm
        )
        if commit:
            await db.commit()
        return result


async def resume(ready, state, gateway):
    tx, run_id, _, routes, schemas = ready
    return await execute_step(
        tx,
        run_id,
        state.version,
        1,
        "worker:restarted",
        gateway,
        routes,
        schemas,
        clock=lambda: AT + timedelta(seconds=2),
    )


async def test_resume_exact_candidate_preserves_originals_and_allows_review_and_gate(ready):
    state, value = await blocked(ready)
    tx, run_id, *_ = ready
    before_run = await rows(tx, run_id)
    before = await originals(ready)
    await accept(ready, state, value, commit=False)
    assert await rows(tx, run_id) == before_run
    state = await accept(ready, state, value)

    class Inspect(Gateway):
        async def generate_messages(self, messages, route, schema, invocation_id):
            assert RECOVERY_INSTRUCTION in messages.task_message
            assert "candidate_output" in json.loads(messages.data_json)
            return await super().generate_messages(messages, route, schema, invocation_id)

    gateway = Inspect(ready)
    state = await resume(ready, state, gateway)
    assert gateway.calls == 1
    after = await originals(ready)
    assert [{k: v for k, v in r.items() if k != "candidate_json"} for r in after[0]] == [
        {k: v for k, v in r.items() if k != "candidate_json"} for r in before[0]
    ]
    assert [
        a for a in after[0][0]["candidate_json"]["assertions"] if a["evaluator"] == "deterministic"
    ] == [
        a for a in before[0][0]["candidate_json"]["assertions"] if a["evaluator"] == "deterministic"
    ]
    assert next(r for r in after[1] if r["execution_id"] == value.execution_id) == before[1][0]
    assert sorted(r["execution_ordinal"] for r in after[1]) == [1, 2]
    assert {r["candidate_id"] for r in after[1]} == {value.candidate_id}
    # Complete the full 35-slot inventory; the failed attempt remains counted.
    gateway = Gateway(ready)
    for _ in range(68):
        state = await resume(ready, state, gateway)
    run, _, _ = await rows(tx, run_id)
    assert run["definition_json"] == before_run[0]["definition_json"]
    assert run["progress_json"]["status"] == "awaiting_review"
    generations, semantics = await originals(ready)
    assert len(generations) == 35 and len(semantics) == 36
    scope = ManagementScope(run_id, 1, 42)
    store = MySQLEvaluationManagement(tx)
    # No signature / approval is manufactured. Closure must pass before gate preview.
    preview = await store.preview_gates(scope, state.version, AT + timedelta(seconds=3))
    assert preview.version == state.version
    assert dict(preview.gate_passes)["G5"] is False
    candidate = await store.get_candidate(scope, value.candidate_id, state.version)
    assert candidate.candidate_id == value.candidate_id
    batch = tuple(
        CandidateHumanReview(
            r["candidate_id"],
            "assessment_semantics",
            scope.actor,
            "approve",
            AT + timedelta(seconds=3),
            "隔离测试的审核回执",
        )
        for r in generations
    )
    reviewed = await store.review(scope, state.version, batch)
    assert len(json.loads(reviewed.reviews_json)) == 35
    assert (await originals(ready))[1] == semantics


@pytest.mark.parametrize(
    "case", ["stale", "org", "release", "target", "fingerprint", "unconfirmed"]
)
async def test_reject_wrong_or_stale_authorization_without_mutation(ready, case):
    state, value = await blocked(ready)
    before = await rows(ready[0], ready[1]), await originals(ready)
    options = {}
    if case == "stale":
        state = replace(state, version=state.version - 1)
    if case == "org":
        options["org"] = 2
    if case == "release":
        options["fingerprint"] = "sha256:" + "0" * 64
    if case == "target":
        value = replace(value, execution_id="missing")
    if case == "fingerprint":
        value = replace(value, output_fingerprint="sha256:" + "0" * 64)
    if case == "unconfirmed":
        options["confirm"] = False
    with pytest.raises((ValueError, CheckpointConflict)):
        await accept(ready, state, value, **options)
    assert (await rows(ready[0], ready[1]), await originals(ready)) == before


async def test_concurrent_and_duplicate_authorizations_are_not_replayed(ready):
    state, value = await blocked(ready)
    results = await asyncio.gather(
        accept(ready, state, value), accept(ready, state, value), return_exceptions=True
    )
    assert sum(isinstance(r, CheckpointConflict) for r in results) == 1
    with pytest.raises(CheckpointConflict):
        await accept(ready, state, value)
    updated = next(r for r in results if not isinstance(r, Exception))
    with pytest.raises(ValueError):
        await accept(ready, updated, value)


async def test_second_failure_stops_without_third_call(ready):
    state, value = await blocked(ready)
    state = await accept(ready, state, value)
    gateway = MissingDecision(ready)
    state = await resume(ready, state, gateway)
    run, _, _ = await rows(ready[0], ready[1])
    assert run["progress_json"]["status"] == "blocked"
    assert run["progress_json"]["transitions"][-1]["cause_code"] == "semantic_budget_exhausted"
    with pytest.raises((ValueError, CheckpointConflict)):
        await accept(ready, state, value)
    with pytest.raises(CheckpointConflict):
        await resume(ready, state, gateway)
    assert gateway.calls == 1
