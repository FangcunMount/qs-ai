import json
from dataclasses import replace
from datetime import UTC, datetime

import pytest
from sqlalchemy import delete

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.interpretation.provider import ModelResponse, ProviderFailure
from qs_ai.infrastructure.persistence.mysql.evaluation_progress import (
    execute_preflight,
    transition_requested,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_step import execute_step
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_generation_completions,
    evaluation_semantic_completions,
)
from qs_ai.infrastructure.qs_server.semantic_assets import load_semantic_assets
from tests.integration.test_evaluation_runs import create, rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.test_evaluation_case import release as case_release
from tests.test_generation_completion_assets import assets

pytestmark = pytest.mark.integration
AT = datetime(2026, 9, 12, tzinfo=UTC)


@pytest.fixture
async def ready(setup_run):
    tx, run_id, release = setup_run
    refs, output, routes, schemas = assets()
    semantic = load_semantic_assets()
    release = replace(
        release,
        profile=case_release().profile,
        prompt=case_release().prompt,
        generation_route=refs.generation_route,
        output_schema=refs.output_schema,
        semantic_route=refs.generation_route,
        semantic_prompt=semantic.prompt,
        semantic_output_schema=semantic.output_schema,
    )
    async with tx.open() as db:
        await create(db, run_id, release)
        await transition_requested(db, run_id, 1, 1, "collecting", "actor:1", "开始", AT)
        await execute_preflight(db, run_id, 2, 1, AT)
        await db.commit()
    try:
        yield tx, run_id, output, routes, schemas
    finally:
        async with tx.open() as db:
            for table in (evaluation_semantic_completions, evaluation_generation_completions):
                await db.execute(delete(table).where(table.c.run_id == str(run_id)))
            await db.commit()


class Gateway:
    def __init__(self, context, fail=False):
        self.context, self.fail, self.calls = context, fail, 0

    async def generate_messages(self, messages, route, schema, invocation_id):
        tx, run_id, output, _, _ = self.context
        _, _, checkpoint = await rows(tx, run_id)
        assert checkpoint["checkpoint_json"]["phase"] == "dispatching"
        assert checkpoint["checkpoint_json"]["invocation_id"] == invocation_id
        self.calls += 1
        if self.fail:
            raise ProviderFailure("provider_timeout", retryable=True, result_unknown=True)
        raw = output.normalized_output.decode()
        data = json.loads(messages.data_json)
        if "candidate_output" in data:
            raw = json.dumps(
                {
                    "schema_version": "ai-explanation-semantic-evaluation-output/v1",
                    "scores": {
                        k: 4
                        for k in (
                            "faithfulness",
                            "cross_dimension_quality",
                            "suggestion_actionability",
                            "audience_clarity",
                            "concision",
                        )
                    },
                    "rationale": "评测说明",
                    "decisions": [
                        dict(
                            type=a["type"],
                            scope=a["scope"],
                            ordinal=a["ordinal"],
                            status="passed",
                            detail="语义评测通过",
                        )
                        for a in data["assertions"]
                    ],
                }
            )
        return ModelResponse(invocation_id, "request:1", route.model, raw, raw, "none", 10, 20, 100)


async def step(ready, gateway, version=3):
    tx, run_id, _, routes, schemas = ready
    return await execute_step(
        tx, run_id, version, 1, "worker:step", gateway, routes, schemas, clock=lambda: AT
    )


async def test_generation_then_semantic_call_only_after_visible_commit(ready):
    gateway = Gateway(ready)
    state = await step(ready, gateway)
    assert state.version == 6 and state.checkpoint is None
    state = await step(ready, gateway, state.version)
    assert state.version == 9 and state.checkpoint is None
    assert gateway.calls == 2


async def test_unknown_result_is_persisted_and_not_replayed(ready):
    gateway = Gateway(ready, fail=True)
    state = await step(ready, gateway)
    tx, run_id, *_ = ready
    run, _, _ = await rows(tx, run_id)
    assert run["progress_json"]["status"] == "blocked"
    with pytest.raises(CheckpointConflict):
        await step(ready, gateway, state.version)
    assert gateway.calls == 1


async def test_dispatch_commit_failure_never_calls_provider(ready, monkeypatch):
    from sqlalchemy.ext.asyncio import AsyncSession

    async def fail_commit(self):
        raise RuntimeError("injected commit failure")

    gateway = Gateway(ready)
    monkeypatch.setattr(AsyncSession, "commit", fail_commit)
    with pytest.raises(RuntimeError):
        await step(ready, gateway)
    assert gateway.calls == 0
    monkeypatch.undo()
    tx, run_id, *_ = ready
    assert (await rows(tx, run_id))[2]["version"] == 3


async def test_acceptance_failure_preserves_dispatched_checkpoint_without_replay(
    ready, monkeypatch
):
    from qs_ai.infrastructure.persistence.mysql import evaluation_step

    async def fail_accept(*args, **kwargs):
        raise RuntimeError("injected acceptance failure")

    gateway = Gateway(ready)
    monkeypatch.setattr(evaluation_step, "complete_evaluated_generation", fail_accept)
    with pytest.raises(RuntimeError):
        await step(ready, gateway)
    tx, run_id, *_ = ready
    checkpoint = (await rows(tx, run_id))[2]
    assert checkpoint["version"] == 5 and checkpoint["checkpoint_json"]["phase"] == "dispatching"
    with pytest.raises(CheckpointConflict):
        await step(ready, gateway, 5)
    assert gateway.calls == 1


async def test_malformed_output_is_saved_then_retried_in_original_slot(ready):
    from tests.integration.test_evaluation_completions import stored

    class Malformed(Gateway):
        async def generate_messages(self, *args):
            response = await super().generate_messages(*args)
            return replace(response, raw_output="not-json", validation_output="not-json")

    gateway = Malformed(ready)
    state = await step(ready, gateway)
    tx, run_id, *_ = ready
    assert state.version == 6 and state.checkpoint is None
    records = await stored(tx, run_id)
    assert records[0]["evidence_json"]["failure"]["code"] == "output_schema_invalid"
    assert records[0]["raw_output"] == b"not-json"
    assert records[0]["candidate_id"] is None
    state = await step(ready, gateway, state.version)
    assert state.version == 9 and state.checkpoint is None
    assert (await rows(tx, run_id))[0]["progress_json"]["status"] == "blocked"
    assert gateway.calls == 2
    assert [(r["case_id"], r["slot_ordinal"]) for r in await stored(tx, run_id)] == [
        ("PROMPT-EVAL-001", 1)
    ] * 2


async def test_semantic_decision_mismatch_is_saved_without_regenerating_candidate(ready):
    from sqlalchemy import select

    from tests.integration.test_evaluation_completions import stored

    class WrongDecision(Gateway):
        async def generate_messages(self, *args):
            response = await super().generate_messages(*args)
            output = json.loads(response.validation_output)
            if "decisions" in output:
                output["decisions"][0]["type"] = "unrequested_assertion"
                return replace(
                    response, raw_output=json.dumps(output), validation_output=json.dumps(output)
                )
            return response

    gateway = WrongDecision(ready)
    state = await step(ready, gateway)
    state = await step(ready, gateway, state.version)
    tx, run_id, *_ = ready
    assert state.version == 9 and state.checkpoint is None
    async with tx.open() as db:
        evidence = (
            await db.execute(
                select(evaluation_semantic_completions.c.evidence_json).where(
                    evaluation_semantic_completions.c.run_id == str(run_id)
                )
            )
        ).scalar_one()
    assert evidence["failure"]["code"] == "semantic_decision_contract_invalid"
    assert (await stored(tx, run_id))[0]["candidate_json"]["review_ready"] is False
    assert len(await stored(tx, run_id)) == 1
    with pytest.raises(CheckpointConflict):
        await step(ready, gateway, state.version)
    assert gateway.calls == 2
