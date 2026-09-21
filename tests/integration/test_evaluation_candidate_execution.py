import asyncio
import json

import pytest
from sqlalchemy import delete, select, update

from qs_ai.application.interpretation.provider import ModelResponse
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_generation_completions,
    evaluation_response_receipts,
    evaluation_runs,
    evaluation_slot_claims,
)
from qs_ai.infrastructure.workflows.evaluation import execute_step
from tests.integration.test_evaluation_step import (
    AT,
)
from tests.integration.test_evaluation_step import (
    persisted_assets as persisted_assets,
)
from tests.integration.test_evaluation_step import (
    ready as ready,
)
from tests.integration.test_evaluation_step import (
    setup_run as setup_run,
)

pytestmark = pytest.mark.integration


@pytest.fixture
async def parallel_run(ready):
    tx, run_id, *_ = ready
    async with tx.open() as db:
        await db.execute(
            update(evaluation_runs)
            .where(evaluation_runs.c.run_id == str(run_id))
            .values(execution_mode="candidate_v2")
        )
        await db.commit()
    try:
        yield ready
    finally:
        async with tx.open() as db:
            for table in (evaluation_response_receipts, evaluation_slot_claims):
                await db.execute(delete(table).where(table.c.run_id == str(run_id)))
            await db.commit()


class ConcurrentGateway:
    def __init__(self, ready):
        self.ready = ready
        self.active = 0
        self.peak = 0
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = []

    async def generate_messages(self, messages, route, schema, invocation_id):
        self.active += 1
        self.peak = max(self.peak, self.active)
        self.calls.append(invocation_id)
        if len(self.calls) == 3:
            self.entered.set()
        try:
            await self.release.wait()
            raw = self.ready[2].normalized_output.decode()
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
            return ModelResponse(
                invocation_id, "request:1", route.model, raw, raw, "none", 1, 2, 10
            )
        finally:
            self.active -= 1


async def execute(ready, gateway, capacity=None):
    tx, run_id, _, routes, schemas = ready
    return await execute_step(
        tx,
        run_id,
        3,
        1,
        "worker:parallel",
        gateway,
        routes,
        schemas,
        clock=lambda: AT,
        candidate_limit=3,
        capacity=capacity,
    )


async def test_three_candidates_overlap_and_stale_run_versions_do_not_lose_results(parallel_run):
    tx, run_id, *_ = parallel_run
    gateway = ConcurrentGateway(parallel_run)
    tasks = [asyncio.create_task(execute(parallel_run, gateway)) for _ in range(3)]
    try:
        await asyncio.wait_for(gateway.entered.wait(), 5)
    finally:
        gateway.release.set()
    results = await asyncio.gather(*tasks)
    assert len(results) == 3 and gateway.peak == 3
    assert len(set(gateway.calls)) == 3
    async with tx.open() as db:
        generations = (
            (
                await db.execute(
                    select(evaluation_generation_completions).where(
                        evaluation_generation_completions.c.run_id == str(run_id)
                    )
                )
            )
            .mappings()
            .all()
        )
        assert len(generations) == 3
        assert len({row["slot_ordinal"] for row in generations}) == 3
        assert (
            await db.execute(
                select(evaluation_slot_claims).where(evaluation_slot_claims.c.run_id == str(run_id))
            )
        ).first() is None
    # The next attempts use each candidate's semantic dependency, not new generation.
    await asyncio.gather(*(execute(parallel_run, gateway) for _ in range(3)))
    async with tx.open() as db:
        candidates = (
            (
                await db.execute(
                    select(evaluation_generation_completions.c.candidate_json).where(
                        evaluation_generation_completions.c.run_id == str(run_id)
                    )
                )
            )
            .scalars()
            .all()
        )
        assert all(candidate["review_ready"] for candidate in candidates)


async def test_worker_cancellation_stops_new_dispatch_and_drains(parallel_run):
    from qs_ai.application.evaluation.management import ManagementScope
    from qs_ai.infrastructure.persistence.mysql.evaluation_cancellation import (
        cancel,
        read_cancellation,
    )
    from qs_ai.infrastructure.persistence.mysql.evaluation_worker import EvaluationWorker
    from tests.integration.test_evaluation_runs import rows

    tx, run_id, _, routes, schemas = parallel_run
    gateway = ConcurrentGateway(parallel_run)
    worker = EvaluationWorker(
        tx,
        gateway,
        routes,
        schemas,
        "worker:parallel",
        enabled=True,
        candidate_limit=3,
        clock=lambda: AT,
    )
    tasks = [asyncio.create_task(worker.once()) for _ in range(3)]
    try:
        await asyncio.wait_for(gateway.entered.wait(), 5)
        version = (await rows(tx, run_id))[2]["version"]
        async with tx.open() as db:
            await cancel(
                db,
                ManagementScope(run_id, 1, 10001),
                version,
                "停止验收",
                AT,
                discard=False,
                confirm=True,
            )
            await db.commit()
        from qs_ai.infrastructure.persistence.mysql.evaluation_management import read_view
        async with tx.open() as db:
            view = await read_view(db, ManagementScope(run_id, 1, 10001))
            assert view.execution_mode == "candidate_v2"
            assert view.active_call_count == 3
            assert view.cancel_draining
            assert view.cancellation_json == ""
            assert json.loads(view.cancel_request_json)["source_version"] == version
        assert await worker.once() is False
    finally:
        gateway.release.set()
    assert all(await asyncio.gather(*tasks))
    assert await worker.once() is True  # Finalize drained intent; no model call.
    run, _, checkpoint = await rows(tx, run_id)
    assert run["progress_json"]["status"] == "canceled"
    assert len(gateway.calls) == 3
    async with tx.open() as db:
        receipt, _ = await read_cancellation(
            db, ManagementScope(run_id, 1, 10001), {**run, **checkpoint}
        )
        assert json.loads(receipt)["status"] == "canceled"


@pytest.mark.parametrize("semantic", [False, True])
async def test_receipt_recovery_commits_without_another_provider_call(
    parallel_run, monkeypatch, semantic
):
    from datetime import timedelta

    from qs_ai.infrastructure.persistence.mysql import evaluation_step
    from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_recovery import (
        recover_candidate,
    )
    from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import (
        active_claims,
        lock_run,
    )

    tx, run_id, _, routes, schemas = parallel_run
    gateway = ConcurrentGateway(parallel_run)
    gateway.release.set()

    async def crash(*args, **kwargs):
        raise RuntimeError("projection crashed")

    if semantic:
        await execute(parallel_run, gateway)
    monkeypatch.setattr(
        evaluation_step, "complete_semantic" if semantic else "complete_evaluated_generation", crash
    )
    with pytest.raises(RuntimeError, match="projection crashed"):
        await execute(parallel_run, gateway)
    monkeypatch.undo()
    async with tx.open() as db:
        await lock_run(db, run_id, 1)
        (claim,) = await active_claims(db, run_id)
    await recover_candidate(tx, 1, claim, AT + timedelta(minutes=6), routes, schemas)
    await recover_candidate(tx, 1, claim, AT + timedelta(minutes=7), routes, schemas)
    assert len(gateway.calls) == (2 if semantic else 1)
    async with tx.open() as db:
        assert (
            len(
                (
                    await db.execute(
                        select(evaluation_generation_completions).where(
                            evaluation_generation_completions.c.run_id == str(run_id)
                        )
                    )
                ).all()
            )
            == 1
        )


async def test_missing_receipt_recovers_unknown_and_preserves_cancel_intent(parallel_run):
    from datetime import timedelta

    from qs_ai.application.evaluation.management import ManagementScope
    from qs_ai.infrastructure.persistence.mysql.evaluation_cancellation import (
        cancel,
        finish_cancellation,
    )
    from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_recovery import (
        recover_candidate,
    )
    from qs_ai.infrastructure.persistence.mysql.evaluation_step import prepare_step
    from tests.integration.test_evaluation_runs import rows

    tx, run_id, _, routes, schemas = parallel_run
    prepared = await prepare_step(
        tx, run_id, 3, 1, "worker:parallel", routes, schemas, clock=lambda: AT, candidate_limit=3
    )
    version = (await rows(tx, run_id))[2]["version"]
    scope = ManagementScope(run_id, 1, 10001)
    async with tx.open() as db:
        await cancel(db, scope, version, "停止验收", AT, discard=False, confirm=True)
        await db.commit()
    await recover_candidate(tx, 1, prepared.claim, AT + timedelta(minutes=6), routes, schemas)
    run, _, _ = await rows(tx, run_id)
    assert run["progress_json"]["status"] == "blocked"
    assert run["progress_json"]["unresolved_result_unknown_count"] == 1
    assert run["progress_json"]["cancel_requested"]
    async with tx.open() as db:
        assert await finish_cancellation(db, scope, AT + timedelta(minutes=7)) is False
    from qs_ai.domain.evaluation.resolution import ResultUnknownResolution
    from qs_ai.infrastructure.persistence.mysql.evaluation_resolution import accept_resolution

    checkpoint = (await rows(tx, run_id))[2]
    async with tx.open() as db:
        await accept_resolution(
            db,
            run_id,
            checkpoint["version"],
            1,
            ResultUnknownResolution(
                prepared.execution_id,
                "authorize_replacement",
                "user:10001",
                "核对完成，保留停止意图",
                True,
                AT + timedelta(minutes=7),
            ),
            confirm=True,
        )
        await db.commit()
    async with tx.open() as db:
        assert await finish_cancellation(db, scope, AT + timedelta(minutes=7))
        await db.commit()
    assert (await rows(tx, run_id))[0]["progress_json"]["status"] == "canceled"


async def test_full_frozen_plan_completes_through_worker(parallel_run):
    from qs_ai.infrastructure.persistence.mysql.evaluation_worker import EvaluationWorker
    from tests.integration.test_evaluation_runs import rows

    tx, run_id, _, routes, schemas = parallel_run
    gateway = ConcurrentGateway(parallel_run)
    gateway.release.set()
    worker = EvaluationWorker(
        tx,
        gateway,
        routes,
        schemas,
        "worker:parallel",
        enabled=True,
        candidate_limit=3,
        clock=lambda: AT,
    )
    for _ in range(40):
        await asyncio.gather(*(worker.once() for _ in range(3)))
        run, _, _ = await rows(tx, run_id)
        if run["progress_json"]["status"] != "collecting":
            break
    assert run["progress_json"]["status"] == "awaiting_review"
    assert len(gateway.calls) == len(set(gateway.calls)) == 70
    async with tx.open() as db:
        candidates = (
            (
                await db.execute(
                    select(evaluation_generation_completions.c.candidate_json).where(
                        evaluation_generation_completions.c.run_id == str(run_id)
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(candidates) == 35
        assert all(c["review_ready"] for c in candidates)


async def test_unknown_blocks_new_dispatch_but_other_calls_commit(parallel_run):
    from qs_ai.application.interpretation.provider import ProviderFailure
    from qs_ai.infrastructure.persistence.mysql.evaluation_worker import EvaluationWorker
    from tests.integration.test_evaluation_runs import rows

    class UnknownFirst(ConcurrentGateway):
        async def generate_messages(self, messages, route, schema, invocation_id):
            if not self.calls:
                self.calls.append(invocation_id)
                await self.entered.wait()
                raise ProviderFailure("provider_timeout", result_unknown=True)
            return await super().generate_messages(messages, route, schema, invocation_id)

    tx, run_id, _, routes, schemas = parallel_run
    gateway = UnknownFirst(parallel_run)
    worker = EvaluationWorker(
        tx,
        gateway,
        routes,
        schemas,
        "worker:parallel",
        enabled=True,
        candidate_limit=3,
        clock=lambda: AT,
    )
    tasks = [asyncio.create_task(worker.once()) for _ in range(3)]
    # Release only the unknown call; the other two remain in flight.
    await asyncio.wait_for(gateway.entered.wait(), 5)
    done, _ = await asyncio.wait(tasks, timeout=5, return_when=asyncio.FIRST_COMPLETED)
    try:
        assert len(done) == 1 and next(iter(done)).result()
        assert await worker.once() is False
        assert len(gateway.calls) == 3
    finally:
        gateway.release.set()
    await asyncio.gather(*tasks)
    run, _, _ = await rows(tx, run_id)
    assert run["progress_json"]["status"] == "blocked"
    assert run["progress_json"]["unresolved_result_unknown_count"] == 1
    async with tx.open() as db:
        assert (
            len(
                (
                    await db.execute(
                        select(evaluation_generation_completions).where(
                            evaluation_generation_completions.c.run_id == str(run_id)
                        )
                    )
                ).all()
            )
            == 3
        )


async def test_prepared_claim_releases_without_unknown_and_renewal_fences_recovery(parallel_run):
    from datetime import timedelta
    from uuid import uuid4

    from qs_ai.application.evaluation.checkpoints import CheckpointConflict
    from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_plan import prepare_candidate
    from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_recovery import (
        recover_candidate,
    )
    from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import lock_run, renew_claim

    tx, run_id, _, routes, schemas = parallel_run
    async with tx.open() as db:
        claim = await prepare_candidate(
            db,
            run_id,
            1,
            "owner:1",
            str(uuid4()),
            str(uuid4()),
            AT,
            AT + timedelta(minutes=5),
            limit=3,
        )
        await db.commit()
    async with tx.open() as db:
        await lock_run(db, run_id, 1)
        renewed = await renew_claim(db, claim, AT + timedelta(minutes=4), AT + timedelta(minutes=9))
        await db.commit()
    with pytest.raises(CheckpointConflict):
        await recover_candidate(tx, 1, claim, AT + timedelta(minutes=6), routes, schemas)
    await recover_candidate(tx, 1, renewed, AT + timedelta(minutes=10), routes, schemas)
    async with tx.open() as db:
        assert (
            await db.execute(
                select(evaluation_slot_claims).where(evaluation_slot_claims.c.run_id == str(run_id))
            )
        ).first() is None
        assert (
            await db.execute(
                select(evaluation_generation_completions).where(
                    evaluation_generation_completions.c.run_id == str(run_id)
                )
            )
        ).first() is None


async def test_cancel_cannot_hide_other_unknown_calls(parallel_run):
    from datetime import timedelta

    from qs_ai.application.evaluation.checkpoints import CheckpointConflict
    from qs_ai.application.evaluation.management import ManagementScope
    from qs_ai.domain.evaluation.resolution import ResultUnknownResolution
    from qs_ai.infrastructure.persistence.mysql.evaluation_cancellation import cancel
    from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_recovery import (
        recover_candidate,
    )
    from qs_ai.infrastructure.persistence.mysql.evaluation_resolution import accept_resolution
    from qs_ai.infrastructure.persistence.mysql.evaluation_step import prepare_step
    from tests.integration.test_evaluation_runs import rows

    tx, run_id, _, routes, schemas = parallel_run
    steps = [
        await prepare_step(
            tx, run_id, 3, 1, "owner:1", routes, schemas, clock=lambda: AT, candidate_limit=3
        )
        for _ in range(2)
    ]
    checkpoint = (await rows(tx, run_id))[2]
    async with tx.open() as db:
        await cancel(
            db,
            ManagementScope(run_id, 1, 10001),
            checkpoint["version"],
            "停止并核对",
            AT,
            discard=False,
            confirm=True,
        )
        await db.commit()
    for step in steps:
        await recover_candidate(tx, 1, step.claim, AT + timedelta(minutes=6), routes, schemas)
    checkpoint = (await rows(tx, run_id))[2]
    decision = ResultUnknownResolution(
        steps[0].execution_id,
        "cancel_run",
        "user:10001",
        "核对调用",
        True,
        AT + timedelta(minutes=7),
    )
    async with tx.open() as db:
        with pytest.raises(CheckpointConflict, match="remaining unknown"):
            await accept_resolution(db, run_id, checkpoint["version"], 1, decision, confirm=True)
    run, _, _ = await rows(tx, run_id)
    assert run["progress_json"]["status"] == "blocked"
    assert run["progress_json"]["unresolved_result_unknown_count"] == 2


async def test_known_failure_does_not_prevent_other_candidates_finishing(parallel_run):
    from qs_ai.application.interpretation.provider import ProviderFailure
    from qs_ai.infrastructure.persistence.mysql.evaluation_worker import EvaluationWorker
    from tests.integration.test_evaluation_runs import rows

    class FailOnce(ConcurrentGateway):
        async def generate_messages(self, messages, route, schema, invocation_id):
            if not self.calls:
                self.calls.append(invocation_id)
                raise ProviderFailure("provider_authentication_failed")
            return await super().generate_messages(messages, route, schema, invocation_id)

    tx, run_id, _, routes, schemas = parallel_run
    gateway = FailOnce(parallel_run)
    gateway.release.set()
    worker = EvaluationWorker(
        tx,
        gateway,
        routes,
        schemas,
        "worker:parallel",
        enabled=True,
        candidate_limit=3,
        clock=lambda: AT,
    )
    for _ in range(40):
        await asyncio.gather(*(worker.once() for _ in range(3)))
        run, _, _ = await rows(tx, run_id)
        if run["progress_json"]["status"] != "collecting":
            break
    assert run["progress_json"]["status"] == "blocked"
    assert len(gateway.calls) == len(set(gateway.calls)) == 69
    async with tx.open() as db:
        candidates = (
            (
                await db.execute(
                    select(evaluation_generation_completions.c.candidate_json).where(
                        evaluation_generation_completions.c.run_id == str(run_id)
                    )
                )
            )
            .scalars()
            .all()
        )
        assert sum(c is not None and c["review_ready"] for c in candidates) == 34


async def test_capacity_exhaustion_rolls_back_claim_and_dispatch(parallel_run):
    from qs_ai.application.evaluation.checkpoints import CheckpointConflict
    from qs_ai.application.execution.model_capacity import ModelCapacity, ProviderCapacity
    from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints

    tx, run_id, *_ = parallel_run
    pool = ModelCapacity({"deepseek": ProviderCapacity(4, 1)}, 3)
    held = [pool.try_acquire("deepseek", evaluation=True) for _ in range(3)]
    gateway = ConcurrentGateway(parallel_run)
    gateway.release.set()
    async with tx.open() as db:
        before = (
            (
                await db.execute(
                    select(evaluation_checkpoints).where(
                        evaluation_checkpoints.c.run_id == str(run_id)
                    )
                )
            )
            .mappings()
            .one()
        )
    with pytest.raises(CheckpointConflict, match="capacity"):
        await execute(parallel_run, gateway, pool)
    assert gateway.calls == []
    async with tx.open() as db:
        after = (
            (
                await db.execute(
                    select(evaluation_checkpoints).where(
                        evaluation_checkpoints.c.run_id == str(run_id)
                    )
                )
            )
            .mappings()
            .one()
        )
        assert dict(before) == dict(after)
        assert (
            await db.execute(
                select(evaluation_slot_claims).where(evaluation_slot_claims.c.run_id == str(run_id))
            )
        ).first() is None
    for token in held:
        token.release()
    await execute(parallel_run, gateway, pool)
    assert len(gateway.calls) == 1
    assert all(pool.try_acquire("deepseek", evaluation=True) for _ in range(3))
