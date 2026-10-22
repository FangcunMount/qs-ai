"""Actual candidate host entrypoints and MySQL roots; controlled transport is not Broker proof."""

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from reliable_messaging.transaction import Decision, LocalOutcome, Result, StoreChecker
from sqlalchemy import delete, text, update

from qs_ai.domain.evaluation.failure import ClassifiedFailure
from qs_ai.infrastructure.persistence.mysql.database import EventSession
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_response import (
    read_candidate_response,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_response_receipts import (
    EvaluationResponse,
    save_response,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import active_claims
from qs_ai.infrastructure.persistence.mysql.evaluation_step import prepare_step
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_response_receipts,
    evaluation_runs,
    evaluation_slot_claims,
)
from qs_ai.infrastructure.workflow_transport.candidate_completion_sender import (
    CandidateCompletionSender,
)
from qs_ai.infrastructure.workflow_transport.candidate_completion_state import (
    CandidateCompletionPlanner,
    CandidateCompletionWrite,
    read_candidate_completion_plan,
)
from qs_ai.infrastructure.workflow_transport.candidate_dispatch_sender import (
    CandidateDispatchSender,
)
from qs_ai.infrastructure.workflow_transport.candidate_dispatch_state import (
    CandidateDispatchPlanner,
)
from qs_ai.infrastructure.workflow_transport.candidate_runtime import CandidateRuntime
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import read_evaluation_state_plan
from qs_ai.infrastructure.workflow_transport.evaluation_state_root import EvaluationStateRoot
from qs_ai.infrastructure.workflows.evaluation import execute_step
from tests.integration.test_evaluation_candidate_execution import ConcurrentGateway
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import AT
from tests.integration.test_evaluation_step import persisted_assets as persisted_assets
from tests.integration.test_evaluation_step import ready as ready
from tests.integration.test_evaluation_unknowns import snapshot
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_mq_storage import keys as keys
from tests.integration.test_transaction_evaluation_admission import trigger_admin as trigger_admin
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.integration.test_transaction_worker_dispatch import original as original

pytestmark = pytest.mark.integration


@pytest.fixture
async def candidate_host(original):
    p = original
    recorder = p.host.tx.database.state_events
    async with p.tx.open() as db:
        await db.execute(
            update(evaluation_runs)
            .where(evaluation_runs.c.run_id == str(p.run_id))
            .values(execution_mode="candidate_v2")
        )
        await db.commit()
    gateway = ConcurrentGateway(p.ready)
    gateway.release.set()
    planner = CandidateCompletionPlanner(
        p.host.tx, signing_key=p.host.keys["ai.sign"], recipient_key=p.host.keys["qs.encrypt"]
    )
    try:
        yield SimpleNamespace(**locals())
    finally:
        p.host.tx.database.state_events = recorder
        async with p.tx.open() as db:
            for table in (evaluation_response_receipts, evaluation_slot_claims):
                await db.execute(delete(table).where(table.c.run_id == str(p.run_id)))
            await db.commit()


@pytest.fixture(params=["generation", "semantic"])
async def durable(candidate_host, request):
    s, p = candidate_host, candidate_host.p
    version = 3
    if request.param == "semantic":
        generated = await execute_step(
            p.tx,
            p.run_id,
            version,
            1,
            "worker:original",
            s.gateway,
            p.routes,
            p.schemas,
            clock=lambda: AT,
            candidate_limit=3,
        )
        version = generated.version
    prepared = await prepare_step(
        p.tx,
        p.run_id,
        version,
        1,
        "worker:original",
        p.routes,
        p.schemas,
        clock=lambda: AT + timedelta(seconds=2),
        candidate_limit=3,
    )
    assert prepared.cp.kind == request.param and prepared.claim is not None
    response = await s.gateway.generate_messages(
        prepared.messages, prepared.route, prepared.schema, prepared.invocation_id
    )

    async def save(unknown=False):
        failure = (
            ClassifiedFailure(
                "generation_execution" if request.param == "generation" else "semantic_evaluation",
                "result_unknown",
                "provider_result_unknown",
                False,
                True,
                "manual_acknowledgement",
                "Original unknown response",
                (prepared.execution_id,),
            )
            if unknown
            else None
        )
        result = EvaluationResponse(
            None if unknown else response, failure, AT + timedelta(seconds=3)
        )
        async with p.tx.open() as db:
            await save_response(db, 1, prepared.claim, result, at=result.finished_at)
            await db.commit()
        async with p.tx.open() as db:
            first = await read_candidate_response(db, 1, prepared.claim)
        assert first is not None
        return first.reference

    return SimpleNamespace(**locals())


@pytest.mark.parametrize("unknown", [False, True])
@pytest.mark.parametrize("other_active", [False, True])
@pytest.mark.parametrize("fault", ["none", "witness_rejected", "client_reply_discarded"])
async def test_durable_candidate_generation_and_semantic_share_original_root_and_exact_first_result(
    durable,
    trigger_admin,
    monkeypatch,
    unknown,
    other_active,
    fault,
):
    d, s, p = durable, durable.s, durable.p
    reference = await d.save(unknown)
    if other_active:
        await prepare_step(
            p.tx,
            p.run_id,
            d.prepared.state.version,
            1,
            "worker:other",
            p.routes,
            p.schemas,
            clock=lambda: AT + timedelta(seconds=2),
            candidate_limit=3,
        )
    before, calls = await snapshot(p.tx, p.run_id), len(s.gateway.calls)
    intent = await s.planner.intent(reference)
    winner = await s.planner.candidate(reference)
    facts, operation = read_candidate_completion_plan(intent, winner)
    assert await snapshot(p.tx, p.run_id) == before
    reserved = await p.host.store.reserve(intent.request, winner)
    assert reserved.acquired
    p.host.executions.append(reserved.execution)
    business = CandidateCompletionWrite()
    root = EvaluationStateRoot(
        p.host.tx,
        p.host.archives,
        p.host.store,
        intent,
        winner,
        business,
        isolation_level="REPEATABLE READ",
    )
    trigger = None
    try:
        p.host.tx.database.state_events = SimpleNamespace(
            flush=AsyncMock(side_effect=AssertionError("No legacy publication in candidate root"))
        )
        if fault == "witness_rejected":
            trigger = "candidate_witness_" + uuid4().hex
            async with trigger_admin.begin() as db:
                await db.execute(
                    text(
                        f"CREATE TRIGGER {trigger} BEFORE UPDATE ON rm_tx_witness FOR EACH ROW "
                        "BEGIN IF NEW.state='committed' THEN SIGNAL SQLSTATE '45000' "
                        "SET MESSAGE_TEXT='isolated candidate witness refusal'; END IF; END"
                    )
                )
        with monkeypatch.context() as patch:
            from qs_ai.infrastructure.persistence.mysql import evaluation_step
            from qs_ai.infrastructure.workflow_transport import candidate_completion_state

            def forbidden(*args, **kwargs):
                raise AssertionError("No identity generation or Seal in original candidate root")

            patch.setattr(evaluation_step, "uuid4", forbidden)
            patch.setattr(candidate_completion_state, "uuid4", forbidden)
            patch.setattr(candidate_completion_state, "prepare", forbidden)
            if fault == "client_reply_discarded":
                commit = EventSession.commit

                async def discarded(db):
                    await commit(db)
                    raise ConnectionError("client discarded candidate completion commit reply")

                patch.setattr(EventSession, "commit", discarded)
            outcome = await root(reserved.execution)
        committed = fault != "witness_rejected"
        assert outcome == (LocalOutcome.COMMITTED if committed else LocalOutcome.ABORTED)
        assert await StoreChecker(p.host.store).check(reserved.execution.references[0]) == (
            Decision.COMMIT if committed else Decision.ROLLBACK
        )
        if committed:
            assert business.state.version == facts.source_version + 1
            _, first = read_evaluation_state_plan(intent, winner)
            assert await p.host.archives.read_committed(first.envelope().message_id) == first
            runtime = SimpleNamespace(store=p.host.store, reserve=AsyncMock(), execute=AsyncMock())
            duplicate = await CandidateCompletionSender(
                p.host.tx, p.host.archives, runtime, s.planner
            )(reference)
            assert duplicate == business.state
            runtime.reserve.assert_not_called()
            runtime.execute.assert_not_called()
            async with p.tx.open() as db:
                remaining = await active_claims(db, p.run_id, lock=False)
            assert len(remaining) == int(other_active)
        else:
            assert await snapshot(p.tx, p.run_id) == before
        assert len(s.gateway.calls) == calls
    finally:
        if trigger:
            async with trigger_admin.begin() as db:
                await db.execute(text(f"DROP TRIGGER IF EXISTS {trigger}"))


async def test_actual_candidate_graph_both_stages_without_legacy_hook(
    candidate_host,
):
    s, p = candidate_host, candidate_host.p
    current = AT

    async def execute(execution, root):
        # The host native test controls the transport only; the SDK store and
        # original business/archive/witness transactions remain real MySQL.
        p.host.executions.append(execution)
        return Result(await root(execution), ())

    transport = SimpleNamespace(store=p.host.store, reserve=p.host.store.reserve, execute=execute)
    dispatch = CandidateDispatchSender(
        p.host.tx,
        p.host.archives,
        transport,
        "worker:original",
        CandidateDispatchPlanner(
            p.host.tx,
            p.schemas,
            signing_key=p.host.keys["ai.sign"],
            recipient_key=p.host.keys["qs.encrypt"],
            limit=3,
            clock=lambda: current,
        ),
    )
    runtime = CandidateRuntime(
        dispatch, CandidateCompletionSender(p.host.tx, p.host.archives, transport, s.planner)
    )
    original_generate = s.gateway.generate_messages

    async def generate_with_original_lease_renewal(*args, **kwargs):
        async with p.tx.open() as db:
            claims = await active_claims(db, p.run_id, lock=False)
        assert len(claims) == 1
        claim = claims[0]
        await runtime.renew(
            1, claim, current, claim.checkpoint.lease_expires_at + timedelta(minutes=1)
        )
        return await original_generate(*args, **kwargs)

    s.gateway.generate_messages = generate_with_original_lease_renewal
    p.host.tx.database.state_events = SimpleNamespace(
        flush=AsyncMock(
            side_effect=AssertionError("Transaction candidate graph cannot use legacy hook")
        )
    )
    version = 3
    for expected in (5, 7):
        current += timedelta(seconds=2)
        result = await execute_step(
            p.host.tx,
            p.run_id,
            version,
            1,
            "worker:original",
            s.gateway,
            p.routes,
            p.schemas,
            clock=lambda at=current: at + timedelta(seconds=1),
            candidate_limit=3,
            candidate_runtime=runtime,
        )
        assert result.version == expected and result.checkpoint is None
        version = result.version
    assert len(s.gateway.calls) == 2 and len(set(s.gateway.calls)) == 2
    assert len(p.host.executions) == 4
    for execution in p.host.executions:
        assert await StoreChecker(p.host.store).check(execution.references[0]) == Decision.COMMIT


async def test_other_claim_requires_explicit_old_abort_replan_without_new_provider_call(durable):
    from reliable_messaging.durable import MessageConflict
    from reliable_messaging.transaction import ReplanAuthorization

    d, s, p = durable, durable.s, durable.p
    reference = await d.save()
    intent = await s.planner.intent(reference)
    winner = await s.planner.candidate(reference)
    _, first = read_candidate_completion_plan(intent, winner)
    reserved = await p.host.store.reserve(intent.request, winner)
    p.host.executions.append(reserved.execution)
    await prepare_step(
        p.tx,
        p.run_id,
        d.prepared.state.version,
        1,
        "worker:other",
        p.routes,
        p.schemas,
        clock=lambda: AT + timedelta(seconds=2),
        candidate_limit=3,
    )
    before, calls = await snapshot(p.tx, p.run_id), len(s.gateway.calls)
    root = EvaluationStateRoot(
        p.host.tx,
        p.host.archives,
        p.host.store,
        intent,
        winner,
        CandidateCompletionWrite(),
        isolation_level="REPEATABLE READ",
    )
    assert await root(reserved.execution) == LocalOutcome.ABORTED
    assert await snapshot(p.tx, p.run_id) == before
    with pytest.raises(MessageConflict):
        await s.planner.verify(reference, winner)
    replacement = await s.planner.candidate(reference, candidate_id=first.candidate_id)
    facts, operation = read_candidate_completion_plan(intent, replacement)
    assert operation.reference == first.reference
    assert operation.response_hash == first.response_hash
    assert operation.candidate_id == first.candidate_id
    assert replacement.digest != winner.digest
    retried = await p.host.store.replan(ReplanAuthorization(reserved.execution, replacement))
    assert retried.acquired
    p.host.executions.append(retried.execution)
    business = CandidateCompletionWrite()
    root = EvaluationStateRoot(
        p.host.tx,
        p.host.archives,
        p.host.store,
        intent,
        replacement,
        business,
        isolation_level="REPEATABLE READ",
    )
    assert await root(retried.execution) == LocalOutcome.COMMITTED
    assert business.state.version == facts.source_version + 1
    assert len(s.gateway.calls) == calls
    assert (
        await StoreChecker(p.host.store).check(reserved.execution.references[0])
        == Decision.ROLLBACK
    )
    assert (
        await StoreChecker(p.host.store).check(retried.execution.references[0]) == Decision.COMMIT
    )
