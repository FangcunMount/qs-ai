"""Serial expiry, terminal evidence and witness on real MySQL; no Broker/model proof."""

from uuid import uuid4

import pytest
from reliable_messaging.transaction import Decision, LocalOutcome, StoreChecker
from sqlalchemy import text

from qs_ai.infrastructure.persistence.mysql.completion_records import (
    generation_completions,
    semantic_completions,
)
from qs_ai.infrastructure.persistence.mysql.database import EventSession
from qs_ai.infrastructure.persistence.mysql.transaction_schema import archive
from qs_ai.infrastructure.workflow_transport.evaluation_state_root import EvaluationStateRoot
from qs_ai.infrastructure.workflow_transport.expired_recovery_state import (
    ExpiredRecoveryPlanner,
    ExpiredRecoveryRequest,
    apply_expired_recovery,
    read_expired_recovery_plan,
)
from tests.integration.test_evaluation_recovery import EXPIRY, pending
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import ready as ready
from tests.integration.test_evaluation_step import step
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_transaction_evaluation_admission import trigger_admin as trigger_admin
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.integration.test_transaction_worker_dispatch import original as original
from tests.messaging_fixtures import keys as keys
from tests.route_fixtures import legacy_routes

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("mode", ["prepared", "generation", "semantic"])
@pytest.mark.parametrize("fault", ["none", "archive_rejected", "client_reply_discarded"])
async def test_original_serial_expiry_audit_terminal_archive_and_witness_are_atomic(
    original, trigger_admin, monkeypatch, mode, fault
):
    p = original
    version = 3
    if mode == "semantic":
        version = (await step(p.ready, p.gateway)).version
    checkpoint = await pending(p.ready, dispatched=mode != "prepared", version=version)
    before = await rows(p.tx, p.run_id)
    request = ExpiredRecoveryRequest(p.run_id, 1, checkpoint.version, "invocation:dead", EXPIRY)
    planner = ExpiredRecoveryPlanner(
        p.host.tx,
        signing_key=p.host.keys["ai.sign"],
        recipient_key=p.host.keys["qs.encrypt"],
        route_resolver=legacy_routes(),
    )
    winner = await planner.candidate(request, "system:recovery", EXPIRY)
    reservation = await p.host.store.reserve(request.intent.request, winner)
    assert reservation.acquired
    owned = reservation.execution
    p.host.executions.append(owned)
    facts, actor, first = read_expired_recovery_plan(
        request, winner, route_resolver=legacy_routes()
    )
    assert actor == "system:recovery" and facts.at == EXPIRY
    root = EvaluationStateRoot(
        p.host.tx,
        p.host.archives,
        p.host.store,
        request.intent,
        owned.plan,
        apply_expired_recovery,
        isolation_level="REPEATABLE READ",
        route_resolver=legacy_routes(),
    )
    trigger = None
    calls = p.gateway.calls
    try:
        if fault == "archive_rejected":
            trigger = "expiry_fault_" + uuid4().hex
            async with trigger_admin.begin() as db:
                await db.execute(
                    text(
                        f"CREATE TRIGGER {trigger} BEFORE INSERT ON {archive.name} FOR EACH ROW "
                        "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='isolated expiry rejection'"
                    )
                )
        with monkeypatch.context() as patch:
            if fault == "client_reply_discarded":
                original_commit = EventSession.commit

                async def discard(db):
                    await original_commit(db)
                    raise ConnectionError("client discarded completed original expiry reply")

                patch.setattr(EventSession, "commit", discard)
            outcome = await root(owned)
        committed = fault != "archive_rejected"
        assert outcome == (LocalOutcome.COMMITTED if committed else LocalOutcome.ABORTED)
        assert await StoreChecker(p.host.store).check(owned.references[0]) == (
            Decision.COMMIT if committed else Decision.ROLLBACK
        )
        current = await rows(p.tx, p.run_id)
        if committed:
            assert current[2]["version"] == checkpoint.version + 1
            assert current[2]["checkpoint_json"] is None
            progress = current[0]["progress_json"]
            assert progress["status"] == ("collecting" if mode == "prepared" else "blocked")
            assert progress["recoveries"][-1]["cause_code"] == (
                "expired_preparation_released"
                if mode == "prepared"
                else "expired_dispatch_result_unknown"
            )
        else:
            assert current == before
        async with p.tx.open() as db:
            table = semantic_completions if mode == "semantic" else generation_completions
            terminal = (
                (
                    await db.execute(
                        table.select().where(
                            table.c.run_id == str(p.run_id),
                            table.c.invocation_id == request.invocation_id,
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            if committed and mode != "prepared":
                assert terminal["evidence_json"]["status"] == "result_unknown"
                assert terminal["evidence_json"]["receipt"] is None and not terminal["raw_output"]
            else:
                assert terminal is None
        assert p.gateway.calls == calls
        assert await p.host.archives.read_committed(first.envelope().message_id) == (
            first if committed else None
        )
    finally:
        if trigger is not None:
            async with trigger_admin.begin() as db:
                await db.execute(text(f"DROP TRIGGER IF EXISTS {trigger}"))
