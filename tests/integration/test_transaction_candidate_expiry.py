"""Actual candidate expiry roots/MySQL; synthetic gateway, no Broker or process-exit proof."""

from datetime import timedelta
from uuid import uuid4

import pytest
from reliable_messaging.transaction import Decision, LocalOutcome, StoreChecker
from sqlalchemy import text

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.infrastructure.persistence.mysql.database import EventSession
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_plan import prepare_candidate
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import active_claims
from qs_ai.infrastructure.persistence.mysql.evaluation_step import prepare_step
from qs_ai.infrastructure.persistence.mysql.transaction_schema import archive
from qs_ai.infrastructure.workflow_transport.candidate_expiry_state import (
    CandidateExpiryPlanner,
    CandidateExpiryRequest,
    apply_candidate_expiry,
    project_candidate_expiry,
    read_candidate_expiry_plan,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_root import EvaluationStateRoot
from qs_ai.infrastructure.workflows.evaluation import execute_step
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import AT
from tests.integration.test_evaluation_step import ready as ready
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_mq_storage import keys as keys
from tests.integration.test_transaction_candidate_runtime import candidate_host as candidate_host
from tests.integration.test_transaction_evaluation_admission import trigger_admin as trigger_admin
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.integration.test_transaction_worker_dispatch import original as original

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("mode", ["prepared", "generation", "semantic"])
@pytest.mark.parametrize("other", [False, True])
@pytest.mark.parametrize("fault", ["none", "archive_rejected", "client_reply_discarded"])
async def test_expired_candidate_claim_terminal_archive_and_witness_share_original_root(
    candidate_host, trigger_admin, monkeypatch, mode, other, fault
):
    s, p = candidate_host, candidate_host.p
    version = 3
    if mode == "semantic":
        version = (
            await execute_step(
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
        ).version
    if mode == "prepared":
        async with p.tx.open() as db:
            claim = await prepare_candidate(
                db,
                p.run_id,
                1,
                "worker:original",
                str(uuid4()),
                str(uuid4()),
                AT,
                AT + timedelta(minutes=1),
                limit=3,
            )
            await db.commit()
    else:
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
        claim = prepared.claim
        assert claim is not None and claim.checkpoint.kind == mode
    if other:
        async with p.tx.open() as db:
            await prepare_candidate(
                db,
                p.run_id,
                1,
                "worker:other",
                str(uuid4()),
                str(uuid4()),
                AT + timedelta(seconds=2),
                AT + timedelta(minutes=2),
                limit=3,
            )
            await db.commit()
    at, request = claim.checkpoint.lease_expires_at, CandidateExpiryRequest(1, claim)
    before = await rows(p.tx, p.run_id)
    async with p.tx.open() as db:
        claims_before = await active_claims(db, p.run_id, lock=False)
    winner = await CandidateExpiryPlanner(
        p.host.tx,
        signing_key=p.host.keys["ai.sign"],
        recipient_key=p.host.keys["qs.encrypt"],
    ).candidate(request, at)
    reserved = await p.host.store.reserve(request.intent.request, winner)
    assert reserved.acquired
    owned = reserved.execution
    p.host.executions.append(owned)
    facts, first, archived = read_candidate_expiry_plan(request, winner)
    assert first.claim == claim and facts.source_version == before[2]["version"]
    root = EvaluationStateRoot(
        p.host.tx,
        p.host.archives,
        p.host.store,
        request.intent,
        owned.plan,
        apply_candidate_expiry,
        isolation_level="REPEATABLE READ",
    )
    trigger, calls = None, len(s.gateway.calls)
    try:
        if fault == "archive_rejected":
            trigger = "candidate_expiry_" + uuid4().hex
            async with trigger_admin.begin() as db:
                await db.execute(
                    text(
                        f"CREATE TRIGGER {trigger} BEFORE INSERT ON {archive.name} FOR EACH ROW "
                        "SIGNAL SQLSTATE '45000' "
                        "SET MESSAGE_TEXT='isolated candidate expiry rejection'"
                    )
                )
        with monkeypatch.context() as patch:
            if fault == "client_reply_discarded":
                original_commit = EventSession.commit

                async def discard(db):
                    await original_commit(db)
                    raise ConnectionError("client discarded completed candidate expiry reply")

                patch.setattr(EventSession, "commit", discard)
            outcome = await root(owned)
        committed = fault != "archive_rejected"
        assert outcome == (LocalOutcome.COMMITTED if committed else LocalOutcome.ABORTED)
        assert await StoreChecker(p.host.store).check(owned.references[0]) == (
            Decision.COMMIT if committed else Decision.ROLLBACK
        )
        after = await rows(p.tx, p.run_id)
        async with p.tx.open() as db:
            current = await active_claims(db, p.run_id, lock=False)
        if committed:
            assert current == tuple(c for c in claims_before if c != claim)
            assert after[2]["version"] == before[2]["version"] + 1
            assert after[0]["progress_json"]["status"] == (
                "collecting" if mode == "prepared" or other else "blocked"
            )
            async with p.tx.open() as db:
                with pytest.raises(CheckpointConflict):
                    await project_candidate_expiry(db, request, at, lock=False)
        else:
            assert after == before and current == claims_before
        assert len(s.gateway.calls) == calls
        assert await p.host.archives.read_committed(archived.envelope().message_id) == (
            archived if committed else None
        )
    finally:
        if trigger is not None:
            async with trigger_admin.begin() as db:
                await db.execute(text(f"DROP TRIGGER IF EXISTS {trigger}"))
