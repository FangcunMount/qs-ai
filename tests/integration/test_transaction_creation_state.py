"""Original creation roots on real MySQL; no half-message or packet-fault claim."""

import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from reliable_messaging.transaction import Decision, LocalOutcome, StoreChecker
from sqlalchemy import delete, select, text

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.infrastructure.persistence.mysql.database import EventSession
from qs_ai.infrastructure.persistence.mysql.evaluation_runs import (
    MySQLRunCreator,
    apply_run_creation,
)
from qs_ai.infrastructure.persistence.mysql.messaging import outbox
from qs_ai.infrastructure.persistence.mysql.transaction_schema import archive
from qs_ai.infrastructure.workflow_transport.creation_state import (
    CreationStatePlanner,
    CreationStateRoot,
)
from qs_ai.infrastructure.workflow_transport.creation_state_plan import (
    CreationStateInput,
    read_creation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_mq_storage import keys as keys
from tests.integration.test_transaction_evaluation_admission import trigger_admin as trigger_admin
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.test_generation_manifest import assets as assets
from tests.test_generation_manifest import complete_release as complete_release
from tests.test_generation_manifest import evaluation_release as evaluation_release

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("mode", ["serial_v1", "candidate_v2"])
@pytest.mark.parametrize(
    "fault",
    ["none", "archive_rejected", "witness_rejected", "client_reply_discarded", "existing_run"],
)
async def test_creation_first_definition_mode_policy_sequence_archive_and_witness_share_root(
    host, setup_run, assets, complete_release, trigger_admin, monkeypatch, mode, fault
):
    from qs_ai.application.evaluation.execution_mode import ExecutionMode

    _, run_id, _ = setup_run
    incoming = CreationStateInput(
        ManagementScope(run_id, 1, 42), complete_release, "原创建事务验证"
    )
    at = datetime(2026, 10, 21, 8, 1, 2, tzinfo=timezone(timedelta(hours=8)))
    creator = MySQLRunCreator(host.tx, *assets[0], execution_mode=ExecutionMode(mode))
    candidate, creation = await CreationStatePlanner(
        creator,
        signing_key=host.keys["ai.sign"],
        recipient_key=host.keys["qs.encrypt"],
    ).candidate(incoming, at)
    owned = (await host.store.reserve(incoming.intent.request, candidate)).execution
    host.executions.append(owned)
    _, original, first = read_creation_state_plan(incoming, owned.plan)
    assert await rows(host.tx, run_id) == [None, None, None]
    assert original.created_at.isoformat() == at.isoformat()
    trigger = None
    try:
        if fault in ("archive_rejected", "witness_rejected"):
            trigger = "creation_fault_" + uuid4().hex
            statement = (
                f"CREATE TRIGGER {trigger} BEFORE INSERT ON {archive.name} FOR EACH ROW "
                "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='isolated creation archive rejection'"
                if fault == "archive_rejected"
                else f"CREATE TRIGGER {trigger} BEFORE UPDATE ON rm_tx_witness FOR EACH ROW "
                "BEGIN IF NEW.state='committed' THEN SIGNAL SQLSTATE '45000' "
                "SET MESSAGE_TEXT='isolated creation witness rejection'; END IF; END"
            )
            async with trigger_admin.begin() as db:
                await db.execute(text(statement))
        elif fault == "existing_run":
            # A separately committed legacy fact cannot be adopted as a new SDK witness.
            async with host.tx.open() as db:
                await apply_run_creation(db, creation)
                await db.commit()
        root = CreationStateRoot(host.tx, host.archives, host.store, incoming, owned.plan, creation)
        with monkeypatch.context() as patch:
            if fault == "client_reply_discarded":
                real_commit = EventSession.commit

                async def discard(db):
                    await real_commit(db)
                    raise ConnectionError("client discarded completed creation commit reply")

                patch.setattr(EventSession, "commit", discard)
            outcome = await root(owned)
        committed = fault in ("none", "client_reply_discarded")
        assert outcome == (LocalOutcome.COMMITTED if committed else LocalOutcome.ABORTED)
        assert await StoreChecker(host.store).check(owned.references[0]) == (
            Decision.COMMIT if committed else Decision.ROLLBACK
        )
        actual = await rows(host.tx, run_id)
        if committed or fault == "existing_run":
            run, policy, checkpoint = actual
            assert run["definition_json"] == creation.definition_json
            assert run["execution_mode"] == mode
            assert json.loads(run["definition_json"])["audit"]["created_at"] == at.isoformat()
            assert policy["definition_json"] == creation.policy.definition_json
            assert checkpoint["version"] == 1 and checkpoint["checkpoint_json"] is None
        else:
            assert actual == [None, None, None]
        assert await host.archives.read_committed(first.envelope().message_id) == (
            first if committed else None
        )
        # read_view is captured inside business, before archive/witness writes.
        # That in-memory candidate is not a commit receipt. The sender must
        # require COMMITTED plus exact fresh evidence before returning it.
        if fault == "existing_run":
            assert root.view is None
        else:
            assert root.view is not None
            assert (root.view.status, root.view.version, root.view.execution_mode) == (
                "requested",
                1,
                mode,
            )
            if not committed:
                assert outcome == LocalOutcome.ABORTED and actual == [None, None, None]
        async with host.tx.open() as db:
            sequence = (
                (
                    await db.execute(
                        select(evaluation_sequences).where(
                            evaluation_sequences.c.run_id == str(run_id)
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            assert (None if sequence is None else (sequence["sequence"], sequence["version"])) == (
                (1, 1) if committed else None
            )
            assert not (
                await db.scalars(
                    select(outbox.c.message_id).where(
                        outbox.c.message_id == first.envelope().message_id
                    )
                )
            ).all()
    finally:
        if trigger is not None:
            async with trigger_admin.begin() as db:
                await db.execute(text(f"DROP TRIGGER {trigger}"))
        async with host.tx.open() as db, db.begin():
            await db.execute(
                delete(evaluation_sequences).where(evaluation_sequences.c.run_id == str(run_id))
            )
