"""Prevalidated original Artifact identity/root atomicity; no SDK/Broker/provider claim."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import select

from qs_ai.application.execution import artifact as module
from qs_ai.application.interpretation.ports import WorkflowResult
from qs_ai.infrastructure.persistence.mysql.schema import artifacts, model_calls, sessions
from tests.integration.test_artifact_acceptance import ready
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("published_configuration")]


@pytest.mark.parametrize("commit", [False, True])
async def test_first_artifact_identity_revalidates_original_content_without_generating_uuid_in_root(
    kit, monkeypatch, commit
):
    claim, artifact = await ready(kit)
    identity = module.ArtifactIdentity(claim.run_id, artifact.invocation_id, artifact.id)
    event_id, at = str(uuid4()), datetime.now(UTC)
    async with kit.transactions.open() as db:
        original_call = (
            (await db.execute(select(model_calls).where(model_calls.c.run_id == claim.run_id)))
            .mappings()
            .one()
        )

    def no_uuid(*args):
        raise AssertionError("No artifact UUID generation in borrowed root")

    with monkeypatch.context() as patch:
        patch.setattr(module, "uuid5", no_uuid)
        async with kit.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            root = db.get_transaction()
            await kit.store.finish_in(
                db,
                claim,
                WorkflowResult("", artifact=artifact),
                state_event_id=event_id,
                at=at,
                artifact_identity=identity,
            )
            assert db.get_transaction() is root and root.is_active
            if commit:
                await db.commit()
    async with kit.transactions.open() as db:
        payload = await db.scalar(
            select(artifacts.c.payload).where(artifacts.c.run_id == claim.run_id)
        )
        status = await db.scalar(select(sessions.c.status).where(sessions.c.id == claim.session.id))
        current_call = (
            (await db.execute(select(model_calls).where(model_calls.c.run_id == claim.run_id)))
            .mappings()
            .one()
        )
    assert current_call == original_call and current_call["status"] == "response_received"
    assert status == ("completed" if commit else "running")
    if commit:
        assert (
            payload["id"] == identity.artifact_id
            and payload["content_fingerprint"] == artifact.content_fingerprint
        )
    else:
        assert payload is None
