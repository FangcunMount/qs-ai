import json
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import select

from qs_ai.application.execution.artifact import build_artifact
from qs_ai.application.execution.errors import LeaseLost
from qs_ai.application.execution.generation import DurableGeneration, FrozenGeneration
from qs_ai.application.interpretation.commands import CancelCommand
from qs_ai.application.interpretation.ports import WorkflowResult
from qs_ai.application.interpretation.preparation import prepare_explanation
from qs_ai.domain.interpretation.model import RuleViolation, Status
from qs_ai.infrastructure.persistence.model_call_codec import JSONModelCallCodec
from qs_ai.infrastructure.persistence.mysql.result_outbox import MySQLResultOutbox
from qs_ai.infrastructure.persistence.mysql.schema import artifacts, sessions
from qs_ai.infrastructure.qs_server.output import QSOutputParser
from qs_ai.infrastructure.qs_server.prompts import load_prompt
from tests.integration.test_generation import Gateway
from tests.integration.test_interpretation import kit as kit
from tests.test_deepseek_request import prepared, route, schema
from tests.test_input_binding import bound_case
from tests.test_output_validation import candidate

pytestmark = pytest.mark.integration


async def ready(kit):
    _, source, _ = bound_case()
    await kit.service.start_external(kit.actor, "7", ("42",), "goal", str(uuid4()), source.items)
    claim = await kit.store.claim(60)
    evidence = await kit.store.evidence(claim)
    release = prepared().release
    package = load_prompt(release.render_policy.template_id, release.render_policy.version)
    request = FrozenGeneration(
        prepare_explanation(claim.session, evidence, release, package), route(), schema()
    )

    class ValidGateway(Gateway):
        async def generate(self, *args):
            response = await super().generate(*args)
            return replace(
                response,
                raw_output=json.dumps(candidate()),
                validation_output=json.dumps(candidate()),
            )

    generated = await DurableGeneration(kit.store, ValidGateway(), JSONModelCallCodec()).execute(
        claim, request
    )
    return claim, build_artifact(claim, evidence, generated, QSOutputParser())


async def test_artifact_and_completed_event_commit_together(kit):
    claim, artifact = await ready(kit)
    await kit.store.finish(claim, WorkflowResult("", artifact=artifact))
    async with kit.transactions.open() as db:
        payload = await db.scalar(
            select(artifacts.c.payload).where(artifacts.c.run_id == claim.run_id)
        )
        assert payload["id"] == artifact.id
        assert (
            await db.scalar(select(sessions.c.status).where(sessions.c.id == claim.session.id))
            == "completed"
        )
    events = await MySQLResultOutbox(kit.transactions).pending(20)
    completed = [event for event in events if event.status == "completed"]
    assert len(completed) == 1
    assert json.loads(completed[0].artifact_json) == payload
    with pytest.raises(LeaseLost):
        await kit.store.finish(claim, WorkflowResult("", artifact=artifact))
    view = await kit.service.get(kit.actor, claim.session.id)
    with pytest.raises(RuleViolation, match="invalid_state"):
        await kit.service.cancel(
            kit.actor,
            claim.session.id,
            CancelCommand(expected_version=view.session.version),
            str(uuid4()),
        )


async def test_outbox_failure_rolls_back_artifact_and_completion(kit, monkeypatch):
    import qs_ai.infrastructure.persistence.mysql.interpretation as persistence

    claim, artifact = await ready(kit)

    async def fail(*args):
        raise ConnectionError("Injected failure before outbox insert")

    with monkeypatch.context() as patch:
        patch.setattr(persistence, "stage_state", fail)
        with pytest.raises(ConnectionError):
            await kit.store.finish(claim, WorkflowResult("", artifact=artifact))
    async with kit.transactions.open() as db:
        assert (
            await db.scalar(select(artifacts.c.id).where(artifacts.c.run_id == claim.run_id))
            is None
        )
        assert (
            await db.scalar(select(sessions.c.status).where(sessions.c.id == claim.session.id))
            == Status.RUNNING
        )
    assert all(
        event.status != "completed"
        for event in await MySQLResultOutbox(kit.transactions).pending(20)
    )
    await kit.store.finish(claim, WorkflowResult("", artifact=artifact))


async def test_cancelled_task_cannot_accept_late_artifact(kit):
    claim, artifact = await ready(kit)
    await kit.service.cancel(
        kit.actor,
        claim.session.id,
        CancelCommand(expected_version=claim.session.version),
        str(uuid4()),
    )
    with pytest.raises(LeaseLost):
        await kit.store.finish(claim, WorkflowResult("", artifact=artifact))
    async with kit.transactions.open() as db:
        assert (
            await db.scalar(select(artifacts.c.id).where(artifacts.c.run_id == claim.run_id))
            is None
        )
