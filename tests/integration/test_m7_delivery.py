"""MQ-only result contract: original artifact, first wire and no repeat model dispatch."""

import json
from dataclasses import asdict

import pytest

from qs_ai.application.interpretation.ports import WorkflowResult
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.persistence.mysql.messaging import outbox
from qs_ai.infrastructure.persistence.mysql.result_outbox import stage_state
from qs_ai.infrastructure.persistence.mysql.schema import model_calls, result_outbox
from tests.integration.test_artifact_acceptance import ready
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_mq_admission import mq_env as mq_env
from tests.integration.test_mq_admission import saved
from tests.integration.test_mq_storage import keys as keys
from tests.probes.session_inspection import read_session

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("published_configuration")]


async def test_completed_artifact_freezes_original_event_and_first_mq_wire(kit, keys, mq_env):
    claim, artifact = await ready(kit)
    await kit.store.finish(claim, WorkflowResult("", artifact=artifact))
    tx = kit.transactions
    source = next(
        row
        for row in await saved(tx, result_outbox)
        if row["session_id"] == claim.session.id and row["payload"]["status"] == "completed"
    )
    message = next(
        row for row in await saved(tx, outbox) if row["message_id"] == source["event_id"]
    )
    event = pb.MessagingBody.FromString(message["body"]).interpretation_state
    assert event.event_id == source["event_id"]
    assert event.request_id == source["payload"]["request_id"]
    assert event.session_id == claim.session.id
    assert source["mq_owned"] and not source["delivered"]
    assert json.loads(event.artifact_json) == asdict(artifact)
    first, calls = await saved(tx, outbox), await saved(tx, model_calls)
    session = (await read_session(kit.service.uows, claim.session.id)).session
    async with tx.open() as db:
        await db.begin()
        await stage_state(db, session)
        await db.commit()
    assert await saved(tx, outbox) == first
    assert await saved(tx, model_calls) == calls
    assert (
        next(row for row in await saved(tx, result_outbox) if row["event_id"] == event.event_id)
        == source
    )
