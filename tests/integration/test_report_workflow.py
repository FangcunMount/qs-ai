import json
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import select

from qs_ai.application.execution.generation import DurableGeneration
from qs_ai.application.execution.report_workflow import ReportWorkflow
from qs_ai.application.execution.worker import ExecuteNext
from qs_ai.application.interpretation.provider import ProviderFailure
from qs_ai.infrastructure.persistence.model_call_codec import JSONModelCallCodec
from qs_ai.infrastructure.persistence.mysql.result_outbox import MySQLResultOutbox
from qs_ai.infrastructure.persistence.mysql.schema import artifacts
from qs_ai.infrastructure.qs_server.output import QSOutputParser
from qs_ai.infrastructure.qs_server.prompts import load_prompt
from tests.integration.test_generation import Gateway
from tests.integration.test_interpretation import kit as kit
from tests.test_deepseek_request import prepared, route, schema
from tests.test_input_binding import bound_case
from tests.test_output_validation import candidate

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "scenario,expected",
    [
        ("success", None),
        ("schema", "output_schema_invalid"),
        ("safety", "forbidden_deterministic_future_prediction"),
        ("before", "access_revoked"),
        ("after", "access_revoked"),
        ("timeout", "provider_result_unknown"),
    ],
)
async def test_worker_to_validated_artifact_or_visible_failure(kit, scenario, expected):
    _, evidence, _ = bound_case()
    receipt = await kit.service.start_external(
        kit.actor, "7", ("42",), "goal", str(uuid4()), evidence.items
    )
    if scenario == "before":
        kit.source.revoked = True

    class Model(Gateway):
        async def generate(self, *args):
            response = await super().generate(*args)
            if scenario == "after":
                kit.source.revoked = True
            if scenario == "timeout":
                raise ProviderFailure("provider_timeout", result_unknown=True)
            content = candidate()
            if scenario == "schema":
                del content["summary"]
            elif scenario == "safety":
                content["summary"] = "你一定会遇到困难。"
            raw = json.dumps(content)
            return replace(response, raw_output=raw, validation_output=raw)

    model = Model()
    release = prepared().release
    workflow = ReportWorkflow(
        DurableGeneration(kit.store, model, JSONModelCallCodec()),
        release,
        load_prompt(release.render_policy.template_id, release.render_policy.version),
        route(),
        schema(),
        QSOutputParser(),
    )
    assert await ExecuteNext(kit.store, kit.source, workflow).once()
    view = await kit.service.get(kit.actor, receipt.session_id)
    assert view.session.failure_code == expected
    assert view.session.status == ("completed" if expected is None else "blocked")
    assert model.calls == (0 if scenario == "before" else 1)
    async with kit.transactions.open() as db:
        artifact = await db.scalar(
            select(artifacts.c.payload).where(artifacts.c.session_id == receipt.session_id)
        )
    events = await MySQLResultOutbox(kit.transactions).pending(20)
    assert any(event.status == "completed" for event in events) == (expected is None)
    if expected is None:
        assert json.loads(artifact["content_json"]) == candidate()
    else:
        assert artifact is None
