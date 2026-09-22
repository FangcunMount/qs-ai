"""Editor parameters must affect execution without changing source fingerprints."""

import json
from dataclasses import replace
from uuid import uuid4

import pytest

from qs_ai.application.governance.solution_models import edited_route, selection
from qs_ai.application.governance.solutions import CreateSolution, ModelSelection, PrepareSolution
from qs_ai.application.interpretation.route_assets import executable_route
from qs_ai.bootstrap.import_routes import baseline_assets
from qs_ai.transport.grpc.solution_input import parse_command


def test_editor_only_exposes_supported_parameters_and_preserves_source():
    _, assets = baseline_assets()
    source = executable_route(assets[0])
    fingerprint = source.fingerprint()
    params = replace(selection(source), max_output_tokens=4000, reasoning_effort="low")
    target = edited_route(source, params, "new-editor-version", (source.model,))
    assert target.max_output_tokens == 4000 and target.reasoning_effort == "low"
    assert target.fingerprint() != fingerprint and source.fingerprint() == fingerprint
    assert json.loads(target.definition_json())["max_output_tokens"] == 4000


@pytest.mark.parametrize(
    "extra",
    [
        {"temperature": 0.5},
        {"endpoint": "https://other"},
        {"max_output_tokens": True},
        {"max_output_tokens": 12001},
    ],
)
def test_unsupported_editor_fields_and_limits_rejected(extra):
    values = dict(
        model="deepseek-v4-pro",
        max_output_tokens=8000,
        timeout_milliseconds=10000,
        reasoning_effort="low",
    )
    with pytest.raises((TypeError, ValueError)):
        ModelSelection(**{**values, **extra})


def test_source_and_command_id_required():
    with pytest.raises((TypeError, ValueError)):
        parse_command(
            json.dumps({"command_id": str(uuid4()), "title": "修改", "reason": "改进"}),
            CreateSolution,
        )
    with pytest.raises((TypeError, ValueError)):
        parse_command(
            json.dumps({"command_id": str(uuid4()), "reason": "改进", "expected_revision": True}),
            PrepareSolution,
        )


def test_model_not_enabled_cannot_be_frozen():
    _, assets = baseline_assets()
    source = executable_route(assets[0])
    with pytest.raises(ValueError, match="not enabled"):
        edited_route(source, selection(source), "new", ())


def test_template_source_is_exact_and_exclusive():
    from dataclasses import asdict

    from qs_ai.infrastructure.qs_server.evaluation_suite import MBTI_ROOT

    body = dict(command_id=str(uuid4()), title="MBTI 首版", reason="验证模板起点")
    command = parse_command(json.dumps({**body, "template_ref": asdict(MBTI_ROOT)}), CreateSolution)
    assert command.template_ref == MBTI_ROOT
    for extra in ({"publication_id": str(uuid4())}, {"source_run_id": str(uuid4())}):
        with pytest.raises(ValueError, match="Exactly one"):
            parse_command(
                json.dumps({**body, "template_ref": asdict(MBTI_ROOT), **extra}), CreateSolution
            )
    with pytest.raises((TypeError, ValueError)):
        parse_command(json.dumps({**body, "template_ref": {"id": "latest"}}), CreateSolution)
