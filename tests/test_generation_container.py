import pytest

from qs_ai.application.execution.report_workflow import ReportWorkflow
from qs_ai.application.interpretation.ports import Workflow
from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings, read_yaml
from qs_ai.infrastructure.interpretation.unconfigured import UnconfiguredWorkflow


async def test_generation_is_disabled_by_default():
    container = create_container(Settings())
    try:
        async with container() as request:
            assert isinstance(await request.get(Workflow), UnconfiguredWorkflow)
    finally:
        await container.close()


@pytest.mark.parametrize("missing", ["endpoint", "credential", "authorization", "http"])
async def test_enabled_generation_requires_complete_configuration(missing):
    values = {
        "generation": {"enabled": True, "endpoint": "https://model.invalid/responses"},
        "model_api_key": "synthetic-test-only",
        "grpc": {"access_address": "qs.invalid:9090"},
    }
    if missing == "endpoint":
        values["generation"]["endpoint"] = None
    elif missing == "credential":
        values["model_api_key"] = None
    elif missing == "authorization":
        values["grpc"]["access_address"] = None
    else:
        values["generation"]["endpoint"] = "http://model.invalid/responses"
    container = create_container(Settings(**values))
    try:
        async with container() as request:
            with pytest.raises(ValueError, match="Generation requires"):
                await request.get(Workflow)
    finally:
        await container.close()


async def test_container_assembles_published_report_workflow_without_network():
    container = create_container(
        Settings(
            generation={"enabled": True, "endpoint": "https://model.invalid/responses"},
            model_api_key="synthetic-test-only",
            grpc={"access_address": "qs.invalid:9090"},
        )
    )
    try:
        async with container() as request:
            workflow = await request.get(Workflow)
            assert isinstance(workflow, ReportWorkflow)
            assert workflow.route.model == "deepseek-v4-pro"
            assert workflow.release.input_policy.profile_version == "v6"
    finally:
        await container.close()


def test_model_key_cannot_enter_yaml(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("model_api_key: synthetic-test-only\n")
    with pytest.raises(ValueError, match="Reserved configuration key"):
        read_yaml(path)
