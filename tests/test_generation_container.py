import pytest

from qs_ai.application.execution.configuration import PublishedReportWorkflow
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
            assert isinstance(workflow, PublishedReportWorkflow)
            assert not hasattr(workflow, "legacy")
    finally:
        await container.close()


def test_model_key_cannot_enter_yaml(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("model_api_key: synthetic-test-only\n")
    with pytest.raises(ValueError, match="Reserved configuration key"):
        read_yaml(path)


@pytest.mark.parametrize(
    "field",
    ["use_publications", "profile_id", "route", "model", "revision", "timeout_milliseconds"],
)
def test_fixed_generation_configuration_is_rejected(field):
    with pytest.raises(ValueError, match="Extra inputs"):
        Settings(generation={field: False if field == "use_publications" else "legacy"})


async def test_legacy_workflow_cannot_reach_reader_or_model():
    from types import SimpleNamespace

    from qs_ai.application.execution.configuration import PublishedReportWorkflow

    class Forbidden:
        async def get(self, *args):
            pytest.fail("legacy workflow must fail before resolving any configuration")

        async def generate(self, *args):
            pytest.fail("legacy workflow must never call the model")

    workflow = PublishedReportWorkflow(Forbidden(), Forbidden())
    result = await workflow.execute(
        SimpleNamespace(session=SimpleNamespace(workflow_version="qs-snapshot-v1")), None
    )
    assert result.failure_code == "configuration_invalid"


async def test_external_admission_requires_report_snapshot_before_persistence():
    from uuid import uuid4

    from qs_ai.application.interpretation.service import InterpretationService
    from qs_ai.domain.interpretation.model import Actor, RuleViolation

    class Forbidden:
        def __call__(self, *args):
            pytest.fail("missing snapshot must fail before entering the transaction")

    with pytest.raises(RuleViolation, match="published_configuration_requires_snapshot"):
        await InterpretationService(Forbidden(), Forbidden()).start_external(
            Actor("1", "2"), "7", ("42",), "goal", str(uuid4())
        )
