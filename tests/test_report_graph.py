import asyncio
from types import SimpleNamespace

import pytest

from qs_ai.application.execution.generation import GeneratedExplanation
from qs_ai.application.interpretation.provider import ProviderFailure
from qs_ai.infrastructure.workflows import report
from tests.test_deepseek_request import prepared, route, schema


@pytest.mark.parametrize("unknown", [False, True])
async def test_graph_runs_stages_and_propagates_unknown_without_retry(monkeypatch, unknown):
    calls = []
    source = prepared()

    def prepare(*args):
        calls.append("prepare")
        return source

    class Generation:
        async def execute(self, claim, frozen):
            calls.append("generate")
            assert frozen.prepared == source
            if unknown:
                raise ProviderFailure("provider_timeout", result_unknown=True)
            return GeneratedExplanation(frozen, SimpleNamespace())

    def artifact(*args):
        calls.append("validate")
        return "artifact"

    monkeypatch.setattr(report, "prepare_explanation", prepare)
    monkeypatch.setattr(report, "build_artifact", artifact)
    workflow = report.ReportWorkflow(Generation(), source.release, None, route(), schema(), None)
    assert set(workflow.graph.get_graph().nodes) == {
        "__start__",
        "prepare",
        "generate",
        "validate",
        "__end__",
    }
    result = await workflow.execute(SimpleNamespace(session=None), None)
    assert calls == (["prepare", "generate"] if unknown else ["prepare", "generate", "validate"])
    assert result.failure_code == ("provider_result_unknown" if unknown else None)


async def test_graph_cancellation_does_not_continue_to_artifact(monkeypatch):
    source = prepared()
    started = asyncio.Event()
    monkeypatch.setattr(report, "prepare_explanation", lambda *args: source)

    class Generation:
        async def execute(self, *args):
            started.set()
            await asyncio.Event().wait()

    def forbidden(*args):
        raise AssertionError("Cancelled model call cannot produce an artifact")

    monkeypatch.setattr(report, "build_artifact", forbidden)
    workflow = report.ReportWorkflow(Generation(), source.release, None, route(), schema(), None)
    task = asyncio.create_task(workflow.execute(SimpleNamespace(session=None), None))
    await asyncio.wait_for(started.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_concurrent_invocations_do_not_share_graph_state(monkeypatch):
    source = prepared()
    entered = 0
    both_entered = asyncio.Event()
    monkeypatch.setattr(report, "prepare_explanation", lambda *args: source)

    class Generation:
        async def execute(self, claim, frozen):
            nonlocal entered
            entered += 1
            if entered == 2:
                both_entered.set()
            await asyncio.wait_for(both_entered.wait(), 1)
            return GeneratedExplanation(frozen, SimpleNamespace(invocation_id=claim.id))

    def artifact(claim, evidence, generated, parser):
        assert generated.response.invocation_id == claim.id
        assert evidence == claim.id
        return claim.id

    monkeypatch.setattr(report, "build_artifact", artifact)
    workflow = report.ReportWorkflow(Generation(), source.release, None, route(), schema(), None)
    results = await asyncio.gather(
        workflow.execute(SimpleNamespace(session=None, id="first"), "first"),
        workflow.execute(SimpleNamespace(session=None, id="second"), "second"),
    )
    assert [result.artifact for result in results] == ["first", "second"]
