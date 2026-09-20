import pytest

from qs_ai.bootstrap.container import create_container
from qs_ai.bootstrap.evaluation import run
from qs_ai.bootstrap.providers.evaluation import EvaluationProvider
from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.evaluation_worker import EvaluationWorker


def configured(**changes):
    values = {
        "evaluation": {"enabled": True},
        "generation": {"endpoint": "https://model.invalid/responses"},
        "model_api_key": "synthetic-test-only",
        "database_url": "mysql+asyncmy://test:test@127.0.0.1:1/test",
    }
    values.update(changes)
    return Settings(**values)


@pytest.mark.parametrize("mode", [{"once": True}])
async def test_execution_disabled_before_constructing_dependencies(mode, monkeypatch):
    def forbidden(*args):
        pytest.fail("Disabled execution must not create dependencies")

    monkeypatch.setattr("qs_ai.bootstrap.evaluation.create_container", forbidden)
    with pytest.raises(ValueError, match="disabled"):
        await run(Settings(), **mode)


@pytest.mark.parametrize(
    "changes",
    [
        {"generation": {"endpoint": None}},
        {"generation": {"endpoint": "http://model.invalid/responses"}},
        {"generation": {"endpoint": "https:///responses"}},
        {"model_api_key": None},
        {"model_api_key": " "},
        {"database_url": None},
    ],
)
async def test_incomplete_configuration_rejected_before_polling(changes):
    container = create_container(configured(**changes), EvaluationProvider())
    try:
        async with container() as request:
            with pytest.raises(ValueError, match="Evaluation requires"):
                await request.get(EvaluationWorker)
    finally:
        await container.close()


async def test_request_scope_builds_worker_without_network_and_closes_client():
    container = create_container(configured(), EvaluationProvider())
    try:
        async with container() as request:
            worker = await request.get(EvaluationWorker)
            assert worker.enabled
            assert worker.owner.startswith("evaluation:")
            assert await request.get(EvaluationWorker) is worker
        assert worker.gateway._client.is_closed
        async with container() as request:
            other = await request.get(EvaluationWorker)
            assert other.owner != worker.owner
            assert other.recovery_cursor is worker.recovery_cursor
    finally:
        await container.close()


def test_evaluation_defaults_are_independent_and_disabled(monkeypatch):
    assert not Settings(environment="production").evaluation.enabled
    monkeypatch.setenv("QS_AI_EVALUATION__ENABLED", "true")
    settings = Settings()
    assert settings.evaluation.enabled
    assert not settings.generation.enabled
    assert settings.evaluation is not settings.worker


@pytest.mark.parametrize("mode", ["probe", "once"])
async def test_entrypoint_scopes_and_modes(mode, monkeypatch, capsys):
    from unittest.mock import AsyncMock

    from dishka import Provider, Scope, provide

    from qs_ai.application.operations.health import CheckReadiness, Readiness

    workers = []

    class Overrides(Provider):
        @provide(scope=Scope.REQUEST, override=True)
        def worker(self) -> EvaluationWorker:
            value = AsyncMock(spec=EvaluationWorker)
            value.once.return_value = True
            workers.append(value)
            return value

        @provide(scope=Scope.REQUEST, override=True)
        def readiness(self) -> CheckReadiness:
            value = AsyncMock(spec=CheckReadiness)
            value.execute.return_value = Readiness("connected")
            return value

    monkeypatch.setattr(
        "qs_ai.bootstrap.evaluation.create_container",
        lambda settings, *providers: create_container(settings, *providers, Overrides()),
    )

    assert await run(configured(), once=mode == "once") == 0
    expected = {"probe": 0, "once": 1, "serve": 2}[mode]
    assert sum(w.once.await_count for w in workers) == expected
    assert len(workers) == (0 if mode == "probe" else expected + 1)
    assert "synthetic-test-only" not in capsys.readouterr().out
