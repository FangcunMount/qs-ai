"""Real HTTP and mTLS listeners with isolated use-case doubles, no model calls."""

import asyncio
import socket
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from dishka import Provider, Scope, provide

from qs_ai.application.execution.worker import ExecuteNext
from qs_ai.application.integration.events import DeliverResults
from qs_ai.application.operations.health import CheckReadiness, Readiness
from qs_ai.bootstrap import server
from qs_ai.bootstrap.container import create_container
from qs_ai.bootstrap.grpc_probe import probe
from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.database import Database
from qs_ai.infrastructure.persistence.mysql.evaluation_worker import EvaluationWorker
from tests.integration.test_delivery import certificates


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.mark.parametrize(
    "generation,evaluation", [(False, False), (True, False), (False, True), (True, True)]
)
async def test_shared_container_real_listeners_and_independent_loops(
    tmp_path, monkeypatch, generation, evaluation
):
    certificates(tmp_path)
    http_port, grpc_port = port(), port()
    counts = dict(generation=0, evaluation=0, delivery=0)
    instances = []
    pool_events = []

    def work(name, spec):
        async def once(*args):
            counts[name] += 1
            await asyncio.sleep(0.01)
            return False

        value = AsyncMock(spec=spec)
        value.once.side_effect = once
        instances.append(value)
        return value

    class Overrides(Provider):
        @provide(scope=Scope.APP, provides=Database, override=True)
        async def database(self) -> AsyncIterator[Database]:
            pool_events.append("open")
            try:
                yield Database(None)
            finally:
                pool_events.append("close")

        @provide(scope=Scope.REQUEST, override=True)
        def worker(self) -> ExecuteNext:
            return work("generation", ExecuteNext)

        @provide(scope=Scope.REQUEST, override=True)
        def evaluation_worker(self) -> EvaluationWorker:
            return work("evaluation", EvaluationWorker)

        @provide(scope=Scope.REQUEST, override=True)
        def deliver(self) -> DeliverResults:
            return work("delivery", DeliverResults)

        @provide(scope=Scope.REQUEST, override=True)
        def readiness(self) -> CheckReadiness:
            value = AsyncMock(spec=CheckReadiness)
            value.execute.return_value = Readiness("connected")
            return value

    containers = []

    def container(settings, *providers):
        value = create_container(settings, *providers, Overrides())
        containers.append(value)
        return value

    monkeypatch.setattr(server, "create_container", container)

    async def preflight(container, settings):
        first = await container.get(Database)
        assert await container.get(Database) is first

    monkeypatch.setattr(server, "preflight", preflight)
    settings = Settings(
        http={"port": http_port},
        generation={"enabled": generation},
        evaluation={"enabled": evaluation, "idle_seconds": 0.01},
        worker={"idle_seconds": 0.01},
        delivery={"idle_seconds": 0.01},
        grpc={
            "bind_address": f"localhost:{grpc_port}",
            "ca_file": str(tmp_path / "ca.pem"),
            "cert_file": str(tmp_path / "ai.pem"),
            "key_file": str(tmp_path / "ai.key"),
        },
    )
    stop = asyncio.Event()
    task = asyncio.create_task(server.serve(settings, stop))
    try:
        async with httpx.AsyncClient(trust_env=False) as client:
            async with asyncio.timeout(5):
                while True:
                    if task.done():
                        await task
                    try:
                        response = await client.get(f"http://127.0.0.1:{http_port}/readyz")
                        if response.status_code == 200:
                            break
                    except httpx.ConnectError:
                        pass
                    await asyncio.sleep(0.01)
            assert (await client.get(f"http://127.0.0.1:{http_port}/healthz")).status_code == 200
            await probe(
                f"localhost:{grpc_port}",
                (tmp_path / "ca.pem").read_bytes(),
                (tmp_path / "ai.pem").read_bytes(),
                (tmp_path / "ai.key").read_bytes(),
                "PERMISSION_DENIED",
            )
            assert len(containers) == 1
            assert bool(counts["generation"]) == generation
            assert bool(counts["evaluation"]) == evaluation
            assert counts["delivery"] > 0
    finally:
        stop.set()
        await asyncio.wait_for(task, 5)
    assert pool_events == ["open", "close"]


@pytest.mark.parametrize("listener", ["http", "grpc"])
async def test_occupied_port_stops_other_components_and_closes_container(
    tmp_path, monkeypatch, listener
):
    certificates(tmp_path)
    with socket.socket() as occupied:
        occupied.bind(("127.0.0.1", 0))
        occupied.listen()
        used = occupied.getsockname()[1]
        settings = Settings(
            http={"port": used if listener == "http" else port()},
            grpc={
                "bind_address": f"127.0.0.1:{used if listener == 'grpc' else port()}",
                "ca_file": str(tmp_path / "ca.pem"),
                "cert_file": str(tmp_path / "ai.pem"),
                "key_file": str(tmp_path / "ai.key"),
            },
            delivery={"shutdown_seconds": 0.1},
        )
        container = MagicMock()
        container.close = AsyncMock()
        receiver = AsyncMock()
        receiver.once.return_value = 0
        container.return_value.__aenter__.return_value.get = AsyncMock(return_value=receiver)
        monkeypatch.setattr(server, "create_container", lambda *args: container)
        monkeypatch.setattr(server, "preflight", AsyncMock())
        monkeypatch.setattr(server, "prune", AsyncMock(return_value=0))
        # No application DI work is needed: this test reaches an actual socket bind failure.
        from fastapi import FastAPI

        monkeypatch.setattr(server, "create_app", lambda *args, **kwargs: FastAPI())
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(server.serve(settings, asyncio.Event()), 5)
        container.close.assert_awaited_once()


async def test_invalid_certificate_fails_before_creating_dependencies(tmp_path, monkeypatch):
    bad = tmp_path / "bad.pem"
    bad.write_text("not a certificate")
    monkeypatch.setattr(
        server, "create_container", lambda *args: pytest.fail("must not create resources")
    )
    settings = Settings(grpc={"ca_file": str(bad), "cert_file": str(bad), "key_file": str(bad)})
    import ssl

    with pytest.raises(ssl.SSLError):
        await server.serve(settings, asyncio.Event())
