from collections.abc import AsyncIterator

import pytest
from dishka import Provider, Scope, provide
from dishka.exceptions import GraphMissingFactoryError
from fastapi.testclient import TestClient

from qs_ai.application.operations.health import CheckReadiness
from qs_ai.bootstrap.container import create_container
from qs_ai.bootstrap.worker import worker_container
from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.main import create_app


class TrackedDatabase(Provider):
    def __init__(self, events: list[str]):
        super().__init__()
        self.events = events

    @provide(scope=Scope.APP, provides=Database, override=True)
    async def database(self) -> AsyncIterator[Database]:
        self.events.append("open")
        try:
            yield Database(None)
        finally:
            self.events.append("close")


async def test_worker_scopes_and_exception_cleanup() -> None:
    events = []
    with pytest.raises(ValueError, match="task failed"):
        async with worker_container(Settings(_env_file=None), TrackedDatabase(events)) as container:
            async with container() as first:
                query1 = await first.get(CheckReadiness)
                transaction1 = await first.get(Transactions)
                database1 = await first.get(Database)
                assert (await query1.execute()).database == "not_configured"
                assert await first.get(CheckReadiness) is query1
            async with container() as second:
                assert await second.get(CheckReadiness) is not query1
                assert await second.get(Transactions) is not transaction1
                assert await second.get(Database) is database1
                raise ValueError("task failed")
    assert events == ["open", "close"]


def test_api_closes_app_resource_once() -> None:
    events = []
    with TestClient(
        create_app(Settings(_env_file=None), providers=(TrackedDatabase(events),))
    ) as c:
        c.get("/readyz")
        c.get("/readyz")
        assert events == ["open"]
    assert events == ["open", "close"]


async def test_default_container_resolves_without_database() -> None:
    container = create_container(Settings(_env_file=None, database_url=None))
    try:
        async with container() as operation:
            assert (
                await (await operation.get(CheckReadiness)).execute()
            ).database == "not_configured"
    finally:
        await container.close()


class MissingDependency:
    pass


class RequiresDependency:
    def __init__(self, dependency: MissingDependency):
        self.dependency = dependency


@pytest.mark.parametrize("with_request_dependency", [False, True])
def test_container_rejects_missing_or_shorter_lived_dependency(with_request_dependency):
    class InvalidProvider(Provider):
        consumer = provide(RequiresDependency, scope=Scope.APP)

    provider = InvalidProvider()
    if with_request_dependency:
        provider.provide(MissingDependency, scope=Scope.REQUEST)
    with pytest.raises(GraphMissingFactoryError):
        create_container(Settings(_env_file=None), provider)
