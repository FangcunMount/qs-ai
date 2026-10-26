"""Owned lifecycle checks with fake transport; no Broker or model network calls."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call

import pytest
from jwcrypto import jwk
from pydantic import ValidationError
from sqlalchemy.exc import ProgrammingError

from qs_ai.bootstrap import messaging
from qs_ai.bootstrap.lifecycle import RuntimeState, supervise
from qs_ai.bootstrap.messaging import MessagingRuntime, read_key
from qs_ai.bootstrap.transaction_services import TransactionServices
from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql import database as database_module
from qs_ai.infrastructure.persistence.mysql.database import Database
from qs_ai.infrastructure.persistence.mysql.messaging_observations import KINDS


def test_messaging_disabled_by_default_and_enabled_requires_explicit_secrets():
    assert not Settings().messaging.enabled
    with pytest.raises(ValidationError):
        Settings(messaging={"enabled": True})


def test_keys_require_private_public_roles_id_and_bounded_file(tmp_path):
    key = jwk.JWK.generate(kty="EC", crv="P-256", kid="test.key")
    path = tmp_path / "key.json"
    path.write_text(key.export_private())
    assert read_key(str(path), private=True, expected_id="test.key").has_private
    for private, kid in [(False, "test.key"), (True, "wrong")]:
        with pytest.raises(ValueError, match="Invalid configured messaging key"):
            read_key(str(path), private=private, expected_id=kid)
    path.write_text(key.export_public())
    assert not read_key(str(path), private=False).has_private
    path.write_text("x" * 65537)
    with pytest.raises(ValueError, match="Invalid configured messaging key"):
        read_key(str(path), private=True)


class Transport:
    def __init__(self, name, events, fail=False):
        self.name, self.events, self.fail = name, events, fail
        self.closed = asyncio.Event()

    async def start(self):
        self.events.append(self.name + ":start")
        if self.fail:
            raise RuntimeError("startup failure")

    async def wait(self):
        await self.closed.wait()

    async def stop(self, **kwargs):
        self.events.append(self.name + ":stop")
        self.closed.set()


@pytest.mark.parametrize("fail", [False, True])
async def test_owner_starts_before_subscribers_and_closes_after_their_drain(fail):
    events = []

    class Owner(Transport):
        async def close(self, **kwargs):
            await self.stop(**kwargs)

    runtime = MessagingRuntime()
    runtime.transaction_services = SimpleNamespace(runtime=Owner("owner", events, fail=fail))
    runtime.subscribers = [Transport("commands", events), Transport("acks", events)]
    stop, state = asyncio.Event(), RuntimeState()
    task = asyncio.create_task(supervise(runtime.components(state, 0.1), state, stop))
    if fail:
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(task, 2)
        assert not state.ready and state.failed
        assert "commands:start" not in events and "acks:start" not in events
    else:
        async with asyncio.timeout(2):
            while not state.ready:  # noqa: ASYNC110 - observe supervisor readiness
                await asyncio.sleep(0.01)
        stop.set()
        await asyncio.wait_for(task, 2)
        assert events[0] == "owner:start"
        assert events.index("owner:stop") > events.index("commands:stop")
        assert events.index("owner:stop") > events.index("acks:stop")
        assert not state.ready
    await runtime.close()


async def test_partial_assembly_failure_closes_only_owned_resources():
    runtime = MessagingRuntime()
    runtime.subscribers = [AsyncMock()]
    resource = AsyncMock()
    runtime.resources.push_async_callback(resource)
    await runtime.close()
    runtime.subscribers[0].stop.assert_awaited_once_with(grace_seconds=0)
    resource.assert_awaited_once_with()


@pytest.mark.parametrize("schema", ["missing", "incomplete", "complete", "canceled"])
async def test_recording_preflight_precedes_transport_and_borrows_shared_pool(monkeypatch, schema):
    # Exercise the real Transactions lifecycle and schema reader without a database/network.
    # The peripheral integration suite separately covers the actual MySQL schema and rows.
    database = Database(None)
    database.engine = AsyncMock()
    session = AsyncMock()
    session.info = {}
    result = MagicMock()
    kinds = KINDS[:-1] if schema == "incomplete" else KINDS
    result.mappings.return_value.all.return_value = [
        {"kind": kind, "recorded_count": 0, "since_epoch_seconds": 1} for kind in kinds
    ]

    async def execute(statement):
        if str(statement) == "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY":
            return None
        if schema == "missing":
            raise ProgrammingError("unredacted SQL", {}, Exception("unredacted database error"))
        if schema == "canceled":
            raise asyncio.CancelledError
        return result

    session.execute.side_effect = execute
    factory = MagicMock()
    factory.return_value.__aenter__.return_value = session
    sessionmaker = MagicMock(return_value=factory)
    monkeypatch.setattr(database_module, "async_sessionmaker", sessionmaker)
    container = MagicMock()
    services = SimpleNamespace(
        signing_key=object(),
        recipient_key=object(),
        archives=object(),
        database_seconds=2,
        runtime=AsyncMock(),
    )
    container.get = AsyncMock(side_effect=[database, services])
    scope = container.return_value.__aenter__.return_value
    scope.get = AsyncMock(return_value=object())
    monkeypatch.setattr(messaging, "read_key", lambda *args, **kwargs: object())
    for name in (
        "TrustedSigner",
        "WorkflowCommandAdmission",
        "PayloadResolver",
        "CommandReceiver",
        "TransactionCommandReceiver",
        "MessagePayloads",
    ):
        monkeypatch.setattr(messaging, name, MagicMock())

    channel_closed = []

    @asynccontextmanager
    async def channel(*args):
        try:
            yield MagicMock()
        finally:
            channel_closed.append(True)

    channel_factory = MagicMock(side_effect=channel)
    monkeypatch.setattr(messaging, "mtls_channel", channel_factory)

    def transport():
        # No transport may be constructed before the preflight read has been rolled back.
        session.rollback.assert_awaited_once()
        factory.return_value.__aexit__.assert_awaited_once()
        return AsyncMock()

    subscriber = MagicMock(side_effect=lambda *args, **kwargs: transport())
    monkeypatch.setattr(messaging, "RocketMQSubscriber", subscriber)
    monkeypatch.setattr(messaging, "read_rocketmq_config", MagicMock(return_value=object()))
    monkeypatch.setenv("GRPC_DEFAULT_SSL_ROOTS_FILE_PATH", "/unused/ca.crt")
    settings = Settings(
        grpc={"access_address": "qs.invalid:9090"},
        messaging={
            "enabled": True,
            "provider": "rocketmq",
            "rocketmq": {
                "endpoint": "127.0.0.1:1",
                "credentials_file": "/unused/credentials.json",
                "ca_file": "/unused/ca.crt",
                "routes_file": "/unused/routes.json",
            },
            "transactions": {
                "ai.mysql": {
                    "store_id": "fixture-primary",
                    "writer_epoch": "m3-1",
                    "route_revision": "m3-1",
                }
            },
            "signing_key_file": "/unused/sign.jwk",
            "decrypt_key_files": {"ai.encrypt": "/unused/decrypt.jwk"},
            "qs_signer_files": {"qs.sign": "/unused/qs-sign.jwk"},
            "qs_recipient_key_file": "/unused/qs-encrypt.jwk",
        },
    )
    if schema == "complete":
        runtime = await MessagingRuntime.create(container, settings, b"", b"", b"")
        assert subscriber.call_count == 2
        for client in runtime.subscribers:
            client.start.assert_not_awaited()
        assert database.state_events is runtime.recorder
        await runtime.close()
        assert channel_closed == [True]
    else:
        error = asyncio.CancelledError if schema == "canceled" else RuntimeError
        with pytest.raises(error) as failure:
            await MessagingRuntime.create(container, settings, b"", b"", b"")
        if schema != "canceled":
            assert str(failure.value) == "MQ required technical observation schema unavailable"
        subscriber.assert_not_called()
        channel_factory.assert_not_called()
    container.get.assert_has_awaits([call(Database), call(TransactionServices | None)])
    assert container.get.await_count == 2
    sessionmaker.assert_called_once()
    assert sessionmaker.call_args.args == (database.engine,)
    session.begin.assert_not_awaited()
    session.connection.assert_awaited_once_with(
        execution_options={"isolation_level": "REPEATABLE READ"}
    )
    assert str(session.execute.await_args_list[0].args[0]) == (
        "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"
    )
    session.commit.assert_not_awaited()
    session.rollback.assert_awaited_once()
    factory.return_value.__aexit__.assert_awaited_once()
    assert database.state_events is None
    database.engine.dispose.assert_not_awaited()


async def test_disabled_messaging_never_checks_recording_schema(monkeypatch):
    require = AsyncMock()
    monkeypatch.setattr(messaging, "require_recording_schema", require)
    container = MagicMock()
    container.get = AsyncMock()
    with pytest.raises(ValueError, match="MQ runtime is required"):
        await MessagingRuntime.create(container, Settings(), b"", b"", b"")
    require.assert_not_awaited()
    container.get.assert_not_awaited()
