"""Trusted maintenance CLI: original preview/apply, operator identity and shutdown boundary."""

from argparse import Namespace
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from pydantic import SecretStr

from qs_ai.application.evaluation.checkpoints import CheckpointState
from qs_ai.bootstrap import recover_semantic_contract as cli
from tests.route_fixtures import legacy_routes
from tests.test_evaluation_resolution import AT


@pytest.fixture
def original(monkeypatch):
    args = Namespace(
        run_id=uuid4(),
        organization_id=1,
        expected_version=7,
        execution_id="execution:first",
        expected_release_fingerprint="sha256:" + "a" * 64,
        reason="original explicit operator exception",
        apply=False,
        expected_output_fingerprint="sha256:" + "b" * 64,
        acknowledge_policy_exception_and_cost=True,
    )
    target = SimpleNamespace(
        execution_id=args.execution_id,
        candidate_id="candidate:first",
        candidate_output_fingerprint="sha256:" + "c" * 64,
        output_fingerprint=args.expected_output_fingerprint,
    )
    events = []

    async def commit():
        events.append("legacy_root_commit")

    async def close():
        events.append("database_close")

    db = SimpleNamespace(
        execute=AsyncMock(
            return_value=SimpleNamespace(mappings=lambda: SimpleNamespace(one=lambda: {}))
        ),
        commit=AsyncMock(side_effect=commit),
    )

    @asynccontextmanager
    async def opened():
        events.append("source_open")
        try:
            yield db
        finally:
            events.append("source_closed")

    database = SimpleNamespace(close=AsyncMock(side_effect=close), engine=object())
    tx = SimpleNamespace(open=opened)
    settings = SimpleNamespace(
        database_url=SecretStr("mysql+asyncmy://invalid/isolated_cli"),
        messaging=SimpleNamespace(transactions={}),
    )

    async def authorize(*values, **kwargs):
        events.append("legacy_authorize")
        assert (
            values[0] is db and values[-1].actor == "operator:unit" and kwargs == {"confirm": True}
        )
        return CheckpointState(args.run_id, 8, None)

    legacy = AsyncMock(side_effect=authorize)
    monkeypatch.setattr(cli, "Settings", lambda: settings)
    monkeypatch.setattr(cli, "Database", lambda url: database)
    monkeypatch.setattr(cli, "Transactions", lambda value: tx)
    monkeypatch.setattr(cli, "decode_semantic_completion", lambda row: target)
    monkeypatch.setattr(cli, "authorize_contract_recovery", legacy)
    monkeypatch.setattr(cli.pwd, "getpwuid", lambda uid: SimpleNamespace(pw_name="unit"))
    monkeypatch.setattr(cli, "datetime", SimpleNamespace(now=lambda tz: AT))
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("apply", [False, True])
async def test_original_cli_preview_rolls_back_and_apply_owns_one_root_without_fabricated_actor(
    original, apply
):
    p = original
    p.args.apply = apply
    result = await cli.run(p.args)
    assert result["mode"] == ("applied" if apply else "preview_rolled_back")
    assert result["result_version"] == 8 and result["actor"] == "operator:unit"
    assert result["existing_generation_outputs"] == "preserved"
    assert p.events == ["source_open", "legacy_authorize"] + (
        ["legacy_root_commit"] if apply else []
    ) + ["source_closed", "database_close"]
    assert p.legacy.await_count == 1 and p.db.commit.await_count == int(apply)
    p.database.close.assert_awaited_once()


@pytest.mark.parametrize("wrong", ["acknowledgement", "output"])
async def test_original_cli_apply_requires_explicit_cost_exception_and_exact_preview_fingerprint(
    original, wrong
):
    p = original
    p.args.apply = True
    if wrong == "acknowledgement":
        p.args.acknowledge_policy_exception_and_cost = False
    else:
        p.args.expected_output_fingerprint = "sha256:" + "d" * 64
    with pytest.raises(ValueError):
        await cli.run(p.args)
    p.legacy.assert_not_called()
    p.db.commit.assert_not_called()
    p.database.close.assert_awaited_once()


@pytest.mark.parametrize("mode", ["preview", "normal", "close_incomplete", "start_failure"])
async def test_transaction_cli_closes_source_before_sender_and_drains_before_primary(
    original, monkeypatch, mode
):
    from qs_ai.infrastructure.workflow_transport.contract_recovery_state import (
        ContractRecoveryInput,
    )

    p = original
    p.args.apply = mode != "preview"
    p.settings.messaging = SimpleNamespace(
        transactions={"ai.mysql": object()},
        enabled=True,
        publish_enabled=True,
        signing_key_file="signing-test-key",
        qs_recipient_key_file="recipient-test-key",
    )

    async def start():
        p.events.append("sdk_start")
        if mode == "start_failure":
            raise ConnectionError("isolated start refusal")

    async def close(*, grace_seconds):
        assert grace_seconds == 30
        p.events.append("sdk_close")
        if mode == "close_incomplete":
            raise cli.ShutdownIncompleteError("isolated in-flight work", {})

    async def wait():
        p.events.append("sdk_wait")

    runtime = SimpleNamespace(
        store=SimpleNamespace(binding=object()),
        start=AsyncMock(side_effect=start),
        close=AsyncMock(side_effect=close),
        wait=AsyncMock(side_effect=wait),
        routing=legacy_routes(),
    )
    sessions = object()

    def construct(options, factory):
        assert options is p.settings.messaging and factory is sessions
        assert p.events == ["source_open", "source_closed"]
        p.events.append("sdk_construct")
        return runtime

    factory = Mock(side_effect=construct)
    monkeypatch.setattr(cli, "async_sessionmaker", lambda engine, **kw: sessions)
    monkeypatch.setattr(cli, "make_transaction_sender", factory)
    monkeypatch.setattr(cli, "TransactionArchive", lambda *values: object())

    def key(path, *, private):
        assert path == ("signing-test-key" if private else "recipient-test-key")
        return object()

    monkeypatch.setattr(cli, "read_key", key)

    async def send(incoming):
        assert isinstance(incoming, ContractRecoveryInput)
        assert incoming.value.actor == "operator:unit" and incoming.value.resolved_at == AT
        p.events.append("sdk_send")
        return incoming.response()

    sending = AsyncMock(side_effect=send)
    monkeypatch.setattr(cli, "ContractRecoverySender", lambda *a, **kw: sending)
    if mode == "start_failure":
        with pytest.raises(ConnectionError):
            await cli.run(p.args)
        sending.assert_not_called()
    else:
        result = await cli.run(p.args)
        assert result["mode"] == ("preview_rolled_back" if mode == "preview" else "applied")
    if mode == "preview":
        factory.assert_not_called()
        sending.assert_not_called()
        runtime.start.assert_not_called()
        assert p.events == ["source_open", "legacy_authorize", "source_closed", "database_close"]
    else:
        p.legacy.assert_not_called()
        p.db.commit.assert_not_called()
        assert p.events[:4] == ["source_open", "source_closed", "sdk_construct", "sdk_start"]
        assert p.events[-2:] == (
            ["sdk_wait", "database_close"]
            if mode == "close_incomplete"
            else ["sdk_close", "database_close"]
        )
        assert sending.await_count == int(mode != "start_failure")
    p.database.close.assert_awaited_once()


@pytest.mark.parametrize("blocked", ["enabled", "publish_enabled"])
async def test_transaction_cli_never_falls_back_to_legacy_when_owner_admission_is_closed(
    original, monkeypatch, blocked
):
    p = original
    p.args.apply = True
    p.settings.messaging = SimpleNamespace(
        transactions={"ai.mysql": object()}, enabled=True, publish_enabled=True
    )
    setattr(p.settings.messaging, blocked, False)
    factory = Mock(side_effect=AssertionError("No transport construction from disabled admission"))
    monkeypatch.setattr(cli, "make_transaction_sender", factory)
    with pytest.raises(ValueError, match="admission"):
        await cli.run(p.args)
    p.legacy.assert_not_called()
    p.db.commit.assert_not_called()
    factory.assert_not_called()
    p.database.close.assert_awaited_once()
