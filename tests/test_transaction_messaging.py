import json
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import ValidationError
from reliable_messaging.transaction import ShutdownIncompleteError
from reliable_messaging.transport import physical_topic, physical_topic_v2

from qs_ai.bootstrap.messaging import MessagingRuntime, read_rocketmq_config
from qs_ai.bootstrap.transaction_messaging import (
    TransactionAdmissionClosed,
    TransactionRuntime,
    make_transaction_sender,
)
from qs_ai.config import MessagingOptions
from qs_ai.infrastructure.workflow_transport.messaging import ACKS, COMMANDS, EVENTS


def options(tmp_path):
    credentials = tmp_path / "credentials.json"
    credentials.write_text(json.dumps({"username": "test", "password": "fixture-only"}))
    manifest = tmp_path / "routes.json"
    routes = [
        {
            "topic": topic,
            "physical_topic": physical_topic_v2(topic, "TRANSACTION", owner, "m3-1"),
            "message_type": "TRANSACTION",
            "owner": owner,
            "route_revision": "m3-1",
        }
        for topic, owner in [(COMMANDS, "qs.mysql"), (ACKS, "qs.mysql"), (EVENTS, "ai.mysql")]
    ]
    manifest.write_text(json.dumps({"version": 2, "routes": routes}))
    return {
        "provider": "rocketmq",
        "rocketmq": {
            "endpoint": "127.0.0.1:1",
            "credentials_file": str(credentials),
            "ca_file": str(tmp_path / "ca.crt"),
            "routes_file": str(manifest),
        },
        "transactions": {
            "ai.mysql": {
                "store_id": "primary-fixture",
                "writer_epoch": "m3-epoch",
                "route_revision": "m3-1",
            }
        },
    }


def test_ai_budget_and_owner_binding_are_explicit(tmp_path):
    value = MessagingOptions.model_validate(options(tmp_path))
    budget = value.transactions["ai.mysql"].limits
    assert (budget.sends, budget.check_workers, budget.check_queue) == (4, 1, 16)
    assert (budget.check_timeout, budget.rpc_timeout, budget.lease) == (2, 5, 60)
    assert MessagingOptions.model_validate_json(value.model_dump_json()) == value


@pytest.mark.parametrize("invalid", ["wrong-owner", "nsq", "revision", "limits"])
def test_transaction_binding_rejects_wrong_owner_provider_revision_and_budget(tmp_path, invalid):
    value = options(tmp_path)
    if invalid == "wrong-owner":
        value["transactions"]["qs.mysql"] = value["transactions"].pop("ai.mysql")
    elif invalid == "nsq":
        value["provider"] = "nsq"
    elif invalid == "revision":
        value["transactions"]["ai.mysql"]["route_revision"] = "unreviewed"
    else:
        value["transactions"]["ai.mysql"]["limits"] = {"sends": 0}
    with pytest.raises(ValidationError):
        MessagingOptions.model_validate(value)


def test_v2_route_fields_survive_workflow_config_loading(tmp_path):
    value = MessagingOptions.model_validate(options(tmp_path))
    config = read_rocketmq_config(value)
    assert {r.message_type for r in config.routes} == {"TRANSACTION"}
    assert {r.owner for r in config.routes} == {"qs.mysql", "ai.mysql"}


@pytest.mark.parametrize("version", [1, 2])
def test_route_manifest_version_cannot_hide_message_type(tmp_path, version):
    value = options(tmp_path)
    path = tmp_path / "routes.json"
    document = json.loads(path.read_text())
    document["version"] = version
    if version == 2:
        document["routes"] = [
            {"topic": topic, "physical_topic": physical_topic(topic)}
            for topic in [COMMANDS, ACKS, EVENTS]
        ]
    path.write_text(json.dumps(document))
    with pytest.raises(ValueError):
        read_rocketmq_config(MessagingOptions.model_validate(value))


def test_sender_constructor_owns_no_session_thread_or_network(tmp_path, monkeypatch):
    value = MessagingOptions.model_validate(options(tmp_path))
    monkeypatch.setenv("GRPC_DEFAULT_SSL_ROOTS_FILE_PATH", value.rocketmq.ca_file)
    sessions = Mock(side_effect=AssertionError("constructor must not open a session"))
    sender = make_transaction_sender(value, sessions)
    assert sender.config.owner == "ai.mysql"
    assert [r.logical_topic for r in sender.config.routes] == [EVENTS]
    assert sender.store.binding.store_id == "primary-fixture"
    assert not sender.stats()["ready"]
    sessions.assert_not_called()


async def test_pause_keeps_checker_running_and_seals_both_send_entries():
    driver = Mock()
    driver.start = AsyncMock()
    driver.reserve = AsyncMock()
    driver.execute = AsyncMock()
    driver.stats.return_value = {"ready": True}
    value = TransactionRuntime(driver, admitted=False)
    await value.start()
    with pytest.raises(TransactionAdmissionClosed):
        await value.reserve(None, None)
    with pytest.raises(TransactionAdmissionClosed):
        await value.execute(None, None)
    driver.reserve.assert_not_awaited()
    driver.execute.assert_not_awaited()
    value.set_admission(True)
    await value.reserve(None, None)
    await value.execute(None, None)
    value.set_admission(False)
    driver.stop_admission.assert_not_called()
    assert not value.stats()["ready"]


async def test_close_preserves_incomplete_result_and_wait():
    driver = Mock()
    incomplete = ShutdownIncompleteError("owned work remains", {"active_sends": 1})
    driver.close = AsyncMock(side_effect=incomplete)
    driver.wait = AsyncMock()
    driver.stats.return_value = {"ready": True}
    value = TransactionRuntime(driver, admitted=True)
    with pytest.raises(ShutdownIncompleteError) as error:
        await value.close(grace_seconds=0)
    assert error.value is incomplete
    driver.stop_admission.assert_called_once()
    with pytest.raises(TransactionAdmissionClosed):
        value.set_admission(True)
    await value.wait()
    driver.wait.assert_awaited_once()


@pytest.mark.parametrize("budget", [-1, float("nan"), float("inf")])
async def test_invalid_shutdown_budget_does_not_seal_admission(budget):
    driver = Mock()
    driver.stats.return_value = {"ready": True}
    value = TransactionRuntime(driver, admitted=True)
    with pytest.raises(ValueError, match="shutdown budget"):
        await value.close(grace_seconds=budget)
    driver.stop_admission.assert_not_called()
    driver.close.assert_not_called()
    assert value.stats()["ready"]


def test_sender_rejects_peer_owner_routes_and_wrong_ca(tmp_path, monkeypatch):
    value = options(tmp_path)
    validated = MessagingOptions.model_validate(value)
    monkeypatch.delenv("GRPC_DEFAULT_SSL_ROOTS_FILE_PATH", raising=False)
    with pytest.raises(ValueError, match="verified CA"):
        make_transaction_sender(validated, Mock())
    monkeypatch.setenv("GRPC_DEFAULT_SSL_ROOTS_FILE_PATH", validated.rocketmq.ca_file)
    manifest = tmp_path / "routes.json"
    document = json.loads(manifest.read_text())
    for route in document["routes"]:
        if route["topic"] == EVENTS:
            route["owner"] = "qs.mysql"
            route["physical_topic"] = physical_topic_v2(EVENTS, "TRANSACTION", "qs.mysql", "m3-1")
    manifest.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="event route absent"):
        make_transaction_sender(validated, Mock())


async def test_legacy_relay_assembly_cannot_silently_accept_transaction_owners(tmp_path):
    settings = Mock()
    settings.messaging = MessagingOptions.model_validate(options(tmp_path))
    with pytest.raises(ValueError, match="transaction-bound workflow assembly"):
        await MessagingRuntime.create(None, settings, b"", b"", b"")
