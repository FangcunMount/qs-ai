"""Explicit final-version Go QS <-> Python AI host profile.

Real Broker and original default consumers/roots. Synthetic read-side evidence;
no real model, public HTTP/system login, production or packet-fault claim.
"""

import asyncio
import importlib.metadata as metadata
import json
import logging
import os
import shutil
import socket
import sys
import tempfile
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import grpc
import pytest
from dishka import Provider, Scope, provide
from reliable_messaging.transaction import Decision, StoreChecker
from sqlalchemy import delete, select

from qs_ai.application.interpretation.ports import EvidenceSource
from qs_ai.bootstrap.container import create_container
from qs_ai.bootstrap.lifecycle import RuntimeState, supervise
from qs_ai.bootstrap.messaging import MessagingRuntime
from qs_ai.bootstrap.providers.evaluation import EvaluationProvider
from qs_ai.bootstrap.transaction_services import TransactionServices
from qs_ai.config import Settings
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import messaging_pb2_grpc as rpc
from qs_ai.infrastructure.persistence.mysql.database import Database
from qs_ai.infrastructure.persistence.mysql.messaging import inbox, outbox
from qs_ai.infrastructure.persistence.mysql.schema import model_calls, result_outbox, sessions
from qs_ai.infrastructure.persistence.mysql.transaction_schema import event_receipts
from tests.integration.test_delivery import certificates
from tests.integration.test_transaction_receipts import kit as kit
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import keys as keys
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.test_input_binding import bound_case

pytestmark = pytest.mark.integration
_CASES = {}


def passed(name):
    _CASES[name] = True


@pytest.fixture(scope="session", autouse=True)
def required_pair(request):
    path = os.environ.get("QS_AI_M3_BROKER_CONFIG")
    if not path:
        pytest.fail("Required isolated paired host configuration absent")
    config = json.loads(Path(path).read_text())
    assert metadata.version("fangcun-reliable-messaging") == "0.4.0a2"
    assert metadata.version("rocketmq-python-client") == "5.1.2"
    assert config["endpoint"].startswith("127.0.0.1:")
    assert config["project"].startswith("rocketmq-m2-")
    assert config["qs_sdk_module"]["Version"] == "v0.5.0-rocketmq-tx.2"
    assert not config["qs_sdk_module"].get("Replace")
    private = Path(config["private_directory"])
    assert private.is_absolute() and private.stat().st_mode & 0o077 == 0
    from rocketmq.v5.log.log_config import logger

    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()
    private_log = Path(config["client_log"])
    private_log.touch(mode=0o600)
    logger.addHandler(logging.FileHandler(private_log))
    logger.setLevel(logging.ERROR)
    try:
        yield config
    finally:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.close()
        print(
            "M3_PAIRED_HOST_RECEIPT "
            + json.dumps(
                dict(
                    cases=_CASES,
                    passed=len(_CASES),
                    failed=request.session.testsfailed,
                    skipped=len(
                        request.config.pluginmanager.get_plugin("terminalreporter").stats.get(
                            "skipped", []
                        )
                    ),
                    root="original_go_mysql_and_python_sqlalchemy",
                    sdk_go="v0.5.0-rocketmq-tx.2",
                    sdk_python="0.4.0a2",
                    client_python="5.1.2",
                    python=sys.version.split()[0],
                    system_login_not_claimed=True,
                    real_provider_not_claimed=True,
                    public_http_not_claimed=True,
                    packet_fault_not_claimed=True,
                    synthetic_evidence_source=True,
                )
            ),
            flush=True,
        )


class QSProcess:
    def __init__(self, process):
        self.process = process
        self.lock = asyncio.Lock()

    async def read(self):
        async with asyncio.timeout(40):
            line = await self.process.stdout.readline()
        assert line and len(line) < 65536, "Owned QS control response absent or excessive"
        return json.loads(line)

    async def call(self, **value):
        async with self.lock:
            self.process.stdin.write(json.dumps(value, ensure_ascii=False).encode() + b"\n")
            await self.process.stdin.drain()
            return await self.read()

    async def close(self):
        if self.process.returncode is None:
            response = await self.call(mode="close")
            assert response == {"ok": True, "closed": True}
        async with asyncio.timeout(10):
            assert await self.process.wait() == 0


def private_files(config, keys):
    directory = Path(tempfile.mkdtemp(prefix="paired-host-", dir=config["private_directory"]))
    directory.chmod(0o700)
    certificates(directory)
    files = {}
    for name, key in keys.items():
        for private in (True, False):
            path = directory / (name + (".private.json" if private else ".public.json"))
            path.write_text(key.export_private() if private else key.export_public())
            path.chmod(0o600)
            files[name + (".private" if private else ".public")] = str(path)
    for path in directory.iterdir():
        path.chmod(0o600)
    ca, ai_cert, ai_key = (
        (directory / name).read_bytes() for name in ("ca.pem", "ai.pem", "ai.key")
    )
    return directory, files, ca, ai_cert, ai_key


@pytest.fixture
async def pair(host, required_pair, monkeypatch):
    config = required_pair
    directory, files, ca, ai_cert, ai_key = await asyncio.to_thread(
        private_files, config, host.keys
    )
    # Reserve the configured AI endpoint before starting the Go process. Its gRPC
    # client is lazy; no business command is issued until both hosts are ready.
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        ai_port = reservation.getsockname()[1]
    routes = directory / "routes.json"
    routes.write_text(json.dumps(dict(version=2, routes=config["routes"])))
    routes.chmod(0o600)
    credential_paths = {}
    for role in ("qs", "ai"):
        path = directory / (role + "-credentials.json")
        path.write_text(json.dumps(config[role]))
        path.chmod(0o600)
        credential_paths[role] = str(path)
    source = Path(config["qs_source"])
    schema = "qs_m3_paired_" + uuid4().hex
    go_config = directory / "go-private.json"
    go_config.write_text(
        json.dumps(
            dict(
                Project=config["project"],
                Source=str(source),
                DSN=config["qs_dsn"],
                Schema=schema,
                TLSDirectory=str(directory),
                AIPayloadEndpoint=f"localhost:{ai_port}",
                Options=dict(
                    enabled=True,
                    provider="rocketmq",
                    signing_key_file=files["qs.sign.private"],
                    ai_recipient_key_file=files["ai.encrypt.public"],
                    decrypt_key_files={"qs.encrypt": files["qs.encrypt.private"]},
                    ai_signer_files={"ai.sign": files["ai.sign.public"]},
                    rocketmq=dict(
                        endpoint=config["endpoint"],
                        credentials_file=credential_paths["qs"],
                        ca_file=config["ca_file"],
                        routes_file=str(routes),
                        instance_id="qs-paired-original-host",
                        publish_enabled=True,
                        transactions={
                            "qs.mysql": dict(
                                store_id="qs-paired-" + uuid4().hex,
                                writer_epoch="m3-1",
                                route_revision="m3-1",
                            )
                        },
                    ),
                ),
            )
        )
    )
    go_config.chmod(0o600)
    stderr = (directory / "go-private-stderr.log").open("wb")
    process, qs, container, runtime, supervisor, server = None, None, None, None, None, None
    stop, state = asyncio.Event(), RuntimeState()
    request_ids = []
    recorder = host.tx.database.state_events
    try:
        env = dict(os.environ)
        env["rocketmq.client.logRoot"] = str(directory / "go-private-client")
        env["rocketmq.client.logLevel"] = "error"
        process = await asyncio.create_subprocess_exec(
            config["qs_binary"],
            str(go_config),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=stderr,
            env=env,
        )
        qs = QSProcess(process)
        ready = await qs.read()
        assert ready.get("ready") is True and ready["schema"] == schema
        qs_port = int(ready["payload_endpoint"].rsplit(":", 1)[1])
        settings = Settings(
            database_url=host.kit.dsn.replace("mysql://", "mysql+asyncmy://", 1),
            evaluation={"enabled": True},
            generation={"endpoint": "https://model.invalid/responses"},
            model_api_key="isolated-not-used",
            grpc={"access_address": f"localhost:{qs_port}"},
            messaging=dict(
                enabled=True,
                provider="rocketmq",
                publish_enabled=True,
                signing_key_file=files["ai.sign.private"],
                qs_recipient_key_file=files["qs.encrypt.public"],
                decrypt_key_files={"ai.encrypt": files["ai.encrypt.private"]},
                qs_signer_files={"qs.sign": files["qs.sign.public"]},
                rocketmq=dict(
                    endpoint=config["endpoint"],
                    credentials_file=credential_paths["ai"],
                    ca_file=config["ca_file"],
                    routes_file=str(routes),
                ),
                transactions={
                    "ai.mysql": dict(
                        store_id=host.store.binding.store_id,
                        writer_epoch="m3-1",
                        route_revision="m3-1",
                    )
                },
            ),
        )
        monkeypatch.setenv("GRPC_DEFAULT_SSL_ROOTS_FILE_PATH", config["ca_file"])

        class Overrides(Provider):
            @provide(scope=Scope.APP, override=True)
            def database(self) -> Database:
                return host.tx.database

            @provide(scope=Scope.APP, override=True)
            def source(self) -> EvidenceSource:
                return host.kit.source

        container = create_container(settings, EvaluationProvider(), Overrides())
        services = await container.get(TransactionServices | None)
        assert services is not None
        reserve = services.runtime.reserve

        async def track(request, candidate):
            value = await reserve(request, candidate)
            if value.acquired:
                host.executions.append(value.execution)
            return value

        monkeypatch.setattr(services.runtime, "reserve", track)
        runtime = await MessagingRuntime.create(container, settings, ca, ai_cert, ai_key)
        assert runtime.relay is None and not runtime.publishers
        server = grpc.aio.server()
        rpc.add_MessagePayloadsServicer_to_server(runtime.payloads, server)
        assert (
            server.add_secure_port(
                f"127.0.0.1:{ai_port}",
                grpc.ssl_server_credentials(
                    [(ai_key, ai_cert)], root_certificates=ca, require_client_auth=True
                ),
            )
            == ai_port
        )
        await server.start()
        supervisor = asyncio.create_task(supervise(runtime.components(state, 20), state, stop))
        async with asyncio.timeout(40):
            while not state.ready:
                if supervisor.done():
                    await supervisor
                await asyncio.sleep(0.02)
        assert len(runtime.subscribers) == 2 and runtime.publisher_ready.is_set()
        passed("actual_ai_default_receivers_and_idle_checker")
        passed("actual_go_command_and_event_runtime")
        passed("no_legacy_relay")
        yield SimpleNamespace(**locals())
    finally:
        stop.set()
        try:
            if supervisor is not None:
                await asyncio.wait_for(supervisor, 45)
            if runtime is not None:
                await runtime.close()
            if qs is not None:
                await qs.close()
            if container is not None:
                await container.close()
        finally:
            host.tx.database.state_events = recorder
            if server is not None:
                await server.stop(2)
                await asyncio.wait_for(server.wait_for_termination(), 5)
            if process is not None and process.returncode is None:
                process.kill()
                await process.wait()
            stderr.close()
            async with host.tx.open() as db:
                if request_ids:
                    await db.execute(delete(inbox).where(inbox.c.aggregate_key.in_(request_ids)))
                ids = [ref.message_id.encode() for e in host.executions for ref in e.references]
                if ids:
                    await db.execute(
                        delete(event_receipts).where(event_receipts.c.event_id.in_(ids))
                    )
                await db.commit()
            await asyncio.to_thread(shutil.rmtree, directory)
            assert not await asyncio.to_thread(directory.exists)
        passed("owned_processes_rpc_and_private_files_closed")


async def test_actual_qs_ai_original_start_receipt_state_and_ack_loop(pair):
    p = pair
    identity = str(uuid4())
    p.request_ids.append(identity)
    start = dict(
        request_id=identity,
        actor=asdict(p.host.kit.actor),
        testee_id="7",
        assessment_ids=["42"],
        goal="paired original-root acceptance",
        evidence=[asdict(bound_case()[1].items[0])],
    )
    assert await p.qs.call(mode="start", start=start) == {"ok": True}
    passed("actual_qs_service_and_command_root")
    async with asyncio.timeout(60):
        while True:
            facts = await p.qs.call(
                mode="facts", request_id=identity, command_id=identity, actor=start["actor"]
            )
            async with p.host.tx.open() as db:
                acks = (await db.execute(select(event_receipts))).mappings().all()
            if (
                facts.get("ok") is True
                and facts["operation"]["status"] == "accepted"
                and facts.get("projection", {}).get("status") == "queued"
                and len(acks) == 2
            ):
                break
            await asyncio.sleep(0.05)
    assert facts["operation"]["transport_status"] == "confirmed"
    assert facts["counts"]["ai_messaging_outbox"] == 0
    assert facts["counts"]["ai_messaging_archive"] == 3
    assert facts["counts"]["rm_tx_witness"] == 3
    assert len(facts["archives"]) == 3 and facts["seals"] == 3
    passed("actual_qs_receipt_state_and_first_ack_roots")
    assert len(p.host.executions) == 1 and len(p.host.executions[0].references) == 2
    for ref in p.host.executions[0].references:
        assert await StoreChecker(p.host.store).check(ref) == Decision.COMMIT
        assert await p.host.archives.read_committed(ref.message_id) is not None
    passed("actual_ai_admission_archive_and_witness")
    async with p.host.tx.open() as db:
        incoming = (
            (await db.execute(select(inbox).where(inbox.c.message_id == identity))).mappings().one()
        )
        assert incoming["wire_sha256"] == facts["command"]["wire_sha256"]
        assert incoming["body_sha256"] == facts["command"]["body_sha256"]
        assert not (await db.execute(select(outbox))).first()
        assert not (await db.execute(select(model_calls))).first()
        session = (await db.execute(select(sessions))).mappings().one()
        assert session["id"] == facts["projection"]["session_id"]
        assert session["request_id"] == identity
        history = (await db.execute(select(result_outbox))).mappings().one()
        assert history["mq_owned"] and history["delivered"] and history["attempts"] == 0
        assert all(row["outcome"] == pb.MessagingEventAcknowledgement.STORED for row in acks)
        archives = {row["id"]: row for row in facts["archives"]}
        for row in acks:
            ack_id = bytes(row["ack_id"]).decode()
            received = (
                (await db.execute(select(inbox).where(inbox.c.message_id == ack_id)))
                .mappings()
                .one()
            )
            assert received["wire_sha256"] == archives[ack_id]["wire_sha256"]
    passed("first_command_and_ack_wire_cross_language")
    passed("actual_ai_business_receipts_and_history")
    passed("no_new_publish_outbox_or_model_call")
