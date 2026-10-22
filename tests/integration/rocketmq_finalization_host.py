"""Explicit T18-C profile: separate worker/provider processes and real SDK/MySQL/Broker.

Synthetic provider, controlled QS admission and observer; not paired QS/system or
packet-loss/DB-commit network fault acceptance. Ordinary collection excludes it.
"""

import asyncio
import hashlib
import json
import os
import shutil
import sys
from dataclasses import asdict
from datetime import UTC
from uuid import uuid4

import pytest
from cryptography.hazmat.primitives.serialization import Encoding, NoEncryption, PrivateFormat
from reliable_messaging.protected import TrustedSigner
from reliable_messaging.rocketmq import Config, RocketMQSubscriber
from reliable_messaging.transaction import Decision, Execution, StoreChecker, decode_plan
from reliable_messaging.transport import RouteBinding, consumer_group_v2, physical_topic_v2
from sqlalchemy import select

from qs_ai.infrastructure.persistence.mysql.messaging import outbox
from qs_ai.infrastructure.persistence.mysql.schema import artifacts, jobs, model_calls, sessions
from qs_ai.infrastructure.persistence.mysql.transaction_schema import execution
from qs_ai.infrastructure.workflow_transport.messaging import EVENTS, authenticate, parse_body
from tests.integration.rocketmq_admission_host import required_broker as required_broker
from tests.integration.test_finalization_characterization import kit as kit
from tests.integration.test_interpretation import expire
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import keys as keys
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.probes.finalization_provider import ProviderProbe
from tests.probes.finalization_proxy import RPCProxy
from tests.test_input_binding import bound_case
from tests.test_output_validation import candidate

pytestmark = pytest.mark.integration
_CASES = {}
_OBSERVER = "qs-m3-ai-state-observer"


def passed(name):
    _CASES[name] = True


@pytest.fixture(scope="session", autouse=True)
def finalization_receipt(required_broker, request):
    yield
    print(
        "M3_AI_FINALIZATION_RECEIPT "
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
                root="original_mysql_sqlalchemy",
                sdk="0.4.0a2",
                client="5.1.2",
                python=sys.version.split()[0],
                system_login_not_claimed=True,
                paired_qs_business_not_claimed=True,
                real_external_provider_not_claimed=True,
                packet_or_database_network_fault_not_claimed=True,
                real_http_counter_verified=_CASES.get("external_http_counter", False),
                process_sigkill_verified=_CASES.get("sigkill_after_committed_response", False),
            )
        ),
        flush=True,
    )


def inputs(host, broker, probe, directory, *, pause):
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory.chmod(0o700)
    for name, value in (("sign", host.keys["ai.sign"]), ("recipient", host.keys["qs.encrypt"])):
        path = directory / (name + ".pem")
        path.write_bytes(value.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()))
        path.chmod(0o600)
    config = dict(
        database_url=host.kit.dsn.replace("mysql://", "mysql+asyncmy://", 1),
        binding=asdict(host.store.binding),
        broker=dict(endpoint=broker["endpoint"], ai=broker["ai"]),
        provider_endpoint=probe.endpoint,
        signing_key=str(directory / "sign.pem"),
        recipient_key=str(directory / "recipient.pem"),
        physical_topic=physical_topic_v2(EVENTS, "TRANSACTION", "ai.mysql", "m3-1"),
        pause_after_response=pause,
        progress=str(directory / "progress.json"),
        client_log=str(directory / "driver-private.log"),
    )
    path = directory / "worker-private.json"
    path.write_text(json.dumps(config))
    path.chmod(0o600)
    return path, directory / "progress.json"


def read_progress(path):
    return json.loads(path.read_text()) if path.exists() else None


async def await_phase(process, path, phase):
    async with asyncio.timeout(60):
        while True:
            current = await asyncio.to_thread(read_progress, path)
            if current and current["phase"] == phase:
                assert current["pid"] == process.pid
                return current
            if current and current["phase"] == "error":
                pytest.fail("worker failed: " + current["error_class"])
            assert process.returncode is None, "Worker exited before required phase"
            await asyncio.sleep(0.02)


async def executions(host):
    async with host.tx.open() as db:
        rows = (
            (await db.execute(select(execution).where(execution.c.owner == b"ai.mysql")))
            .mappings()
            .all()
        )
    return [
        Execution(
            decode_plan(bytes(row["plan"])),
            row["fence"],
            bytes(row["writer_epoch"]).decode(),
            bytes(32),
            row["lease_until"].replace(tzinfo=UTC),
        )
        for row in rows
    ]


@pytest.fixture
async def counting_provider(tmp_path):
    root = tmp_path / "t18-private"
    probe = ProviderProbe(root / "provider", candidate())
    try:
        await asyncio.to_thread(probe.start)
        yield probe
    finally:
        await asyncio.to_thread(probe.close)
        await asyncio.to_thread(shutil.rmtree, root)
        assert probe.process is None and not await asyncio.to_thread(root.exists)


@pytest.fixture
async def routing_proxy(required_broker):
    proxy = RPCProxy(required_broker)
    try:
        await proxy.start()
        yield proxy
    finally:
        await proxy.close()
        assert proxy.process is None or proxy.process.returncode is not None


@pytest.mark.parametrize("kill_after_response", [False, True])
async def test_actual_independent_report_worker_http_counter_and_broker(
    host, required_broker, tmp_path, kill_after_response, counting_provider, routing_proxy
):
    _, source, _ = bound_case()
    receipt = await host.kit.service.start_external(
        host.kit.actor, "7", ("42",), "goal", str(uuid4()), source.items
    )
    probe = counting_provider
    worker_broker = dict(required_broker, endpoint=routing_proxy.address)
    processes, messages, failures = [], {}, []
    physical = physical_topic_v2(EVENTS, "TRANSACTION", "ai.mysql", "m3-1")

    async def observe(delivery):
        header = authenticate(
            delivery.wire,
            EVENTS,
            decrypt_keys={"qs.encrypt": host.keys["qs.encrypt"]},
            trusted_signers={"ai.sign": TrustedSigner("qs-ai", host.keys["ai.sign"])},
        )
        first = await host.archives.read_committed(header.message_id)
        assert first is not None and first.wire == delivery.wire
        state = parse_body(header, first.body).interpretation_state
        assert state.session_id == receipt.session_id
        digest = hashlib.sha256(delivery.wire).hexdigest()
        assert header.message_id not in messages or messages[header.message_id] == (
            digest,
            state.status,
        )
        messages[header.message_id] = (digest, state.status)

    async def failed(*_):
        failures.append(True)
        raise AssertionError("Unexpected isolated finalization observer failure")

    observer = RocketMQSubscriber(
        Config(
            required_broker["endpoint"],
            required_broker["observer"]["username"],
            required_broker["observer"]["password"],
            (
                RouteBinding(
                    EVENTS,
                    _OBSERVER,
                    physical,
                    consumer_group_v2(EVENTS, _OBSERVER),
                    "TRANSACTION",
                    "ai.mysql",
                    "m3-1",
                ),
            ),
            max_in_flight=1,
        ),
        EVENTS,
        _OBSERVER,
        handler=observe,
        failed_handler=failed,
        invalid_handler=failed,
        retry_cap=1,
    )
    try:
        await observer.start()
        first_config, marker = await asyncio.to_thread(
            inputs,
            host,
            worker_broker,
            probe,
            probe.directory.parent / "first",
            pause=kill_after_response,
        )
        first = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "tests.probes.finalization_worker",
            "--config",
            str(first_config),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        processes.append(first)
        assert first.pid != os.getpid() and first.pid != probe.process.pid
        if kill_after_response:
            signal = await await_phase(first, marker, "response_committed")
            async with host.tx.open() as db:
                old_call = (
                    (
                        await db.execute(
                            select(model_calls).where(model_calls.c.run_id == receipt.run_id)
                        )
                    )
                    .mappings()
                    .one()
                )
                old_job = (
                    (await db.execute(select(jobs).where(jobs.c.run_id == receipt.run_id)))
                    .mappings()
                    .one()
                )
                old_version = await db.scalar(
                    select(sessions.c.version).where(sessions.c.id == receipt.session_id)
                )
                assert old_call["status"] == "response_received" and old_call["response_json"]
                assert old_call["invocation_id"] == signal["invocation_id"]
                assert (
                    await db.scalar(
                        select(artifacts.c.id).where(artifacts.c.run_id == receipt.run_id)
                    )
                    is None
                )
            first.kill()
            await asyncio.wait_for(first.wait(), 5)
            assert first.returncode == -9
            passed("sigkill_after_committed_response")
            await expire(host.kit, receipt.session_id)
            next_config, marker = await asyncio.to_thread(
                inputs,
                host,
                worker_broker,
                probe,
                probe.directory.parent / "replacement",
                pause=False,
            )
            replacement = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "tests.probes.finalization_worker",
                "--config",
                str(next_config),
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            processes.append(replacement)
            assert replacement.pid != first.pid
            await await_phase(replacement, marker, "done")
            assert await asyncio.wait_for(replacement.wait(), 30) == 0
        else:
            await await_phase(first, marker, "done")
            assert await asyncio.wait_for(first.wait(), 30) == 0
        async with host.tx.open() as db:
            call = (
                (
                    await db.execute(
                        select(model_calls).where(model_calls.c.run_id == receipt.run_id)
                    )
                )
                .mappings()
                .one()
            )
            job = (
                (await db.execute(select(jobs).where(jobs.c.run_id == receipt.run_id)))
                .mappings()
                .one()
            )
            session = (
                (await db.execute(select(sessions).where(sessions.c.id == receipt.session_id)))
                .mappings()
                .one()
            )
            artifact = (
                (await db.execute(select(artifacts).where(artifacts.c.run_id == receipt.run_id)))
                .mappings()
                .one()
            )
            assert session["status"] == "completed" and job["status"] == "done"
            assert artifact["payload"]["invocation_id"] == call["invocation_id"]
            assert (
                await db.scalar(select(outbox.c.message_id).where(outbox.c.producer == "qs-ai"))
                is None
            )
        assert await asyncio.to_thread(probe.counts) == {call["invocation_id"]: 1}
        passed("external_http_counter")
        passed("no_new_publish_outbox")
        prefix = "recovery" if kill_after_response else "normal"
        if kill_after_response:
            assert dict(call) == dict(old_call)
            assert (
                job["attempt"] == old_job["attempt"] and job["fence_token"] > old_job["fence_token"]
            )
            assert session["version"] == old_version + 1
            passed("replacement_reuses_original_response")
        async with asyncio.timeout(60):
            while len(messages) != 2:
                assert not failures
                await asyncio.sleep(0.05)
        assert {s for _, s in messages.values()} == {"running", "completed"}
        passed(prefix + "_broker_delivery")
        passed(prefix + "_actual_worker")
        proxy_stats = await routing_proxy.stats()
        assert proxy_stats["seen"] >= 2 and proxy_stats["dropped"] == 0
        passed("rpc_proxy_forwarding")
        committed = await executions(host)
        assert len(committed) == 2
        for owned in committed:
            for reference in owned.references:
                assert await StoreChecker(host.store).check(reference) == Decision.COMMIT
            first_archive = await host.archives.read_committed(owned.references[0].message_id)
            assert first_archive is not None
        passed(prefix + "_witness_and_archive")
    finally:
        try:
            for process in processes:
                if process.returncode is None:
                    process.kill()
                await asyncio.wait_for(process.wait(), 5)
        finally:
            try:
                await observer.stop(grace_seconds=20)
            finally:
                # Register every child fence, including after a failed assertion,
                # before its original host fixture removes the protocol binding.
                host.executions.extend(await executions(host))
        assert all(process.returncode is not None for process in processes)
        passed("all_owned_processes_stopped")
