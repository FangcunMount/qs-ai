"""Real paired-host process/network/Broker failures; explicitly selected only.

Uses original roots and the existing default receivers. Synthetic read-side
evidence; no real provider/public HTTP/system/production acceptance claim.
"""

import asyncio
import json
import subprocess
from contextlib import suppress
from pathlib import Path

import pytest
from reliable_messaging.transaction import Decision, LocalOutcome, StoreChecker
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from tests.integration.rocketmq_paired_host import QSProcess, start_loop
from tests.integration.rocketmq_paired_host import host as host
from tests.integration.rocketmq_paired_host import keys as keys
from tests.integration.rocketmq_paired_host import kit as kit
from tests.integration.rocketmq_paired_host import pair as pair
from tests.integration.rocketmq_paired_host import (
    required_disposable_primary as required_disposable_primary,
)
from tests.integration.rocketmq_paired_host import required_pair as required_pair

pytestmark = pytest.mark.integration
_CASES = {}
_PROOF = {}


def passed(name):
    _CASES[name] = True


async def until(predicate, seconds=100):
    async with asyncio.timeout(seconds):
        while not await predicate():  # noqa: ASYNC110 - External DB/Broker has no event hook.
            await asyncio.sleep(0.05)


async def control(config, name, expected):
    path = Path(config["fault_control"][name])
    assert path.is_absolute() and path.is_relative_to(Path(config["private_directory"]))

    def read():
        if not path.exists():
            return False
        assert path.stat().st_mode & 0o077 == 0
        return path.read_text() == expected

    await until(lambda: asyncio.to_thread(read), seconds=180)


@pytest.fixture(scope="session", autouse=True)
def fault_receipt(required_pair, request):
    assert required_pair.get("paired_faults") is True
    source = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    yield
    for name, value in _PROOF.items():
        resource = value["resources"]
        if resource.get("closed") and resource.get("private_files_removed"):
            original = resource.get("original_exit_code")
            replacements = resource.get("replacement_exit_codes")
            if name == "qs_sigkill" and original == -9 and replacements == [0]:
                passed("recovered_process_resources_drained")
            if name == "broker_restart" and original == 0 and replacements == []:
                passed("broker_fault_resources_drained")
    print(
        "M3_PAIRED_FAULT_RECEIPT "
        + json.dumps(
            dict(
                source_sha=source,
                qs_source_sha=required_pair["qs_source_sha"],
                profile=required_pair["paired_profile"],
                passed=len(_CASES),
                failed=request.session.testsfailed,
                skipped=len(
                    request.config.pluginmanager.get_plugin("terminalreporter").stats.get(
                        "skipped", []
                    )
                ),
                cases=_CASES,
                proof=_PROOF,
                scope=(
                    "actual paired Go QS/Python AI original roots with MQ disconnect, "
                    "producer SIGKILL and Broker SIGKILL/restart"
                ),
                public_http_real_provider_system_production_not_claimed=True,
                synthetic_read_side=True,
            )
        ),
        flush=True,
    )


async def original_producer_loss(p):
    url = make_url(p.config["qs_primary_url"])
    assert url.host == "127.0.0.1" and url.drivername == "mysql+asyncmy"
    assert p.schema.startswith("qs_m3_paired_")
    engine = create_async_engine(url.set(database=p.schema), pool_size=2, max_overflow=0)
    blocker, task = None, None
    observation = dict(resources=p.resource_receipt)
    _PROOF["qs_sigkill"] = observation

    async def before(identity, _):
        nonlocal blocker, task
        async with engine.begin() as db:
            await db.execute(
                text(
                    "INSERT INTO ai_messaging_aggregates(aggregate_key,next_sequence) "
                    "VALUES(:key,1)"
                ),
                {"key": identity},
            )
        blocker = await engine.connect()
        await blocker.begin()
        await blocker.execute(
            text(
                "SELECT next_sequence FROM ai_messaging_aggregates "
                "WHERE aggregate_key=:key FOR UPDATE"
            ),
            {"key": identity},
        )

        async def prepared():
            async with engine.connect() as db:
                count = await db.scalar(
                    text("SELECT COUNT(*) FROM rm_tx_witness WHERE half_outcome='prepared'")
                )
                waits = await db.scalar(
                    text(
                        "SELECT COUNT(*) FROM performance_schema.data_lock_waits w "
                        "JOIN performance_schema.data_locks l "
                        "ON l.ENGINE_LOCK_ID=w.REQUESTING_ENGINE_LOCK_ID "
                        "WHERE l.OBJECT_SCHEMA=DATABASE() "
                        "AND l.OBJECT_NAME='ai_messaging_aggregates'"
                    )
                )
                return count == 1 and waits > 0

        async def disconnect():
            await until(prepared, seconds=20)
            stats = await p.qs_proxy.stats()
            assert stats["seen"] == 1 and stats["dropped"] == 0
            await (
                p.qs_proxy.close()
            )  # Real listener/connection loss before releasing the original root.
            assert p.qs_proxy.process.returncode is not None and blocker.in_transaction()
            await blocker.commit()
            observation["half_prepared_before_loss"] = 1
            observation["proxy_stopped_before_root_release"] = True
            observation["original_root_lock_wait_observed"] = True
            passed("half_ack_before_actual_mq_disconnect")

        task = asyncio.create_task(disconnect())

    async def after(identity, start):
        assert task is not None
        await task
        old = p.qs
        old_state = (await old.call(mode="owner_state"))["protocol"]
        assert old_state["completed_results"] == old_state["local_committed"] == 1
        assert old_state["half_prepared"] == old_state["final_unknown"] == 1
        assert not p.host.executions
        facts = await old.call(
            mode="facts", request_id=identity, command_id=identity, actor=start["actor"]
        )
        assert facts["counts"]["rm_tx_execution"] == facts["counts"]["rm_tx_witness"] == 1
        assert facts["counts"]["ai_messaging_outbox"] == 0 and facts["seals"] == 1
        first_wire = facts["command"]["wire_sha256"]
        passed("old_local_committed_final_unknown")
        old_pid = old.process.pid
        await old.sigkill_after_committed_root()
        assert old.process.returncode == -9
        passed("producer_sigkill_exit_negative9")
        data = json.loads(p.go_config.read_text())
        original_binding = data["Options"]["rocketmq"]["transactions"]
        data["RecoverExisting"] = True
        data["PayloadBind"] = p.ready["payload_endpoint"]
        data["Options"]["rocketmq"]["endpoint"] = p.config["endpoint"]
        assert data["Options"]["rocketmq"]["transactions"] == original_binding
        path = p.directory / "go-recovery-private.json"
        path.write_text(json.dumps(data))
        path.chmod(0o600)
        process = await asyncio.create_subprocess_exec(
            p.config["qs_binary"],
            str(path),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=p.stderr,
            env=p.env,
            umask=0o077,
        )
        replacement = QSProcess(process, p.directory)
        p.replacements.append(replacement)
        ready = await replacement.read()
        assert ready["ready"] and ready["recovered"] and ready["schema"] == p.schema
        assert ready["payload_endpoint"] == p.ready["payload_endpoint"] and process.pid != old_pid
        p.qs = replacement

        async def checked():
            state = (await replacement.call(mode="owner_state"))["protocol"]
            return state["check_commit"] > 0

        await until(checked)
        state = (await replacement.call(mode="owner_state"))["protocol"]
        assert not state["ready"] and state["half_prepared"] == state["completed_results"] == 0
        recovered = await replacement.call(
            mode="facts", request_id=identity, command_id=identity, actor=start["actor"]
        )
        assert recovered["command"]["wire_sha256"] == first_wire
        assert recovered["counts"]["rm_tx_execution"] == recovered["counts"]["rm_tx_witness"] == 1
        assert recovered["seals"] == 0
        observation.update(
            old_exit_code=-9,
            distinct_producer_pid=True,
            old_seals=1,
            recovered_completed_results=0,
            recovered_half_prepared=0,
            recovered_broker_commit_checks=state["check_commit"],
            original_wire_unchanged=True,
        )
        passed("same_primary_epoch_readonly_resume_no_new_half")
        passed("new_paused_broker_commit_callback")
        passed("same_first_command_wire_after_resume")
        assert await replacement.call(mode="open_recovery") == {"ok": True}

    try:
        result = await start_loop(p, before_start=before, after_start=after, expected_seals=2)
        assert result.facts["seals"] + observation["old_seals"] == 3
        assert len(p.host.executions) == 1
        observation["final_recovered_seals"] = result.facts["seals"]
        observation["provider_invocations"] = 0  # Also asserted from the real table by start_loop.
        passed("recovered_pair_receipt_state_ack_without_model")
    finally:
        if task is not None and not task.done():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        if blocker is not None:
            await blocker.close()
        await engine.dispose()


async def broker_loss(p):
    committed, release = asyncio.Event(), asyncio.Event()
    original = p.services.runtime.execute
    local_calls = 0
    observation = dict(resources=p.resource_receipt)
    _PROOF["broker_restart"] = observation

    async def observed_execute(owned, local):
        async def real_root(exact):
            nonlocal local_calls
            local_calls += 1
            result = await local(exact)
            assert result == LocalOutcome.COMMITTED
            committed.set()
            await (
                release.wait()
            )  # Only a fault-point barrier; original root and result are unchanged.
            return result

        return await original(owned, real_root)

    p.services.runtime.execute = observed_execute

    async def after(_, __):
        async with asyncio.timeout(40):
            await committed.wait()
        assert local_calls == 1 and len(p.host.executions) == 1
        exact = p.host.executions[0]
        assert len(exact.references) == 2
        for reference in exact.references:
            assert await StoreChecker(p.host.store).check(reference) == Decision.COMMIT
        before_kill = p.services.runtime.stats()
        assert before_kill["completed_results"] == 0
        passed("ai_root_witness_committed_before_broker_kill")
        print('\nM3_PAIRED_FAULT_STATE {"event":"ai_root_committed"}', flush=True)
        await control(p.config, "broker_killed", "broker-killed")
        release.set()

        async def final_unknown():
            value = p.services.runtime.stats()
            return (
                value["completed_results"] == 1
                and value["local_committed_results"] == 1
                and value["final_unknown_results"] == 2
                and value["pending_calls"] == 0
                and value["active_sends"] == 0
            )

        await until(final_unknown, seconds=30)
        passed("actual_final_rpc_unknown_after_committed_root")
        print('\nM3_PAIRED_FAULT_STATE {"event":"final_unknown_observed"}', flush=True)
        await control(p.config, "broker_ready", "broker-ready")
        passed("broker_sigkill_and_same_container_restart")

        async def rechecked():
            return p.services.runtime.stats()["check_commit"] >= before_kill["check_commit"] + 2

        await until(rechecked)
        stats = p.services.runtime.stats()
        assert stats["half_prepared_results"] == 2 and stats["completed_results"] == 1
        assert local_calls == 1
        observation.update(
            local_calls=local_calls,
            half_prepared=2,
            actual_final_unknown=2,
            broker_commit_checks=stats["check_commit"] - before_kill["check_commit"],
            pending_calls=0,
        )
        passed("broker_recheck_original_ai_first_wire")

    try:
        await start_loop(p, after_start=after)
        assert len(p.host.executions) == 1 and local_calls == 1
        observation["provider_invocations"] = 0
        passed("pair_both_business_acks_without_executor_replay")
        passed("no_additional_model_or_old_business_executor")
    finally:
        release.set()
        p.services.runtime.execute = original


@pytest.mark.parametrize("pair", ["qs_sigkill", "broker_restart"], indirect=True)
async def test_actual_paired_fault_recovery(pair):
    if pair.fault_mode == "qs_sigkill":
        await original_producer_loss(pair)
    else:
        await broker_loss(pair)
