"""T18-D five original worker paths; real RPC loss, HTTP ledger and process death.

Imported by the explicit finalization profile only. Fixture generation for the
semantic dependency is controlled; this does not claim a paired QS business run.
"""

import asyncio
import hashlib
import json
import os
import sys
from uuid import uuid4

import pytest
from reliable_messaging.protected import TrustedSigner
from reliable_messaging.rocketmq import Config, RocketMQSubscriber
from reliable_messaging.transaction import Decision, StoreChecker
from reliable_messaging.transport import RouteBinding, consumer_group_v2, physical_topic_v2
from sqlalchemy import delete, select, update

from qs_ai.application.interpretation.route_assets import executable_route
from qs_ai.infrastructure.persistence.mysql.messaging import outbox
from qs_ai.infrastructure.persistence.mysql.schema import (
    artifacts,
    evaluation_checkpoints,
    evaluation_completions,
    evaluation_response_receipts,
    evaluation_runs,
    evaluation_slot_claims,
    jobs,
    model_calls,
    sessions,
)
from qs_ai.infrastructure.workflow_transport.messaging import EVENTS, authenticate, parse_body
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences

PATHS = (
    "interpretation_report",
    "serial_generation",
    "serial_semantic",
    "candidate_generation",
    "candidate_semantic",
)
SCENARIOS = (
    "normal",
    "response_committed_terminal_mq_unavailable",
    "terminal_committed_confirmation_lost",
    "response_committed_process_sigkill",
)


async def response_row(tx, workflow, run_id, invocation_id):
    table = model_calls if workflow == "interpretation_report" else evaluation_response_receipts
    async with tx.open() as db:
        return (
            (
                await db.execute(
                    select(table).where(
                        table.c.run_id == str(run_id),
                        table.c.invocation_id == invocation_id,
                    )
                )
            )
            .mappings()
            .one()
        )


async def business(tx, workflow, run_id):
    async with tx.open() as db:
        if workflow == "interpretation_report":
            job = (await db.execute(select(jobs).where(jobs.c.run_id == run_id))).mappings().one()
            state = (
                (await db.execute(select(sessions).where(sessions.c.active_run_id == run_id)))
                .mappings()
                .one()
            )
            terminal = (
                (await db.execute(select(artifacts).where(artifacts.c.run_id == run_id)))
                .mappings()
                .all()
            )
            return dict(
                version=state["version"],
                status=state["status"],
                terminal=terminal,
                attempt=job["attempt"],
                fence=job["fence_token"],
            )
        checkpoint = (
            (
                await db.execute(
                    select(evaluation_checkpoints).where(
                        evaluation_checkpoints.c.run_id == str(run_id)
                    )
                )
            )
            .mappings()
            .one()
        )
        terminal = (
            (
                await db.execute(
                    select(evaluation_completions).where(
                        evaluation_completions.c.run_id == str(run_id),
                        evaluation_completions.c.kind == workflow.split("_")[1],
                    )
                )
            )
            .mappings()
            .all()
        )
        return dict(
            version=checkpoint["version"],
            checkpoint=checkpoint["checkpoint_json"],
            terminal=terminal,
        )


async def wait_worker(process, marker, *, error=None):
    from tests.integration.rocketmq_finalization_host import read_progress

    assert await asyncio.wait_for(process.wait(), 60) == (1 if error else 0)
    value = await asyncio.to_thread(read_progress, marker)
    assert value["pid"] == process.pid
    if error:
        assert value == dict(phase="error", pid=process.pid, error_class=error)
    else:
        assert value["phase"] == "done" and value["worked"] is True


@pytest.mark.parametrize("workflow", PATHS)
@pytest.mark.parametrize("scenario", SCENARIOS)
async def test_independent_five_path_finalization_matrix(
    host,
    required_broker,
    evaluation_ready,
    workflow,
    scenario,
    counting_provider,
    routing_proxy,
):
    from tests.integration.rocketmq_finalization_host import (
        await_phase,
        executions,
        inputs,
        passed,
    )
    from tests.integration.test_evaluation_candidate_execution import ConcurrentGateway
    from tests.integration.test_evaluation_step import AT, Gateway, step
    from tests.integration.test_interpretation import expire
    from tests.test_input_binding import bound_case

    report = workflow == "interpretation_report"
    if report:
        _, source, _ = bound_case()
        receipt = await host.kit.service.start_external(
            host.kit.actor, "7", ("42",), "goal", str(uuid4()), source.items
        )
        run_id, tx, provider = receipt.run_id, host.tx, None
    else:
        tx, run_id, _, routes, schemas = evaluation_ready
        provider = executable_route(routes.get.return_value).provider
        if workflow.startswith("candidate_"):
            async with tx.open() as db:
                await db.execute(
                    update(evaluation_runs)
                    .where(evaluation_runs.c.run_id == str(run_id))
                    .values(execution_mode="candidate_v2")
                )
                await db.commit()
        if workflow.endswith("semantic"):
            if workflow.startswith("serial_"):
                await step(evaluation_ready, Gateway(evaluation_ready))
            else:
                from qs_ai.infrastructure.workflows.evaluation import execute_step

                seed = ConcurrentGateway(evaluation_ready)
                seed.release.set()
                await execute_step(
                    tx,
                    run_id,
                    3,
                    1,
                    "worker:controlled-seed",
                    seed,
                    routes,
                    schemas,
                    clock=lambda: AT,
                    candidate_limit=1,
                )

    probe, messages, failures, processes = counting_provider, {}, [], []
    physical = physical_topic_v2(EVENTS, "TRANSACTION", "ai.mysql", "m3-1")
    subscription = "qs-m3-ai-state-observer"

    async def observe(delivery):
        header = authenticate(
            delivery.wire,
            EVENTS,
            decrypt_keys={"qs.encrypt": host.keys["qs.encrypt"]},
            trusted_signers={"ai.sign": TrustedSigner("qs-ai", host.keys["ai.sign"])},
        )
        first = await host.archives.read_committed(header.message_id)
        assert first is not None and first.wire == delivery.wire
        state = parse_body(header, first.body)
        if report:
            assert state.interpretation_state.run_id == str(run_id)
        else:
            assert state.evaluation_state.run_id == str(run_id)
        digest = hashlib.sha256(delivery.wire).hexdigest()
        assert header.message_id not in messages or messages[header.message_id] == digest
        messages[header.message_id] = digest

    async def failed(*_):
        failures.append(True)
        raise AssertionError("Unexpected isolated finalization matrix observer failure")

    observer = RocketMQSubscriber(
        Config(
            required_broker["endpoint"],
            required_broker["observer"]["username"],
            required_broker["observer"]["password"],
            (
                RouteBinding(
                    EVENTS,
                    subscription,
                    physical,
                    consumer_group_v2(EVENTS, subscription),
                    "TRANSACTION",
                    "ai.mysql",
                    "m3-1",
                ),
            ),
            max_in_flight=1,
        ),
        EVENTS,
        subscription,
        handler=observe,
        failed_handler=failed,
        invalid_handler=failed,
        retry_cap=1,
    )

    async def start(label, *, pause, recover=False):
        path, marker = await asyncio.to_thread(
            inputs,
            host,
            dict(required_broker, endpoint=routing_proxy.address),
            probe,
            probe.directory.parent / label,
            pause=pause,
        )
        config = json.loads(path.read_text())
        config.update(
            workflow=workflow,
            provider=provider,
            recover=recover,
            resume_file=str(path.parent / "resume"),
            outcomes=str(path.parent / "outcomes.json"),
        )
        path.write_text(json.dumps(config))
        child = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "tests.probes.finalization_worker",
            "--config",
            str(path),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        processes.append(child)
        assert child.pid != os.getpid() and child.pid != probe.process.pid
        return child, marker, path.parent

    try:
        await observer.start()
        first, marker, directory = await start("matrix-first", pause=True)
        signal = await await_phase(first, marker, "response_committed")
        invocation = signal["invocation_id"]
        original_response = dict(await response_row(tx, workflow, run_id, invocation))
        before = await business(tx, workflow, run_id)
        assert not before["terminal"] and await asyncio.to_thread(probe.counts) == {invocation: 1}
        first_executions = await executions(host)
        assert len(first_executions) == 1
        if scenario == "response_committed_process_sigkill":
            first.kill()
            assert await asyncio.wait_for(first.wait(), 5) == -9
        else:
            if scenario == "response_committed_terminal_mq_unavailable":
                await routing_proxy.arm("before", "SendMessage")
            elif scenario == "terminal_committed_confirmation_lost":
                await routing_proxy.arm("after", "EndTransaction")
            (directory / "resume").touch(mode=0o600)
            fault = scenario != "normal"
            if fault:
                injected = await routing_proxy.receive("injected")
                assert injected["kind"] == "rocketmq" and injected["dropped"] == 1
                assert injected["backend_ack"] is (
                    scenario == "terminal_committed_confirmation_lost"
                )
                assert injected["boundary"] == ("after" if injected["backend_ack"] else "before")
            await wait_worker(
                first,
                marker,
                error=(
                    "TransactionSendError"
                    if scenario == "response_committed_terminal_mq_unavailable"
                    else None
                ),
            )
            outcomes = json.loads((directory / "outcomes.json").read_text())
            assert len(outcomes) == 2
            final = outcomes[-1]
            if scenario == "terminal_committed_confirmation_lost":
                assert final["local"] == "committed"
                assert final["branches"] == [dict(half="prepared", final="unknown")]
            elif scenario == "response_committed_terminal_mq_unavailable":
                assert final["local"] == "not_started"
                assert final["branches"][0]["half"] == "unknown"
                assert await business(tx, workflow, run_id) == before
            else:
                assert final["local"] == "committed"
                assert final["branches"] == [dict(half="prepared", final="commit_confirmed")]

        recovering = scenario in (
            "response_committed_process_sigkill",
            "response_committed_terminal_mq_unavailable",
        )
        if recovering:
            if report:
                await expire(host.kit, receipt.session_id)
            replacement, marker, _ = await start("matrix-replacement", pause=False, recover=True)
            assert replacement.pid != first.pid
            await wait_worker(replacement, marker)
        after = await business(tx, workflow, run_id)
        assert after["version"] == before["version"] + 1 and len(after["terminal"]) == 1
        assert dict(await response_row(tx, workflow, run_id, invocation)) == original_response
        assert await asyncio.to_thread(probe.counts) == {invocation: 1}
        if report:
            assert after["status"] == "completed" and after["attempt"] == before["attempt"]
            assert after["terminal"][0]["payload"]["invocation_id"] == invocation
        else:
            assert after["checkpoint"] is None
            assert after["terminal"][0]["invocation_id"] == invocation
            assert after["terminal"][0]["evidence_json"]["status"] == "succeeded"
        async with tx.open() as db:
            assert (
                await db.scalar(select(outbox.c.message_id).where(outbox.c.producer == "qs-ai"))
                is None
            )
        committed = []
        aborted = []
        for owned in await executions(host):
            decisions = [await StoreChecker(host.store).check(ref) for ref in owned.references]
            if set(decisions) == {Decision.COMMIT}:
                committed.append(owned)
                for reference in owned.references:
                    first_archive = await host.archives.read_committed(reference.message_id)
                    assert first_archive is not None
                    async with asyncio.timeout(60):
                        while reference.message_id not in messages:
                            assert not failures
                            await asyncio.sleep(0.02)
                    assert (
                        messages[reference.message_id]
                        == hashlib.sha256(first_archive.wire).hexdigest()
                    )
            else:
                assert set(decisions) == {Decision.ROLLBACK}
                aborted.append(owned)
        assert len(committed) == 2 and len(messages) == 2
        assert len(aborted) == int(scenario == "response_committed_terminal_mq_unavailable")
        if aborted:
            # Explicit host retry changes only execution fence, retaining first plan/wire.
            winner = next(
                e for e in committed if e.plan.operation_id == aborted[0].plan.operation_id
            )
            assert aborted[0].plan == winner.plan
            assert aborted[0].fence < winner.fence
        if scenario in (
            "response_committed_terminal_mq_unavailable",
            "terminal_committed_confirmation_lost",
        ):
            stats = await routing_proxy.stats()
            assert stats["dropped"] == 1
            assert stats["backend_commit_acks"] == int(
                scenario == "terminal_committed_confirmation_lost"
            )
        passed("matrix_" + workflow + "_" + scenario)
    finally:
        try:
            for child in processes:
                if child.returncode is None:
                    child.kill()
                await asyncio.wait_for(child.wait(), 5)
        finally:
            try:
                await observer.stop(grace_seconds=20)
            finally:
                host.executions.extend(await executions(host))
                async with tx.open() as db:
                    for table in (
                        evaluation_response_receipts,
                        evaluation_slot_claims,
                        evaluation_sequences,
                    ):
                        await db.execute(delete(table).where(table.c.run_id == str(run_id)))
                    await db.commit()
        assert all(child.returncode is not None for child in processes)
