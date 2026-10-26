"""Current original MySQL archive/receipt roots and real loopback mTLS RPC.

Controlled command bindings and model-free large questions; no Broker, paired
QS business, database network fault or system-account acceptance is claimed.
Explicit infra collection only; ordinary discovery does not select this file.
"""

import asyncio
import importlib.metadata as metadata
import json
import shutil
import sys
from types import SimpleNamespace

import grpc
import pytest
from reliable_messaging.transaction import Decision, LocalOutcome, StoreChecker, execution_key
from sqlalchemy import delete, insert, select

from qs_ai.application.interpretation.ports import WorkflowResult
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import messaging_pb2_grpc as rpc
from qs_ai.infrastructure.persistence.mysql.messaging import MessagingStore, outbox
from qs_ai.infrastructure.persistence.mysql.messaging_observations import observations
from qs_ai.infrastructure.persistence.mysql.schema import model_calls, questions, result_outbox
from qs_ai.infrastructure.persistence.mysql.transaction_archive import ArchivePayloadReader
from qs_ai.infrastructure.persistence.mysql.transaction_schema import archive, witness
from qs_ai.infrastructure.workflow_transport.interpretation_finish_state import (
    InterpretationFinishInput,
    InterpretationFinishPlanner,
    InterpretationFinishWrite,
    finish_request_id,
    read_finish_plan,
)
from qs_ai.infrastructure.workflow_transport.interpretation_state_root import (
    InterpretationStateRoot,
)
from qs_ai.infrastructure.workflow_transport.payload_access import MySQLPayloadAccess
from qs_ai.transport.grpc.message_payloads import MessagePayloads
from tests.integration import test_transaction_receipts as receipts
from tests.integration.test_delivery import certificates
from tests.integration.test_transaction_interpretation_state import original as original
from tests.integration.test_transaction_receipts import committed as committed
from tests.integration.test_transaction_receipts import kit as kit
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import keys as keys
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)

pytestmark = pytest.mark.integration
_CASES = {}
_SERVERS = dict(started=0, closed=0)


def passed(name):
    _CASES[name] = True


@pytest.fixture(scope="session", autouse=True)
def payload_receipt(request):
    yield
    print(
        "M3_AI_CURRENT_PAYLOAD_RECEIPT "
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
                rpc_servers=_SERVERS,
                root="original_mysql_sqlalchemy",
                sdk=metadata.version("fangcun-reliable-messaging"),
                client="5.1.2",
                python=sys.version.split()[0],
                system_login_not_claimed=True,
                paired_qs_business_not_claimed=True,
                broker_transaction_gate_not_claimed=True,
                real_database_network_fault_not_claimed=True,
                synthetic_command_binding=True,
            )
        ),
        flush=True,
    )


@pytest.fixture
async def payload_plan(original):
    p = original
    incoming = InterpretationFinishInput(
        p.claim,
        WorkflowResult(
            "checkpoint:current-payload",
            question="Synthetic large question " + "x" * 40000,
            can_skip=True,
        ),
    )
    intent = incoming.intent(await finish_request_id(p.host.kit.store, incoming))
    planner = InterpretationFinishPlanner(
        p.host.kit.store,
        signing_key=p.host.keys["ai.sign"],
        recipient_key=p.host.keys["qs.encrypt"],
    )
    winner = await planner.candidate(incoming, intent)
    reserved = await p.host.store.reserve(intent.request, winner)
    assert reserved.acquired
    p.host.executions.append(reserved.execution)
    _, first, question_id = read_finish_plan(incoming, intent, winner)
    assert first.envelope().HasField("payload_reference") and 32768 < len(first.body) < 65535
    passed("large_reference_wire")
    business = InterpretationFinishWrite(p.host.kit.store, incoming, intent, winner)
    root = InterpretationStateRoot(
        p.host.tx, p.host.archives, p.host.store, intent, winner, business
    )
    yield SimpleNamespace(
        original=p,
        host=p.host,
        incoming=incoming,
        intent=intent,
        owned=reserved.execution,
        first=first,
        root=root,
        reference=first.envelope().payload_reference,
        question_id=question_id,
    )


async def counter(host, name):
    async with host.tx.open() as db:
        return int(
            await db.scalar(
                select(observations.c.recorded_count).where(observations.c.kind == name)
            )
            or 0
        )


@pytest.fixture
async def payload_rpc(host, tmp_path):
    directory = tmp_path / "private-payload-tls"
    directory.mkdir(mode=0o700)
    certificates(directory)
    for path in directory.iterdir():
        path.chmod(0o600)
    server, channels = grpc.aio.server(), []

    class ReadLegacy(MessagingStore):
        calls = 0

        async def payload(self, db, reference, workload):
            self.calls += 1
            return await super().payload(db, reference, workload)

    legacy = ReadLegacy()
    access = MySQLPayloadAccess(host.tx, ArchivePayloadReader(host.archives))
    recorded_workload_denial = asyncio.Event()
    record = access.record_failure

    async def observe_failure(kind):
        value = await record(kind)
        if kind == "payload_serve_workload_denied":
            recorded_workload_denial.set()
        return value

    access.record_failure = observe_failure
    rpc.add_MessagePayloadsServicer_to_server(MessagePayloads(access), server)
    ca = (directory / "ca.pem").read_bytes()
    port = server.add_secure_port(
        "127.0.0.1:0",
        grpc.ssl_server_credentials(
            [((directory / "ai.key").read_bytes(), (directory / "ai.pem").read_bytes())],
            root_certificates=ca,
            require_client_auth=True,
        ),
    )
    assert port > 0
    await server.start()
    _SERVERS["started"] += 1

    def stub(identity="qs"):
        credentials = grpc.ssl_channel_credentials(
            root_certificates=ca,
            private_key=(directory / (identity + ".key")).read_bytes() if identity else None,
            certificate_chain=(directory / (identity + ".pem")).read_bytes() if identity else None,
        )
        channel = grpc.aio.secure_channel(f"localhost:{port}", credentials)
        channels.append(channel)
        return rpc.MessagePayloadsStub(channel)

    try:
        yield SimpleNamespace(
            stub=stub, legacy=legacy, recorded_workload_denial=recorded_workload_denial
        )
    finally:
        try:
            for channel in channels:
                await channel.close()
        finally:
            await server.stop(2)
            async with asyncio.timeout(5):
                await server.wait_for_termination()
            _SERVERS["closed"] += 1
            shutil.rmtree(directory)
            assert not directory.exists()
            passed("rpc_channels_and_servers_closed")


async def committed_body(p, transport):
    assert await p.root(p.owned) == LocalOutcome.COMMITTED
    result = await transport.stub().Get(p.reference, timeout=3)
    assert result.reference == p.reference and result.body == p.first.body
    assert await StoreChecker(p.host.store).check(p.owned.references[0]) == Decision.COMMIT
    async with p.host.tx.open() as db:
        assert (
            await db.scalar(
                select(outbox.c.message_id).where(outbox.c.message_id == p.reference.message_id)
            )
            is None
        )
        history = (
            (
                await db.execute(
                    select(result_outbox).where(result_outbox.c.event_id == p.reference.message_id)
                )
            )
            .mappings()
            .one()
        )
        assert history["mq_owned"] and not history["delivered"] and history["attempts"] == 0
        assert (
            await db.scalar(
                select(model_calls.c.run_id).where(model_calls.c.run_id == p.original.claim.run_id)
            )
            is None
        )
    passed("no_new_publish_outbox")
    return result


@pytest.mark.parametrize("commit", [True, False])
async def test_current_payload_original_root_mtls_visibility_and_first_body(
    payload_plan, payload_rpc, monkeypatch, commit
):
    p, transport = payload_plan, payload_rpc
    entered, release = asyncio.Event(), asyncio.Event()
    original_bind = p.host.archives.bind

    async def bind(db, owned):
        assert owned == p.owned
        guard = await original_bind(db, owned)
        put = guard.put

        async def staged(branch):
            await put(branch)
            # This physical writer can see its archive; another RPC boundary cannot.
            assert (
                await db.scalar(
                    select(archive.c.message_id).where(
                        archive.c.message_id == p.reference.message_id.encode()
                    )
                )
                is not None
            )
            entered.set()
            await release.wait()
            if not commit:
                raise RuntimeError("Controlled original-root rollback, not a network failure")

        guard.put = staged
        return guard

    monkeypatch.setattr(p.host.archives, "bind", bind)
    task = asyncio.create_task(p.root(p.owned))
    try:
        async with asyncio.timeout(10):
            await entered.wait()
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await transport.stub().Get(p.reference, timeout=3)
        assert error.value.code() == grpc.StatusCode.NOT_FOUND
        passed("uncommitted_current_payload_hidden")
        release.set()
        result = await asyncio.wait_for(task, 5)
        if commit:
            assert result == LocalOutcome.COMMITTED
            value = await transport.stub().Get(p.reference, timeout=3)
            assert value.reference == p.reference and value.body == p.first.body
            assert await StoreChecker(p.host.store).check(p.owned.references[0]) == Decision.COMMIT
            async with p.host.tx.open() as db:
                assert (
                    await db.scalar(select(questions.c.text).where(questions.c.id == p.question_id))
                    == p.incoming.result.question
                )
                first_wire = await db.scalar(
                    select(archive.c.first_wire).where(
                        archive.c.message_id == p.reference.message_id.encode()
                    )
                )
                assert bytes(first_wire) == p.first.wire
            passed("committed_current_body_exact")
            passed("committed_business_archive_witness")
        else:
            assert result == LocalOutcome.ABORTED
            assert (
                await StoreChecker(p.host.store).check(p.owned.references[0]) == Decision.ROLLBACK
            )
            with pytest.raises(grpc.aio.AioRpcError) as error:
                await transport.stub().Get(p.reference, timeout=3)
            assert error.value.code() == grpc.StatusCode.NOT_FOUND
            passed("rolled_back_current_body_hidden")
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize(
    "field,value",
    [
        ("producer", "qs-server"),
        ("destination", "other"),
        ("organization_id", "999"),
        ("body_sha256", "b" * 64),
        ("body_length", 1),
    ],
)
async def test_current_payload_mtls_reference_mismatch_diagnostics(
    payload_plan, payload_rpc, field, value
):
    p, transport = payload_plan, payload_rpc
    await committed_body(p, transport)
    before = await counter(p.host, "payload_serve_reference_mismatch")
    storage = await counter(p.host, "payload_serve_storage_unavailable")
    old_reads = transport.legacy.calls
    reference = pb.MessagePayloadReference()
    reference.CopyFrom(p.reference)
    setattr(reference, field, value)
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await transport.stub().Get(reference, timeout=3)
    assert error.value.code() == grpc.StatusCode.NOT_FOUND
    assert await counter(p.host, "payload_serve_reference_mismatch") == before + 1
    assert await counter(p.host, "payload_serve_storage_unavailable") == storage
    assert transport.legacy.calls == old_reads
    passed("reference_" + field)


@pytest.mark.parametrize("identity", ["other", "missing"])
async def test_current_payload_mtls_wrong_or_missing_client_is_denied(
    payload_plan, payload_rpc, identity, monkeypatch
):
    p, transport = payload_plan, payload_rpc
    await committed_body(p, transport)  # Positive control on the same endpoint first.
    entered = 0
    read = p.host.archives.read_committed

    async def observe(message_id):
        nonlocal entered
        entered += 1
        return await read(message_id)

    monkeypatch.setattr(p.host.archives, "read_committed", observe)
    before = await counter(p.host, "payload_serve_workload_denied")
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await transport.stub("other" if identity == "other" else None).Get(p.reference, timeout=3)
    if identity == "other":
        assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED
        # context.abort sends the status before its caller records the bounded
        # technical observation. Await that completion rather than assume the
        # client status is a barrier for subsequent server-side SQL.
        await asyncio.wait_for(transport.recorded_workload_denial.wait(), 2)
        assert await counter(p.host, "payload_serve_workload_denied") == before + 1
        passed("wrong_workload_denied")
    else:
        assert error.value.code() in {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.UNAUTHENTICATED}
        assert await counter(p.host, "payload_serve_workload_denied") == before
        passed("missing_certificate_denied")
    assert entered == 0


async def test_current_payload_missing_exact_witness_is_unavailable_without_fallback(
    payload_plan, payload_rpc
):
    p, transport = payload_plan, payload_rpc
    first = await committed_body(p, transport)
    key = execution_key(
        p.owned.plan.owner, p.owned.plan.operation_id, p.owned.writer_epoch, p.owned.fence
    )
    async with p.host.tx.open() as db:
        saved = (
            (await db.execute(select(witness).where(witness.c.execution_key == key)))
            .mappings()
            .all()
        )
        assert len(saved) == 1
        # Isolated storage-corruption fixture; never a permitted SDK state transition.
        await db.execute(delete(witness).where(witness.c.execution_key == key))
        await db.commit()
    before, old_reads = (
        await counter(p.host, "payload_serve_storage_unavailable"),
        transport.legacy.calls,
    )
    try:
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await transport.stub().Get(p.reference, timeout=3)
        assert error.value.code() == grpc.StatusCode.UNAVAILABLE
        assert transport.legacy.calls == old_reads
        assert await counter(p.host, "payload_serve_storage_unavailable") == before + 1
        passed("missing_authority_unavailable")
    finally:
        async with p.host.tx.open() as db:
            await db.execute(insert(witness), [dict(row) for row in saved])
            await db.commit()
    restored = await transport.stub().Get(p.reference, timeout=3)
    assert restored == first and restored.body == p.first.body
    passed("restored_authority_first_body")


@pytest.mark.parametrize("kind", [pb.COMMAND_RECEIPT, pb.INTERPRETATION_STATE])
async def test_current_receipt_original_root_atomicity_and_first_receipt(committed, kind):
    await receipts.test_new_business_receipt_and_original_history_share_ack_commit_and_rollback(
        committed, kind
    )
    passed("receipt_atomic_" + str(kind))


async def test_current_receipt_held_then_stored_does_not_reopen_delivery(committed):
    await receipts.test_held_and_stored_receipts_are_retained_without_reopening_completed_delivery(
        committed
    )
    passed("receipt_held_stored")


async def test_current_receipt_conflict_and_digest_do_not_change_business(committed):
    await receipts.test_conflicting_first_ack_and_wrong_event_digest_cannot_change_receipts(
        committed
    )
    passed("receipt_conflict")


async def test_current_receipt_concurrent_duplicate_retains_one_result(committed):
    await receipts.test_concurrent_duplicate_ack_has_one_original_business_receipt(committed)
    passed("receipt_concurrent")
