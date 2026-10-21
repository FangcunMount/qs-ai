"""Original bounded scanner responsibilities; no model/network or storage proof."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import Binding, LocalOutcome, Result, State

from qs_ai.application.execution.errors import LeaseLost
from qs_ai.infrastructure.workflow_transport.interpretation_claim_sender import (
    InterpretationClaimPending,
    InterpretationClaimQuotaDenied,
)
from qs_ai.infrastructure.workflow_transport.transaction_root import TransactionAdmissionClosed
from qs_ai.infrastructure.workflow_transport.transactional_interpretation_execution import (
    TransactionalInterpretationExecutionStore,
)


@pytest.fixture
def original():
    opened, events = [], []
    db = object()

    @asynccontextmanager
    async def open():
        opened.append(True)
        events.append("source-open")
        try:
            yield db
        finally:
            opened.pop()
            events.append("source-closed")

    store = SimpleNamespace(
        transactions=SimpleNamespace(open=open),
        claim_candidates=AsyncMock(return_value=[("job1", "session1")]),
        renew=AsyncMock(),
        evidence=AsyncMock(),
    )
    protocol_store = SimpleNamespace(
        binding=Binding("ai.mysql", "fixture", "m3-1"), lookup=AsyncMock(return_value=None)
    )
    runtime = SimpleNamespace(store=protocol_store, stats=Mock(return_value={"ready": True}))
    adapter = TransactionalInterpretationExecutionStore(
        store,
        SimpleNamespace(),
        runtime,
        signing_key=None,
        recipient_key=None,
    )
    sources = [
        SimpleNamespace(intent=lambda _: SimpleNamespace(request=object()), exhausted=False)
        for _ in range(2)
    ]

    async def source(*_):
        assert not opened, "Due-job connection must close before planning or MQ"
        return sources[0]

    claims = AsyncMock(return_value=object())
    claims.source = AsyncMock(side_effect=source)
    claims.retry = AsyncMock(return_value=object())
    adapter._claims = claims
    adapter._finish, adapter._defer = AsyncMock(), AsyncMock(return_value=True)
    return SimpleNamespace(**locals())


async def test_constructor_inert_then_first_claim_closes_source_and_keeps_original_scan(original):
    p = original
    assert not p.events
    result = await p.adapter.claim(30)
    assert result is p.claims.return_value and p.events == ["source-open", "source-closed"]
    p.store.claim_candidates.assert_awaited_once_with(p.db)
    p.claims.assert_awaited_once_with(p.sources[0], 30)
    p.claims.retry.assert_not_called()


async def test_exact_aborted_execution_is_an_explicit_host_retry_not_a_new_plan(original):
    p = original
    previous = SimpleNamespace(state=State.ABORTED, execution=object())
    p.protocol_store.lookup.return_value = previous
    assert await p.adapter.claim(30) is p.claims.retry.return_value
    p.claims.retry.assert_awaited_once_with(p.sources[0], 30, previous.execution)
    p.claims.assert_not_called()


async def test_prepared_owner_is_skipped_before_next_original_candidate(original):
    p = original
    p.store.claim_candidates.return_value.append(("job2", "session2"))
    p.claims.source.side_effect = p.sources
    p.protocol_store.lookup.side_effect = [SimpleNamespace(state=State.PREPARED), None]
    assert await p.adapter.claim(30) is p.claims.return_value
    p.claims.assert_awaited_once_with(p.sources[1], 30)
    p.claims.retry.assert_not_called()


async def test_quota_denial_only_after_exact_abort_defers_then_continues_next_candidate(original):
    p = original
    p.store.claim_candidates.return_value.append(("job2", "session2"))
    p.claims.source.side_effect = p.sources
    proof = SimpleNamespace(state=State.ABORTED, execution=object())
    p.protocol_store.lookup.side_effect = [None, proof, None]
    p.claims.side_effect = [InterpretationClaimQuotaDenied("original quota denied"), object()]
    assert await p.adapter.claim(30) is not None
    p.adapter._defer.assert_awaited_once_with(p.sources[0], 30, proof.execution)
    p.claims.retry.assert_not_called()
    assert p.claims.await_count == 2


async def test_lost_quota_abort_proof_cannot_modify_job(original):
    p = original
    p.protocol_store.lookup.side_effect = [None, SimpleNamespace(state=State.PREPARED)]
    p.claims.side_effect = InterpretationClaimQuotaDenied("original quota denied")
    with pytest.raises(InterpretationClaimPending):
        await p.adapter.claim(30)
    p.adapter._defer.assert_not_called()


async def test_half_failure_does_not_run_job_only_quota_maintenance(original):
    p = original
    error = TransactionSendError("half unknown", Result(LocalOutcome.NOT_STARTED, ()))
    p.claims.side_effect = error
    with pytest.raises(TransactionSendError):
        await p.adapter.claim(30)
    p.adapter._defer.assert_not_called()
    p.claims.retry.assert_not_called()
    assert p.protocol_store.lookup.await_count == 1


async def test_original_attempts_exhausted_stops_scan_after_final_state(original):
    p = original
    p.store.claim_candidates.return_value.append(("job2", "session2"))
    p.sources[0].exhausted = True
    p.claims.return_value = None
    assert await p.adapter.claim(30) is None
    assert p.claims.source.await_count == 1


async def test_stale_source_does_not_receive_a_new_model_grant(original):
    p = original
    p.claims.source.side_effect = LeaseLost("original source no longer eligible")
    assert await p.adapter.claim(30) is None
    p.protocol_store.lookup.assert_not_called()
    p.claims.assert_not_called()


async def test_closed_admission_prevents_due_job_query_and_all_new_work(original):
    p = original
    p.runtime.stats.return_value = {"ready": False}
    with pytest.raises(TransactionAdmissionClosed):
        await p.adapter.claim(30)
    assert not p.events
    p.store.claim_candidates.assert_not_called()


async def test_original_lease_evidence_and_finalization_keep_their_separate_owners(original):
    p = original
    claim, result = object(), object()
    await p.adapter.renew(claim, 30)
    assert await p.adapter.evidence(claim) is p.store.evidence.return_value
    await p.adapter.finish(claim, result)
    p.store.renew.assert_awaited_once_with(claim, 30)
    p.adapter._finish.assert_awaited_once_with(claim, result)
    p.claims.assert_not_called()
