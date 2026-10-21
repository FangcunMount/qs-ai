"""Original evaluation finalization keeps evidence, root ownership and commit ordering."""

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointState
from qs_ai.domain.evaluation.failure import ClassifiedFailure
from qs_ai.infrastructure.persistence.mysql import evaluation_step as module


@pytest.fixture
def original(monkeypatch):
    events = []
    root = SimpleNamespace(is_active=True)
    db = AsyncMock(spec=AsyncSession)
    db.get_transaction = Mock(return_value=root)
    db.in_nested_transaction = Mock(return_value=False)

    async def connection(**kwargs):
        assert kwargs == {"execution_options": {"isolation_level": "REPEATABLE READ"}}
        events.append("isolation")

    async def commit():
        events.append("commit")

    db.connection.side_effect, db.commit.side_effect = connection, commit

    @asynccontextmanager
    async def opened():
        events.append("open")
        try:
            yield db
        finally:
            events.append("close")

    tx = SimpleNamespace(open=opened)
    run_id, at = uuid4(), datetime(2026, 10, 22, tzinfo=UTC)
    cp = SimpleNamespace(
        kind="generation",
        case_id="case:first",
        slot_ordinal=1,
        execution_ordinal=1,
        candidate_id="candidate:first",
    )
    prepared = SimpleNamespace(
        state=CheckpointState(run_id, 5, None),
        cp=cp,
        at=at,
        invocation_id="invocation:first",
        execution_id="execution:first",
        candidate_fingerprint="sha256:" + "a" * 64,
        assertions=(),
        release=Mock(),
        suite=Mock(),
        route=Mock(),
        schema={},
        max_output_characters=100000,
        semantic=None,
        claim=None,
    )
    generation, semantic = AsyncMock(), AsyncMock()

    async def accepted(*args, **kwargs):
        assert args[0] is db and args[1] == run_id and args[2:5] == (5, 1, "worker:first")
        events.append("business")
        return CheckpointState(run_id, 6, None)

    generation.side_effect = semantic.side_effect = accepted
    monkeypatch.setattr(module, "complete_evaluated_generation", generation)
    monkeypatch.setattr(module, "complete_semantic", semantic)
    monkeypatch.setattr(module, "emit", lambda *args, **kwargs: events.append("emit"))
    binding = SimpleNamespace(validate=AsyncMock())
    monkeypatch.setattr(module, "bind", lambda _: binding)
    failure = ClassifiedFailure(
        "generation_execution",
        "result_unknown",
        "provider_result_unknown",
        False,
        True,
        "manual_acknowledgement",
        "Original provider result unknown",
        ("execution:first",),
    )
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("kind", ["generation", "semantic"])
@pytest.mark.parametrize("rejected", [False, True])
async def test_original_finish_keeps_unknown_evidence_and_one_root_commit(original, kind, rejected):
    p = original
    p.cp.kind = kind
    accept = p.generation if kind == "generation" else p.semantic
    if rejected:
        accept.side_effect = ConnectionError("original business acceptance refused")
    if rejected:
        with pytest.raises(ConnectionError):
            await module.finish_step(
                p.tx,
                p.run_id,
                1,
                "worker:first",
                Mock(),
                Mock(),
                p.prepared,
                None,
                p.failure,
                clock=lambda: p.at,
            )
        assert p.events == ["open", "isolation", "close"]
        p.db.commit.assert_not_called()
    else:
        result = await module.finish_step(
            p.tx,
            p.run_id,
            1,
            "worker:first",
            Mock(),
            Mock(),
            p.prepared,
            None,
            p.failure,
            clock=lambda: p.at,
        )
        assert result.version == 6
        assert p.events == ["open", "isolation", "business", "commit", "close", "emit"]
        completion = accept.await_args.args[5]
        assert completion.status == "result_unknown" and completion.failure == p.failure
        assert completion.provider_call_count == 1 and completion.finished_at == p.at
        if kind == "generation":
            assert accept.await_args.kwargs["candidate_id"] == ""
        p.db.commit.assert_awaited_once()


@pytest.mark.parametrize("kind", ["generation", "semantic"])
async def test_original_completed_candidate_replay_does_not_commit_or_emit(
    original, monkeypatch, kind
):
    from qs_ai.infrastructure.persistence.mysql import evaluation_candidate_completion as candidate

    p = original
    p.cp.kind, p.prepared.claim = kind, object()
    completed = CheckpointState(p.run_id, 7, None)
    monkeypatch.setattr(candidate, "completed_claim_state", AsyncMock(return_value=completed))
    result = await module.finish_step(
        p.tx,
        p.run_id,
        1,
        "worker:first",
        Mock(),
        Mock(),
        p.prepared,
        None,
        p.failure,
        clock=lambda: p.at,
    )
    assert result == completed and p.events == ["open", "isolation", "close"]
    p.db.commit.assert_not_called()
    p.generation.assert_not_called()
    p.semantic.assert_not_called()


@pytest.mark.parametrize("kind", ["generation", "semantic"])
async def test_borrowed_completion_stages_original_evidence_without_root_ownership(original, kind):
    p = original
    p.cp.kind = kind
    value = await module.prepare_step_completion(
        p.prepared, None, p.failure, Mock(), clock=lambda: p.at
    )
    assert p.events == []
    assert "Original provider result unknown" not in repr(value)
    state, applied = await module.finish_step_in(
        p.db, p.run_id, 1, "worker:first", Mock(), Mock(), value
    )
    assert state.version == 6 and applied is True and p.events == ["business"]
    assert p.binding.validate.await_count == 2
    p.db.connection.assert_not_called()
    p.db.commit.assert_not_called()
    p.db.rollback.assert_not_called()
    assert p.db.get_transaction() is p.root and p.root.is_active


@pytest.mark.parametrize("fault", ["handle", "nested", "ended", "inactive"])
async def test_borrowed_completion_rejects_wrong_root_before_any_business(original, fault):
    from reliable_messaging.transaction import BindingError

    p = original
    value = await module.prepare_step_completion(
        p.prepared, None, p.failure, Mock(), clock=lambda: p.at
    )
    if fault == "nested":
        p.db.in_nested_transaction.return_value = True
    elif fault == "ended":
        p.db.get_transaction.return_value = None
    elif fault == "inactive":
        p.root.is_active = False
    with pytest.raises(BindingError):
        await module.finish_step_in(
            object() if fault == "handle" else p.db,
            p.run_id,
            1,
            "worker:first",
            Mock(),
            Mock(),
            value,
        )
    assert p.events == []
    p.generation.assert_not_called()
    p.semantic.assert_not_called()


async def test_borrowed_completion_detects_replaced_original_root_without_commit_claim(original):
    from reliable_messaging.transaction import BindingError

    p = original
    value = await module.prepare_step_completion(
        p.prepared, None, p.failure, Mock(), clock=lambda: p.at
    )

    async def replace_root(*args, **kwargs):
        p.db.get_transaction.return_value = SimpleNamespace(is_active=True)
        return CheckpointState(p.run_id, 6, None)

    p.generation.side_effect = replace_root
    with pytest.raises(BindingError):
        await module.finish_step_in(p.db, p.run_id, 1, "worker:first", Mock(), Mock(), value)
    p.db.commit.assert_not_called()


async def test_candidate_identity_and_evidence_are_prepared_before_entering_original_root(
    original, monkeypatch
):
    from qs_ai.domain.evaluation.completion import ProviderReceipt

    p = original
    receipt = ProviderReceipt("invocation:first", "request:first", "original", "model", 1, 1, 100)
    evidence = SimpleNamespace(receipt=receipt, raw=b"{}", normalized=b"{}", failure=None)
    monkeypatch.setattr(module, "response_evidence", Mock(return_value=evidence))
    candidate_id = uuid4()
    monkeypatch.setattr(module, "uuid4", Mock(return_value=candidate_id))
    value = await module.prepare_step_completion(
        p.prepared, Mock(), None, Mock(), clock=lambda: p.at
    )
    assert p.events == [] and value.candidate_id == str(candidate_id)

    def no_identity():
        raise AssertionError("Candidate identity must not be generated inside borrowed root")

    monkeypatch.setattr(module, "uuid4", no_identity)
    await module.finish_step_in(p.db, p.run_id, 1, "worker:first", Mock(), Mock(), value)
    assert p.generation.await_args.kwargs["candidate_id"] == str(candidate_id)


async def test_explicit_first_candidate_identity_does_not_generate_another_in_completion(
    original, monkeypatch
):
    from tests.test_generation_completion import completion

    p = original
    first = completion()
    evidence = SimpleNamespace(
        receipt=first.receipt,
        raw=first.raw_output,
        normalized=first.normalized_output,
        failure=None,
    )
    monkeypatch.setattr(module, "response_evidence", Mock(return_value=evidence))
    p.prepared.invocation_id = first.invocation_id
    p.prepared.execution_id = first.execution_id
    monkeypatch.setattr(
        module, "uuid4", Mock(side_effect=AssertionError("Original first ID must be reused"))
    )
    result = await module.prepare_step_completion(
        p.prepared, Mock(), None, Mock(), clock=lambda: p.at, candidate_id="candidate:first"
    )
    assert result.candidate_id == "candidate:first"
    assert result.completion.status == "succeeded"
    assert result.completion.raw_output == first.raw_output


@pytest.mark.parametrize("candidate", [True, 1, "", " bad", "x" * 129])
async def test_explicit_candidate_identity_requires_original_bounded_success_identity(
    original, monkeypatch, candidate
):
    from tests.test_generation_completion import completion

    p = original
    first = completion()
    monkeypatch.setattr(
        module,
        "response_evidence",
        Mock(
            return_value=SimpleNamespace(
                receipt=first.receipt,
                raw=first.raw_output,
                normalized=first.normalized_output,
                failure=None,
            )
        ),
    )
    p.prepared.invocation_id = first.invocation_id
    with pytest.raises(ValueError):
        await module.prepare_step_completion(
            p.prepared, Mock(), None, Mock(), clock=lambda: p.at, candidate_id=candidate
        )


async def test_unknown_generation_can_reuse_only_explicit_absence_of_candidate(
    original, monkeypatch
):
    p = original
    monkeypatch.setattr(
        module,
        "uuid4",
        Mock(side_effect=AssertionError("No candidate for unknown provider result")),
    )
    result = await module.prepare_step_completion(
        p.prepared, None, p.failure, Mock(), clock=lambda: p.at, candidate_id=""
    )
    assert result.candidate_id == "" and result.completion.status == "result_unknown"
    with pytest.raises(ValueError):
        await module.prepare_step_completion(
            p.prepared, None, p.failure, Mock(), clock=lambda: p.at, candidate_id="candidate:wrong"
        )


async def test_semantic_completion_cannot_accept_generation_candidate_override(original):
    p = original
    p.cp.kind = "semantic"
    with pytest.raises(ValueError):
        await module.prepare_step_completion(
            p.prepared, None, p.failure, Mock(), clock=lambda: p.at, candidate_id=""
        )
