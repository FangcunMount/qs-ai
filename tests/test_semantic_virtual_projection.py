"""Readonly virtual terminal rows must match the original post-insert review projection."""

from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.infrastructure.persistence.mysql import evaluation_semantic as original
from qs_ai.infrastructure.qs_server.semantic_output import SemanticResult
from tests.test_generation_completion import completion as generation
from tests.test_semantic_completion import completion as semantic


def fixture(monkeypatch, *, staged, await_review):
    run_id, generated, value = uuid4(), generation(), semantic()
    candidate = dict(id=value.candidate_id, review_ready=True)
    progress = dict(
        status="collecting", transitions=[], preflight=dict(status="passed", case_id="")
    )
    run = dict(progress_json=progress)
    policy = object()
    accepted = original.SemanticAcceptance(
        policy,
        generated,
        candidate,
        SemanticResult("v1", (("score", 1),), "Original rationale", (), value.output_fingerprint),
        dict(status=value.status),
        dict(slots=[]),
    )
    current = dict(execution_id=generated.execution_id, candidate_json=dict(review_ready=False))
    other = dict(execution_id="generation:other", candidate_json=dict(review_ready=False))
    generations = [current, other] if staged else [{**current, "candidate_json": candidate}, other]
    semantics = [] if staged else [original.semantic_terminal_values(run_id, value, accepted)]
    dispatches = [dict(invocation_id=value.invocation_id)]

    def rows(value):
        result = Mock()
        result.mappings.return_value.all.return_value = value
        return result

    db = SimpleNamespace(
        execute=AsyncMock(side_effect=[rows(generations), rows(semantics), rows(dispatches)]),
        commit=AsyncMock(),
        rollback=AsyncMock(),
    )
    projected = Mock(return_value=("original slots",))
    action = Mock(return_value=SimpleNamespace(kind="await_review" if await_review else "continue"))
    monkeypatch.setattr(original, "project_slots", projected)
    monkeypatch.setattr(original, "next_action", action)
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("staged", [False, True])
@pytest.mark.parametrize("lock", [False, True])
@pytest.mark.parametrize("await_review", [False, True])
async def test_virtual_and_post_insert_projection_keep_original_rows_and_no_commit(
    monkeypatch, staged, lock, await_review
):
    p = fixture(monkeypatch, staged=staged, await_review=await_review)
    actual = await original.project_semantic_progress(
        p.db,
        p.run_id,
        p.value,
        " worker:first ",
        p.run,
        p.accepted,
        candidate_claim=False,
        lock=lock,
        before_insert=staged,
    )
    assert actual["status"] == ("awaiting_review" if await_review else "collecting")
    assert len(actual["transitions"]) == await_review
    if await_review:
        assert actual["transitions"][0]["evidence_refs"] == [p.value.execution_id]
        assert actual["transitions"][0]["actor"] == "worker:first"
        assert actual["transitions"][0]["at"] == p.value.finished_at.isoformat()
    args = p.projected.call_args.args
    assert args[1][0]["candidate_json"] == p.candidate
    assert args[1][1] is p.other
    assert len(args[3]) == 1
    assert args[3][0] == original.semantic_terminal_values(p.run_id, p.value, p.accepted)
    assert args[3][0]["raw_output"] == p.value.raw_output
    assert args[3][0]["result_json"] == asdict(p.accepted.result)
    assert p.current["candidate_json"] == dict(review_ready=False)
    assert p.semantics == ([] if staged else [args[3][0]])
    assert p.progress["status"] == "collecting" and p.progress["transitions"] == []
    assert len(p.db.execute.await_args_list) == 3
    assert all(c.args[0].is_select for c in p.db.execute.await_args_list)
    assert all(
        (c.args[0]._for_update_arg is not None) == lock for c in p.db.execute.await_args_list
    )
    p.db.commit.assert_not_called()
    p.db.rollback.assert_not_called()


@pytest.mark.parametrize(
    "damage", ["missing_generation", "duplicate_generation", "existing_terminal"]
)
async def test_virtual_projection_rejects_missing_or_repeated_terminal_source_before_any_write(
    monkeypatch, damage
):
    p = fixture(monkeypatch, staged=True, await_review=True)
    if damage == "missing_generation":
        p.generations.clear()
    elif damage == "duplicate_generation":
        p.generations.append(dict(p.current))
    else:
        p.semantics.append(dict(execution_id=p.value.execution_id))
    with pytest.raises(CheckpointConflict):
        await original.project_semantic_progress(
            p.db,
            p.run_id,
            p.value,
            "worker:first",
            p.run,
            p.accepted,
            candidate_claim=False,
            lock=False,
            before_insert=True,
        )
    p.projected.assert_not_called()
    p.action.assert_not_called()
    assert all(c.args[0].is_select for c in p.db.execute.await_args_list)
    p.db.commit.assert_not_called()


@pytest.mark.parametrize("branch", ["candidate_owner", "cancellation"])
async def test_virtual_read_never_takes_over_original_candidate_or_cancel_owner(
    monkeypatch, branch
):
    p = fixture(monkeypatch, staged=True, await_review=True)
    if branch == "cancellation":
        p.progress["cancel_requested"] = dict(source_version=8, version=9)
    actual = await original.project_semantic_progress(
        p.db,
        p.run_id,
        p.value,
        "worker:first",
        p.run,
        p.accepted,
        candidate_claim=branch == "candidate_owner",
        lock=False,
        before_insert=True,
    )
    assert actual["status"] == "collecting" and actual["transitions"] == []
    p.db.execute.assert_not_called()
    p.projected.assert_not_called()
    p.action.assert_not_called()
    p.db.commit.assert_not_called()
