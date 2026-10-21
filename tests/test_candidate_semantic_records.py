"""Exact virtual terminal preserves all other ownership and original bytes without writes."""

from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.domain.evaluation.failure import ClassifiedFailure
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_records import (
    semantic_terminal_records,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import encode
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import SlotClaim
from tests.test_semantic_readonly_source import source
from tests.test_semantic_virtual_projection import fixture as semantic_fixture


def fixture(monkeypatch, *, success=True, other=True):
    p = semantic_fixture(monkeypatch, staged=True, await_review=False)
    p.generated = replace(
        p.generated,
        invocation_id="generation:invocation",
        receipt=replace(p.generated.receipt, invocation_id="generation:invocation"),
    )
    p.value = replace(p.value, candidate_output_fingerprint=p.generated.normalized_fingerprint)
    if not success:
        p.value = replace(
            p.value,
            status="result_unknown",
            failure=ClassifiedFailure(
                "semantic_evaluation",
                "result_unknown",
                "provider_result_unknown",
                False,
                True,
                "manual_acknowledgement",
                "Original unknown response",
                (p.value.execution_id,),
            ),
        )
    candidate = dict(
        id=p.value.candidate_id,
        generation_execution_id=p.generated.execution_id,
        normalized_output_fingerprint=p.generated.normalized_fingerprint,
        review_ready=False,
    )
    accepted = replace(
        p.accepted,
        generated=p.generated,
        evidence=dict(status=p.value.status),
        candidate={**candidate, "review_ready": True} if success else candidate,
        result=p.accepted.result if success else None,
    )
    cp = replace(source().cp, case_id=p.generated.case_id, slot_ordinal=p.generated.slot_ordinal)
    claim = SlotClaim(p.run_id, 3, cp)
    other_claim = replace(
        claim,
        checkpoint=replace(
            cp,
            case_id="case:other",
            kind="generation",
            candidate_id="",
            execution_id="execution:other",
            invocation_id="invocation:other",
            phase="prepared",
            dispatch_started_at=None,
        ),
    )
    active = (claim, other_claim) if other else (claim,)
    dispatches = [
        dict(
            run_id=str(p.run_id),
            invocation_id=cp.invocation_id,
            execution_id=cp.execution_id,
            kind=cp.kind,
            case_id=cp.case_id,
            slot_ordinal=cp.slot_ordinal,
            candidate_id=cp.candidate_id,
            checkpoint_json=encode(cp),
        )
    ]
    current = dict(
        run_id=str(p.run_id),
        execution_id=p.generated.execution_id,
        invocation_id=p.generated.invocation_id,
        candidate_id=p.value.candidate_id,
        candidate_json=candidate,
        raw_output=b"original-generation-bytes",
    )
    generations, semantics = [current], []
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("success", [False, True])
@pytest.mark.parametrize("other", [False, True])
def test_virtual_semantic_terminal_keeps_candidate_other_claim_and_first_bytes(
    monkeypatch, success, other
):
    p = fixture(monkeypatch, success=success, other=other)
    result = semantic_terminal_records(
        p.p.run_id,
        p.claim,
        p.active,
        p.p.value,
        p.accepted,
        dispatches=p.dispatches,
        generations=p.generations,
        semantics=p.semantics,
    )
    assert result.active == ((p.other_claim,) if other else ())
    assert result.dispatches == tuple(p.dispatches)
    assert result.generations[0]["candidate_json"] == p.accepted.candidate
    assert result.generations[0]["raw_output"] == b"original-generation-bytes"
    assert len(result.semantics) == 1 and result.semantics[0]["raw_output"] == p.p.value.raw_output
    assert result.semantics[0]["invocation_id"] == p.claim.checkpoint.invocation_id
    assert p.current["candidate_json"]["review_ready"] is False and p.semantics == []
    assert "original-generation-bytes" not in repr(result)


@pytest.mark.parametrize(
    "damage",
    [
        "foreign_run",
        "missing_claim",
        "duplicate_claim",
        "foreign_active",
        "duplicate_slot",
        "missing_dispatch",
        "changed_dispatch",
        "foreign_record",
        "missing_generation",
        "duplicate_generation",
        "wrong_candidate",
        "already_reviewed",
        "existing_execution",
        "existing_invocation",
    ],
)
def test_virtual_semantic_terminal_rejects_ambiguous_or_changed_sources_before_projection(
    monkeypatch, damage
):
    p = fixture(monkeypatch)
    run_id, active = p.p.run_id, p.active
    if damage == "foreign_run":
        run_id = uuid4()
    elif damage == "missing_claim":
        active = (p.other_claim,)
    elif damage == "duplicate_claim":
        active = (*active, p.claim)
    elif damage == "foreign_active":
        active = (p.claim, replace(p.other_claim, run_id=uuid4()))
    elif damage == "duplicate_slot":
        active = (p.claim, replace(p.claim, version=4))
    elif damage == "missing_dispatch":
        p.dispatches.clear()
    elif damage == "changed_dispatch":
        p.dispatches[0]["execution_id"] = "foreign:dispatch"
    elif damage == "foreign_record":
        p.current["run_id"] = str(uuid4())
    elif damage == "missing_generation":
        p.generations.clear()
    elif damage == "duplicate_generation":
        p.generations.append(dict(p.current))
    elif damage == "wrong_candidate":
        p.current["candidate_id"] = "candidate:foreign"
    elif damage == "already_reviewed":
        p.current["candidate_json"] = {**p.candidate, "review_ready": True}
    elif damage == "existing_execution":
        p.semantics.append(
            dict(
                run_id=str(run_id),
                execution_id=p.p.value.execution_id,
                invocation_id="another:invocation",
            )
        )
    elif damage == "existing_invocation":
        p.semantics.append(
            dict(
                run_id=str(run_id),
                execution_id="another:execution",
                invocation_id=p.p.value.invocation_id,
            )
        )
    with pytest.raises(CheckpointConflict):
        semantic_terminal_records(
            run_id,
            p.claim,
            active,
            p.p.value,
            p.accepted,
            dispatches=p.dispatches,
            generations=p.generations,
            semantics=p.semantics,
        )
