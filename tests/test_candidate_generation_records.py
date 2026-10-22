"""Proposed generation records preserve current ownership separation and original bytes."""

from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.domain.evaluation.failure import ClassifiedFailure
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_records import (
    generation_terminal_records,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import encode
from qs_ai.infrastructure.persistence.mysql.evaluation_completions import GenerationAcceptance
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import SlotClaim
from tests.test_generation_completion import completion
from tests.test_semantic_readonly_source import source


def fixture(*, unknown=False, other=True):
    run_id, value = uuid4(), completion()
    if unknown:
        value = replace(
            value,
            status="result_unknown",
            failure=ClassifiedFailure(
                "generation_execution",
                "result_unknown",
                "provider_result_unknown",
                False,
                True,
                "manual_acknowledgement",
                "Original unknown result",
                (value.execution_id,),
            ),
        )
    cp = replace(
        source().cp,
        kind="generation",
        candidate_id="",
        case_id=value.case_id,
        slot_ordinal=value.slot_ordinal,
        execution_id=value.execution_id,
        invocation_id=value.invocation_id,
    )
    claim = SlotClaim(run_id, 3, cp)
    other_claim = replace(
        claim,
        checkpoint=replace(
            cp,
            slot_ordinal=2,
            execution_id="generation:other",
            invocation_id="invocation:other",
            phase="prepared",
            dispatch_started_at=None,
        ),
    )
    active = (claim, other_claim) if other else (claim,)
    dispatches = [
        dict(
            run_id=str(run_id),
            execution_id=cp.execution_id,
            invocation_id=cp.invocation_id,
            kind=cp.kind,
            case_id=cp.case_id,
            slot_ordinal=cp.slot_ordinal,
            candidate_id="",
            checkpoint_json=encode(cp),
        )
    ]
    candidate = (
        None
        if unknown
        else dict(
            id="candidate:first",
            generation_execution_id=value.execution_id,
            normalized_output_fingerprint=value.normalized_fingerprint,
            review_ready=False,
        )
    )
    accepted = GenerationAcceptance(object(), candidate, dict(status=value.status), {})
    generations, semantics = [], []
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("unknown", [False, True])
@pytest.mark.parametrize("other", [False, True])
def test_generation_overlay_keeps_first_terminal_candidate_bytes_and_other_claims(unknown, other):
    p = fixture(unknown=unknown, other=other)
    actual = generation_terminal_records(
        p.run_id,
        p.claim,
        p.active,
        p.value,
        p.accepted,
        dispatches=p.dispatches,
        generations=p.generations,
        semantics=p.semantics,
    )
    assert actual.active == ((p.other_claim,) if other else ())
    assert actual.generations[-1]["raw_output"] == p.value.raw_output
    assert actual.generations[-1]["candidate_json"] == p.candidate
    assert actual.generations[-1]["candidate_id"] == (None if unknown else "candidate:first")
    assert actual.dispatches == tuple(p.dispatches) and actual.semantics == ()
    assert p.generations == [] and p.semantics == []


@pytest.mark.parametrize(
    "damage",
    [
        "foreign_run",
        "missing_claim",
        "duplicate_claim",
        "missing_dispatch",
        "wrong_dispatch",
        "existing_execution",
        "existing_invocation",
        "foreign_record",
        "candidate_mismatch",
        "review_ready",
    ],
)
def test_generation_overlay_refuses_ambiguous_or_changed_original_source(damage):
    p = fixture()
    run_id, active = p.run_id, p.active
    if damage == "foreign_run":
        run_id = uuid4()
    elif damage == "missing_claim":
        active = (p.other_claim,)
    elif damage == "duplicate_claim":
        active = (*active, p.claim)
    elif damage == "missing_dispatch":
        p.dispatches.clear()
    elif damage == "wrong_dispatch":
        p.dispatches[0]["execution_id"] = "foreign:execution"
    elif damage == "foreign_record":
        p.dispatches[0]["run_id"] = str(uuid4())
    elif damage == "candidate_mismatch":
        p.candidate["generation_execution_id"] = "foreign:execution"
    elif damage == "review_ready":
        p.candidate["review_ready"] = True
    else:
        p.generations.append(
            dict(
                run_id=str(run_id),
                execution_id=p.value.execution_id
                if damage == "existing_execution"
                else "another:execution",
                invocation_id=p.value.invocation_id
                if damage == "existing_invocation"
                else "another:invocation",
            )
        )
    with pytest.raises(CheckpointConflict):
        generation_terminal_records(
            run_id,
            p.claim,
            active,
            p.value,
            p.accepted,
            dispatches=p.dispatches,
            generations=p.generations,
            semantics=p.semantics,
        )
