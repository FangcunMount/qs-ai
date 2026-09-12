from dataclasses import replace
from datetime import timedelta

import pytest

from qs_ai.domain.evaluation.actions import CandidateProgress, ExecutionResult, SlotProgress
from qs_ai.domain.evaluation.closure import ClosureTransition, validate_closed_inventory
from qs_ai.domain.evaluation.failure import ClassifiedFailure
from qs_ai.domain.evaluation.resolution import ResultUnknownResolution
from qs_ai.infrastructure.qs_server.evaluation_policies import load_execution_policy
from qs_ai.infrastructure.qs_server.evaluation_suite import V6
from qs_ai.infrastructure.qs_server.preflight import run_preflight
from tests.test_quality_gates import AT, complete


def inventory():
    values = complete()
    start = AT - timedelta(seconds=10)
    closed = values["semantics"][-1].finished_at
    return dict(
        created_at=start,
        requested_by="user:42",
        transitions=(
            ClosureTransition("", "requested", "user:42", "evaluation_requested", start),
            ClosureTransition("requested", "collecting", "user:42", "evaluation_started", start),
            ClosureTransition(
                "collecting",
                "awaiting_review",
                "worker:1",
                "candidate_evidence_complete",
                closed,
                (values["semantics"][-1].execution_id,),
            ),
        ),
        preflight=run_preflight(V6, start),
        slots=tuple(
            SlotProgress(
                c.case_id,
                c.slot_ordinal,
                (ExecutionResult("succeeded"),),
                CandidateProgress(c.evidence.candidate_id, True, (ExecutionResult("succeeded"),)),
            )
            for c in values["candidates"]
        ),
        generations=values["generations"],
        semantics=values["semantics"],
        resolutions=(),
        policy=load_execution_policy(),
    )


def test_closed_inventory_binds_preflight_times_slots_and_completion():
    value = inventory()
    assert validate_closed_inventory(**value) == value["semantics"][-1].finished_at


@pytest.mark.parametrize(
    "case",
    [
        "missing_slot",
        "history",
        "early_close",
        "early_preflight",
        "wrong_closing_execution",
        "overlap_preflight",
    ],
)
def test_incomplete_or_inconsistent_closure_is_rejected(case):
    value = inventory()
    if case == "missing_slot":
        value["slots"] = value["slots"][:-1]
    elif case == "history":
        value["transitions"] = value["transitions"][1:]
    elif case == "early_close":
        value["transitions"] = (
            *value["transitions"][:-1],
            replace(value["transitions"][-1], at=AT),
        )
    elif case == "early_preflight":
        value["preflight"] = replace(
            value["preflight"], evaluated_at=value["created_at"] - timedelta(seconds=1)
        )
    elif case == "wrong_closing_execution":
        value["transitions"] = (
            *value["transitions"][:-1],
            replace(value["transitions"][-1], evidence_refs=("generation:0",)),
        )
    elif case == "overlap_preflight":
        value["preflight"] = replace(value["preflight"], case_id=value["slots"][0].case_id)
    with pytest.raises(ValueError):
        validate_closed_inventory(**value)


def unknown_inventory():
    value = inventory()
    failure = ClassifiedFailure(
        "generation_execution",
        "result_unknown",
        "response_lost",
        False,
        True,
        "manual_acknowledgement",
        "响应未知",
        ("unknown:1",),
    )
    first = replace(
        value["generations"][0],
        execution_id="unknown:1",
        invocation_id="unknown-call:1",
        status="result_unknown",
        started_at=AT - timedelta(seconds=6),
        finished_at=AT - timedelta(seconds=5),
        receipt=None,
        raw_output=b"",
        normalized_output=b"",
        normalized_fingerprint="",
        failure=failure,
    )
    recovered = replace(value["generations"][0], execution_ordinal=2)
    value["generations"] = (first, recovered, *value["generations"][1:])
    resolution = ResultUnknownResolution(
        first.execution_id,
        "authorize_replacement",
        "user:42",
        "已核对未知调用",
        True,
        AT - timedelta(seconds=4),
    )
    value["resolutions"] = (resolution,)
    value["transitions"] = (
        *value["transitions"][:2],
        ClosureTransition(
            "collecting",
            "blocked",
            "worker:1",
            "result_unknown_requires_review",
            first.finished_at,
            (first.execution_id,),
        ),
        ClosureTransition(
            "blocked",
            "collecting",
            resolution.actor,
            "manual_recovery_approved",
            resolution.resolved_at,
            (first.execution_id,),
        ),
        value["transitions"][-1],
    )
    value["slots"] = (
        replace(
            value["slots"][0],
            generation=(
                ExecutionResult("result_unknown", failure, True),
                ExecutionResult("succeeded"),
            ),
        ),
        *value["slots"][1:],
    )
    return value


def test_resolved_unknown_requires_original_call_resolution_and_recovery_history():
    value = unknown_inventory()
    assert validate_closed_inventory(**value) == value["semantics"][-1].finished_at


@pytest.mark.parametrize(
    "case",
    ["missing_resolution", "early_replacement", "missing_audit", "unapproved", "over_budget"],
)
def test_later_success_cannot_hide_invalid_recovery(case):
    value = unknown_inventory()
    if case == "missing_resolution":
        value["resolutions"] = ()
    elif case == "early_replacement":
        values = list(value["generations"])
        values[1] = replace(values[1], started_at=AT - timedelta(seconds=5))
        value["generations"] = tuple(values)
    elif case == "missing_audit":
        value["transitions"] = (*value["transitions"][:2], value["transitions"][-1])
    elif case == "unapproved":
        slot = value["slots"][0]
        value["slots"] = (
            replace(
                slot,
                generation=(
                    replace(slot.generation[0], replacement_authorized=False),
                    slot.generation[1],
                ),
            ),
            *value["slots"][1:],
        )
    elif case == "over_budget":
        slot = value["slots"][0]
        value["slots"] = (
            replace(slot, generation=(*slot.generation, ExecutionResult("succeeded"))),
            *value["slots"][1:],
        )
    with pytest.raises(ValueError):
        validate_closed_inventory(**value)
