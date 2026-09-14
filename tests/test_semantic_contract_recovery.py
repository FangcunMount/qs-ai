from dataclasses import replace
from datetime import timedelta

import pytest

from qs_ai.domain.evaluation.actions import ExecutionResult
from qs_ai.domain.evaluation.closure import ClosureTransition, validate_closed_inventory
from qs_ai.domain.evaluation.contract_recovery import ContractRecovery, validate_recoveries
from qs_ai.domain.evaluation.failure import ClassifiedFailure
from tests.test_evaluation_closure import inventory
from tests.test_quality_gates import AT


def recovered_inventory():
    data = inventory()
    accepted = data["semantics"][0]
    failure = ClassifiedFailure(
        "semantic_evaluation",
        "semantic_execution",
        "semantic_decision_contract_invalid",
        True,
        False,
        "retry_semantic",
        "Semantic decision evidence invalid",
        ("semantic:failed",),
    )
    failed = replace(
        accepted,
        execution_id="semantic:failed",
        status="failed",
        failure=failure,
        started_at=AT - timedelta(seconds=3),
        finished_at=AT - timedelta(seconds=2),
    )
    value = ContractRecovery(
        failed.execution_id,
        failed.candidate_id,
        failed.candidate_output_fingerprint,
        failed.output_fingerprint,
        "operator:root",
        "授权有限重试，保留原策略和失败证据",
        AT - timedelta(seconds=1),
        True,
    )
    data["semantics"] = (failed, replace(accepted, execution_ordinal=2), *data["semantics"][1:])
    data["contract_recoveries"] = (value,)
    data["transitions"] = (
        *data["transitions"][:2],
        ClosureTransition(
            "collecting",
            "blocked",
            "worker:1",
            "semantic_recovery_not_allowed",
            failed.finished_at,
            (failed.execution_id,),
        ),
        ClosureTransition(
            "blocked",
            "collecting",
            value.actor,
            "semantic_contract_recovery_approved",
            value.resolved_at,
            (failed.execution_id,),
        ),
        data["transitions"][-1],
    )
    first = data["slots"][0]
    candidate = replace(
        first.candidate,
        semantic=(
            ExecutionResult("failed", failure, contract_recovery_authorized=True),
            ExecutionResult("succeeded"),
        ),
    )
    data["slots"] = (replace(first, candidate=candidate), *data["slots"][1:])
    return data


def test_closed_inventory_requires_audited_exception_and_retains_original_policy():
    data = recovered_inventory()
    policy = data["policy"].definition_json
    assert validate_closed_inventory(**data) == data["semantics"][-1].finished_at
    assert data["policy"].definition_json == policy
    assert not data["policy"].allows_automatic_semantic_recovery(data["semantics"][0].failure)


@pytest.mark.parametrize(
    "case",
    [
        "missing_auth",
        "missing_block",
        "missing_resume",
        "actor",
        "time",
        "budget",
        "duplicate",
        "fingerprint",
        "instruction",
        "ack",
        "successful_quality_failure",
    ],
)
def test_unapproved_or_mismatched_recovery_cannot_close(case):
    data = recovered_inventory()
    auth = data["contract_recoveries"][0]
    with pytest.raises(ValueError):
        if case == "missing_auth":
            data["contract_recoveries"] = ()
        if case == "missing_block":
            data["transitions"] = (*data["transitions"][:2], *data["transitions"][3:])
        if case == "missing_resume":
            data["transitions"] = (*data["transitions"][:3], *data["transitions"][4:])
        if case == "actor":
            auth = replace(auth, actor="operator:other")
        if case == "time":
            auth = replace(auth, resolved_at=AT + timedelta(seconds=1))
        if case == "budget":
            data["semantics"] = (
                replace(data["semantics"][0], execution_ordinal=2),
                *data["semantics"][1:],
            )
        if case == "duplicate":
            data["contract_recoveries"] = (auth, auth)
        if case == "fingerprint":
            auth = replace(auth, output_fingerprint="sha256:" + "0" * 64)
        if case == "instruction":
            auth = replace(auth, instruction_fingerprint="sha256:" + "0" * 64)
        if case == "ack":
            auth = replace(auth, acknowledged_policy_exception_and_cost=False)
        if case == "successful_quality_failure":
            data["semantics"] = (
                replace(data["semantics"][0], status="succeeded", failure=None),
                *data["semantics"][1:],
            )
        if case not in ("missing_auth", "duplicate"):
            data["contract_recoveries"] = (auth,)
        validate_closed_inventory(**data)


def test_contract_authorization_cannot_be_attached_to_unknown_or_generation_failure():
    data = recovered_inventory()
    failure = data["semantics"][0].failure
    with pytest.raises(ValueError):
        ExecutionResult("result_unknown", failure, contract_recovery_authorized=True)
    for code in ("semantic_provider_rate_limited", "provider_result_unknown", "quality_failure"):
        wrong = replace(data["semantics"][0], failure=replace(failure, code=code))
        with pytest.raises(ValueError):
            validate_recoveries(data["contract_recoveries"], (wrong,), data["policy"])
