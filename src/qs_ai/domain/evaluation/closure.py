"""Validate the closed execution inventory before calculating any release gate."""

import re
from dataclasses import dataclass
from datetime import datetime

from qs_ai.domain.evaluation.actions import SlotProgress, next_action
from qs_ai.domain.evaluation.completion import GenerationCompletion
from qs_ai.domain.evaluation.contract_recovery import ContractRecovery, validate_recoveries
from qs_ai.domain.evaluation.policy import ExecutionPolicy
from qs_ai.domain.evaluation.preflight import PreflightEvidence
from qs_ai.domain.evaluation.resolution import (
    ResultUnknownResolution,
    UnknownExecution,
    resolve_unknown,
)
from qs_ai.domain.evaluation.semantic_completion import SemanticCompletion


@dataclass(frozen=True)
class ClosureTransition:
    source: str
    target: str
    actor: str
    cause: str
    at: datetime
    evidence_refs: tuple[str, ...] = ()


def validate_closed_inventory(
    created_at: datetime,
    requested_by: str,
    transitions: tuple[ClosureTransition, ...],
    preflight: PreflightEvidence,
    slots: tuple[SlotProgress, ...],
    generations: tuple[GenerationCompletion, ...],
    semantics: tuple[SemanticCompletion, ...],
    resolutions: tuple[ResultUnknownResolution, ...],
    policy: ExecutionPolicy,
    contract_recoveries: tuple[ContractRecovery, ...] = (),
) -> datetime:
    """Return the proven closure time; dispatched/terminal binding is checked by the adapter."""
    if (
        created_at.tzinfo is None
        or created_at.utcoffset() is None
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:._/-]{0,127}", requested_by)
    ):
        raise ValueError("Valid creator and creation time required")
    if not all(
        isinstance(v, tuple) for v in (transitions, slots, generations, semantics, resolutions)
    ):
        raise ValueError("Immutable closed evidence required")
    if len(transitions) < 3 or transitions[0] != ClosureTransition(
        "", "requested", requested_by, "evaluation_requested", created_at
    ):
        raise ValueError("Creation audit differs from initial transition")
    executions: tuple[GenerationCompletion | SemanticCompletion, ...] = (*generations, *semantics)
    indexed = {e.execution_id: e for e in executions}
    if len(indexed) != len(executions):
        raise ValueError("Execution identities must be unique across stages")
    decisions = {r.execution_id: r for r in resolutions}
    validate_recoveries(contract_recoveries, semantics, policy)
    contract_decisions = {r.execution_id: r for r in contract_recoveries}
    state, previous = "requested", created_at
    allowed = {
        ("requested", "collecting"): "evaluation_started",
        ("collecting", "blocked"): "result_unknown_requires_review",
        ("blocked", "collecting"): "manual_recovery_approved",
        ("collecting", "awaiting_review"): "candidate_evidence_complete",
    }
    for transition in transitions[1:]:
        if (
            transition.at.tzinfo is None
            or transition.at.utcoffset() is None
            or transition.at < previous
            or transition.source != state
            or (
                allowed.get((state, transition.target)) != transition.cause
                and (state, transition.target, transition.cause)
                not in (
                    ("collecting", "blocked", "semantic_recovery_not_allowed"),
                    ("blocked", "collecting", "semantic_contract_recovery_approved"),
                )
            )
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:._/-]{0,127}", transition.actor)
        ):
            raise ValueError("Invalid closed Run transition chain")
        if transition.target == "blocked":
            if len(transition.evidence_refs) != 1:
                raise ValueError("Unknown transition requires its execution reference")
            value = indexed.get(transition.evidence_refs[0])
            if (
                value is None
                or (
                    value.status != "result_unknown"
                    if transition.cause == "result_unknown_requires_review"
                    else not isinstance(value, SemanticCompletion)
                    or value.execution_id not in contract_decisions
                )
                or value.finished_at != transition.at
            ):
                raise ValueError("Unknown transition differs from execution")
        elif transition.source == "blocked":
            if len(transition.evidence_refs) != 1:
                raise ValueError("Recovery transition requires its resolution reference")
            if transition.cause == "semantic_contract_recovery_approved":
                contract_decision = contract_decisions.get(transition.evidence_refs[0])
                if contract_decision is None or (
                    contract_decision.actor,
                    contract_decision.resolved_at,
                ) != (
                    transition.actor,
                    transition.at,
                ):
                    raise ValueError("Recovery transition differs from contract authorization")
            else:
                decision = decisions.get(transition.evidence_refs[0])
                if decision is None or (
                    decision.actor,
                    decision.resolved_at,
                    decision.decision,
                ) != (
                    transition.actor,
                    transition.at,
                    "authorize_replacement",
                ):
                    raise ValueError("Recovery transition differs from manual authorization")
        elif transition.target == "awaiting_review":
            if len(transition.evidence_refs) != 1:
                raise ValueError("Closure requires its final semantic execution")
            value = indexed.get(transition.evidence_refs[0])
            if (
                not isinstance(value, SemanticCompletion)
                or value.status != "succeeded"
                or value.finished_at != transition.at
            ):
                raise ValueError("Closure differs from accepted semantic evidence")
        state, previous = transition.target, transition.at
    if state != "awaiting_review" or not transitions[1].at <= preflight.evaluated_at <= previous:
        raise ValueError("Complete preflight and unique closure required")
    if any(slot.case_id == preflight.case_id for slot in slots):
        raise ValueError("Preflight overlaps a generation case")
    if any(
        not preflight.evaluated_at <= e.started_at <= e.finished_at <= previous for e in executions
    ):
        raise ValueError("Execution is outside the preflight and closure interval")
    unknowns = tuple(
        UnknownExecution(e.execution_id, kind, e.execution_ordinal, e.finished_at)
        for kind, records in (("generation", generations), ("semantic", semantics))
        for e in records
        if e.status == "result_unknown"
    )
    unknown_ids = {e.execution_id for e in unknowns}
    for cause in ("result_unknown_requires_review", "manual_recovery_approved"):
        references = [ref for t in transitions if t.cause == cause for ref in t.evidence_refs]
        if len(references) != len(unknown_ids) or set(references) != unknown_ids:
            raise ValueError("Unknown execution history differs from audited transitions")
    for cause in ("semantic_recovery_not_allowed", "semantic_contract_recovery_approved"):
        references = [ref for t in transitions if t.cause == cause for ref in t.evidence_refs]
        if len(references) != len(contract_decisions) or set(references) != set(contract_decisions):
            raise ValueError("Contract recovery differs from audited transitions")
    prior: tuple[ResultUnknownResolution, ...] = ()
    for resolution in resolutions:
        result = resolve_unknown("blocked", unknowns, prior, resolution, policy)
        if resolution.decision != "authorize_replacement" or resolution.resolved_at > previous:
            raise ValueError("Closed Run has invalid unknown resolution")
        prior = result.resolutions
    if len(prior) != len(unknowns):
        raise ValueError("Closed Run still contains unresolved calls")
    if len(generations) > policy.generation_per_run or len(semantics) > policy.semantic_per_run:
        raise ValueError("Closed Run exceeded its execution budget")
    for slot in slots:
        candidate = slot.candidate
        if candidate is None:
            raise ValueError("Closed slot requires a candidate")
        for history, limit, recovery in (
            (
                slot.generation,
                policy.generation_per_slot,
                policy.allows_automatic_generation_recovery,
            ),
            (
                candidate.semantic,
                policy.semantic_per_candidate,
                policy.allows_automatic_semantic_recovery,
            ),
        ):
            if not 1 <= len(history) <= limit or history[-1].status != "succeeded":
                raise ValueError("Closed execution history or budget is invalid")
            for item in history[:-1]:
                if not (item.replacement_authorized or item.contract_recovery_authorized) and (
                    item.status != "failed" or item.failure is None or not recovery(item.failure)
                ):
                    raise ValueError("Execution recovery was not allowed by frozen policy")
        histories: tuple[list[GenerationCompletion | SemanticCompletion], ...] = (
            sorted(
                (
                    g
                    for g in generations
                    if (g.case_id, g.slot_ordinal) == (slot.case_id, slot.ordinal)
                ),
                key=lambda e: e.execution_ordinal,
            ),
            sorted(
                (s for s in semantics if s.candidate_id == candidate.candidate_id),
                key=lambda e: e.execution_ordinal,
            ),
        )
        for records in histories:
            for first, second in zip(records, records[1:], strict=False):
                if first.finished_at > second.started_at:
                    raise ValueError("Replacement overlaps its previous execution")
                if first.execution_id in contract_decisions and (
                    contract_decisions[first.execution_id].resolved_at > second.started_at
                ):
                    raise ValueError("Retry precedes contract recovery authorization")
                if (
                    first.status == "result_unknown"
                    and decisions[first.execution_id].resolved_at > second.started_at
                ):
                    raise ValueError("Replacement precedes manual authorization")
    if (
        next_action("collecting", preflight.status, preflight.case_id, slots, policy).kind
        != "await_review"
    ):
        raise ValueError("Candidate evidence is not complete")
    return previous
