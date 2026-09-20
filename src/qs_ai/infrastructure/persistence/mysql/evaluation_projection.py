"""Reconstruct planned slots from terminal evidence, never from dispatch counts alone."""

import json
from datetime import datetime
from typing import Any

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.domain.evaluation.actions import CandidateProgress, ExecutionResult, SlotProgress
from qs_ai.domain.evaluation.completion import GenerationCompletion, ProviderReceipt
from qs_ai.domain.evaluation.contract_recovery import decode_recoveries, validate_recoveries
from qs_ai.domain.evaluation.failure import ClassifiedFailure, ProviderDiagnostics
from qs_ai.domain.evaluation.policy import ExecutionPolicy
from qs_ai.domain.evaluation.resolution import (
    ResultUnknownResolution,
    UnknownExecution,
    resolve_unknown,
)
from qs_ai.domain.evaluation.semantic_completion import SemanticCompletion
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import decode


def decode_completion(row: Any) -> GenerationCompletion:
    value = dict(row["evidence_json"])
    for key in ("started_at", "finished_at"):
        value[key] = datetime.fromisoformat(value[key])
    if value["receipt"] is not None:
        value["receipt"] = ProviderReceipt(**value["receipt"])
    if value["failure"] is not None:
        failure = dict(value["failure"])
        failure["evidence_refs"] = tuple(failure["evidence_refs"])
        if failure["provider_diagnostics"] is not None:
            failure["provider_diagnostics"] = ProviderDiagnostics(**failure["provider_diagnostics"])
        value["failure"] = ClassifiedFailure(**failure)
    value["raw_output"] = row["raw_output"]
    value["normalized_output"] = row["normalized_output"]
    result = GenerationCompletion(**value)
    for key in ("execution_id", "invocation_id", "case_id", "slot_ordinal", "execution_ordinal"):
        if getattr(result, key) != row[key]:
            raise CheckpointConflict("Stored terminal identity differs from index")
    return result


def project_slots(
    plan: list[dict],
    records: list[Any],
    dispatches: list[Any],
    semantic_records: list[Any] | None = None,
    resolutions: list[dict] | None = None,
    contract_recoveries: list[dict] | None = None,
    *,
    policy: ExecutionPolicy,
) -> tuple[SlotProgress, ...]:
    """Every dispatch must have terminal evidence before new preparation."""
    semantic_records = semantic_records or []
    recovered = validate_recoveries(
        decode_recoveries(contract_recoveries or []),
        tuple(decode_semantic_completion(r) for r in semantic_records),
        policy,
    )
    authorized: set[str] = set()
    if resolutions:
        unknowns = tuple(
            UnknownExecution(
                r["execution_id"],
                kind,
                r["execution_ordinal"],
                datetime.fromisoformat(r["evidence_json"]["finished_at"]),
            )
            for kind, rows in (("generation", records), ("semantic", semantic_records))
            for r in rows
            if r["evidence_json"]["status"] == "result_unknown"
        )
        prior: tuple[ResultUnknownResolution, ...] = ()
        for raw in resolutions:
            resolution = ResultUnknownResolution(
                **{**raw, "resolved_at": datetime.fromisoformat(raw["resolved_at"])}
            )
            result = resolve_unknown("blocked", unknowns, prior, resolution, policy)
            prior = result.resolutions
            if resolution.decision == "authorize_replacement":
                authorized.add(resolution.execution_id)
    ledger = {row["invocation_id"]: row for row in dispatches}
    if len(ledger) != len(dispatches) or len(records) + len(semantic_records) != len(ledger):
        raise CheckpointConflict("Dispatch and terminal evidence require reconciliation")
    grouped: dict[tuple[str, int], list[tuple[GenerationCompletion, Any]]] = {
        (slot["case_id"], slot["ordinal"]): [] for slot in plan
    }
    seen: set[str] = set()
    for row in records:
        value = decode_completion(row)
        entry = ledger.get(value.invocation_id)
        if entry is None or entry["kind"] != "generation" or value.invocation_id in seen:
            raise CheckpointConflict("Terminal evidence requires unique generation dispatch")
        seen.add(value.invocation_id)
        cp = decode(entry["checkpoint_json"])
        if cp is None or not value.matches_checkpoint(cp, cp.owner):
            raise CheckpointConflict("Stored completion does not match dispatch checkpoint")
        if any(
            entry[key] != getattr(cp, key)
            for key in (
                "execution_id",
                "invocation_id",
                "kind",
                "case_id",
                "slot_ordinal",
                "candidate_id",
            )
        ):
            raise CheckpointConflict("Dispatch index differs from checkpoint evidence")
        if (value.case_id, value.slot_ordinal) not in grouped:
            raise CheckpointConflict("Stored completion outside frozen slots")
        grouped[value.case_id, value.slot_ordinal].append((value, row))
    slots = []
    for slot in plan:
        history = sorted(
            grouped[slot["case_id"], slot["ordinal"]], key=lambda item: item[0].execution_ordinal
        )
        if [value.execution_ordinal for value, _ in history] != list(range(1, len(history) + 1)):
            raise CheckpointConflict("Incomplete terminal execution sequence")
        candidate = None
        executions = []
        for value, row in history:
            if candidate is not None:
                raise CheckpointConflict("Generation continued after candidate acceptance")
            stored = row["candidate_json"]
            if value.status == "succeeded":
                if (
                    not stored
                    or (
                        stored["id"],
                        stored["generation_execution_id"],
                        stored["normalized_output_fingerprint"],
                        stored["accepted_at"],
                    )
                    != (
                        row["candidate_id"],
                        value.execution_id,
                        value.normalized_fingerprint,
                        value.finished_at.isoformat(),
                    )
                    or not stored.get("assertions")
                ):
                    raise CheckpointConflict("Successful generation missing matching candidate")
                candidate = project_candidate(
                    stored, value, semantic_records, ledger, seen, authorized, recovered
                )
            elif stored is not None or row["candidate_id"] is not None:
                raise CheckpointConflict("Failed execution cannot own candidate")
            executions.append(
                ExecutionResult(value.status, value.failure, value.execution_id in authorized)
            )
        slots.append(SlotProgress(slot["case_id"], slot["ordinal"], tuple(executions), candidate))
    if seen != ledger.keys():
        raise CheckpointConflict("Unmatched dispatch or semantic candidate")
    return tuple(slots)


def decode_semantic_completion(row: Any) -> SemanticCompletion:
    fields = dict(row["evidence_json"])
    fingerprint = fields.pop("output_fingerprint")
    for key in ("started_at", "finished_at"):
        fields[key] = datetime.fromisoformat(fields[key])
    if fields["receipt"] is not None:
        fields["receipt"] = ProviderReceipt(**fields["receipt"])
    if fields["failure"] is not None:
        failure = dict(fields["failure"])
        failure["evidence_refs"] = tuple(failure["evidence_refs"])
        if failure["provider_diagnostics"] is not None:
            failure["provider_diagnostics"] = ProviderDiagnostics(**failure["provider_diagnostics"])
        fields["failure"] = ClassifiedFailure(**failure)
    value = SemanticCompletion(
        **fields, raw_output=row["raw_output"], normalized_output=row["normalized_output"]
    )
    if value.output_fingerprint != fingerprint or any(
        getattr(value, k) != row[k]
        for k in ("execution_id", "invocation_id", "candidate_id", "execution_ordinal")
    ):
        raise CheckpointConflict("Semantic bytes or index differ from evidence")
    return value


def project_candidate(
    stored: dict,
    generated: GenerationCompletion,
    records: list[Any],
    ledger: dict,
    seen: set[str],
    authorized: set[str],
    recovered: set[str],
) -> CandidateProgress:
    history = sorted(
        (r for r in records if r["candidate_id"] == stored["id"]),
        key=lambda r: r["execution_ordinal"],
    )
    if [r["execution_ordinal"] for r in history] != list(range(1, len(history) + 1)):
        raise CheckpointConflict("Incomplete semantic execution sequence")
    executions = []
    accepted = None
    for row in history:
        if accepted is not None:
            raise CheckpointConflict("Semantic execution continued after accepted result")
        value = decode_semantic_completion(row)
        fingerprint = value.output_fingerprint
        entry = ledger.get(value.invocation_id)
        cp = decode(entry["checkpoint_json"]) if entry is not None else None
        if (
            cp is None
            or not value.matches_checkpoint(cp, cp.owner, generated.normalized_fingerprint)
            or (cp.case_id, cp.slot_ordinal) != (generated.case_id, generated.slot_ordinal)
            or value.invocation_id in seen
        ):
            raise CheckpointConflict("Semantic completion does not match candidate dispatch")
        assert entry is not None
        if any(
            entry[k] != getattr(cp, k)
            for k in (
                "execution_id",
                "invocation_id",
                "kind",
                "case_id",
                "slot_ordinal",
                "candidate_id",
            )
        ):
            raise CheckpointConflict("Semantic dispatch index mismatch")
        seen.add(value.invocation_id)
        result = row["result_json"]
        if value.status == "succeeded":
            output = json.loads(value.normalized_output)
            if (
                result is None
                or result["output_fingerprint"] != fingerprint
                or dict(result["scores"]) != output["scores"]
                or result["rationale"] != output["rationale"].strip()
            ):
                raise CheckpointConflict("Semantic result differs from normalized evidence")
            expected = {(d["type"], d["scope"], d["ordinal"]): d for d in output["decisions"]}
            if len(expected) != len(output["decisions"]) or len(result["decisions"]) != len(
                expected
            ):
                raise CheckpointConflict("Semantic decisions incomplete or duplicated")
            current = {
                (a["type"], a["scope"], a["ordinal"]): a for a in stored["semantic_assertions"]
            }
            for decision in result["decisions"]:
                decision_key = (decision["type"], decision["scope"], decision["ordinal"])
                raw = expected.pop(decision_key, None)
                if (
                    raw is None
                    or decision != current.get(decision_key)
                    or decision["status"] != raw["status"]
                    or decision["detail"] != raw["detail"].strip()
                    or decision["evaluator"] != result["evaluator_version"]
                ):
                    raise CheckpointConflict(
                        "Candidate decisions differ from accepted semantic result"
                    )
            accepted = value.execution_id
        elif result is not None:
            raise CheckpointConflict("Failed semantic execution cannot contain result")
        executions.append(
            ExecutionResult(
                value.status,
                value.failure,
                value.execution_id in authorized,
                value.execution_id in recovered,
            )
        )
    if (
        stored["review_ready"] != (accepted is not None)
        or stored.get("accepted_semantic_execution_id") != accepted
    ):
        raise CheckpointConflict("Candidate review readiness lacks matching semantic evidence")
    return CandidateProgress(stored["id"], stored["review_ready"], tuple(executions))
