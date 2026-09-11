"""Reconstruct planned slots from terminal evidence, never from dispatch counts alone."""

from datetime import datetime
from typing import Any

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.domain.evaluation.actions import CandidateProgress, ExecutionResult, SlotProgress
from qs_ai.domain.evaluation.completion import GenerationCompletion, ProviderReceipt
from qs_ai.domain.evaluation.failure import ClassifiedFailure, ProviderDiagnostics
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
    plan: list[dict], records: list[Any], dispatches: list[Any]
) -> tuple[SlotProgress, ...]:
    """Semantic terminal projection will extend this; pending dispatches block preparation."""
    ledger = {row["invocation_id"]: row for row in dispatches}
    if len(ledger) != len(dispatches) or len(records) != len(ledger):
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
                candidate = CandidateProgress(stored["id"], stored["review_ready"])
            elif stored is not None or row["candidate_id"] is not None:
                raise CheckpointConflict("Failed execution cannot own candidate")
            executions.append(ExecutionResult(value.status, value.failure))
        slots.append(SlotProgress(slot["case_id"], slot["ordinal"], tuple(executions), candidate))
    return tuple(slots)
