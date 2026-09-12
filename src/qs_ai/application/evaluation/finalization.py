"""Encode a validated final gate and audit for persistence and transport."""

import json
from dataclasses import asdict

from qs_ai.application.evaluation.gates import GatePreview
from qs_ai.domain.evaluation.finalization import finalize_review


def final_record(preview: GatePreview, actor: str, reason: str) -> dict:
    decision = finalize_review(preview.quality, actor, reason)
    record = {
        "schema_version": "qs-ai-evaluation-finalization/v1",
        "run_id": preview.run_id,
        "source_version": preview.version,
        "version": preview.version + 1,
        "release_fingerprint": preview.release_fingerprint,
        **asdict(decision),
        "finalized_at": decision.finalized_at.isoformat(),
        "status": decision.status,
        "gate_result": {
            **asdict(preview.quality),
            "evaluated_at": preview.quality.evaluated_at.isoformat(),
            "gate_passes": dict(preview.gate_passes),
        },
    }
    encoded = json.dumps(record, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    if len(encoded.encode()) > 256 * 1024:
        raise ValueError("Finalization evidence exceeds response bound")
    return json.loads(encoded)


def final_transition(record: dict) -> dict:
    return {
        "from": "awaiting_review",
        "to": record["status"],
        "actor": record["actor"],
        "cause_code": "human_review_finalized",
        "reason": record["reason"],
        "at": record["finalized_at"],
    }
