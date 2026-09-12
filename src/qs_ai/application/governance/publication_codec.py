"""Canonical retained publication records; never parse a client approval as authority."""

import json
from dataclasses import asdict
from datetime import datetime
from uuid import UUID

from qs_ai.application.governance.publication import (
    MovePublication,
    PublicationScope,
    PublishConfiguration,
)
from qs_ai.domain.evaluation.finalization import FinalReview
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef
from qs_ai.domain.governance.manifest import AssetReference, GenerationManifest
from qs_ai.domain.governance.profile import ProfileAsset
from qs_ai.domain.governance.publication import (
    PublicationAudit,
    PublicationEvidence,
    PublishedConfiguration,
    ReleaseSelector,
)


def scalar(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    raise TypeError("Unsupported publication value")


def canonical(value: object) -> str:
    return json.dumps(
        value,
        default=scalar,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def publication_json(value: PublishedConfiguration) -> str:
    return canonical({"schema_version": "qs-ai-publication/v1", "publication": asdict(value)})


def read_publication(raw: str) -> PublishedConfiguration:
    if len(raw.encode()) > 256 * 1024:
        raise ValueError("Publication record exceeds limit")
    value = json.loads(raw)["publication"]
    proof = value["evidence"]
    audit = value["audit"]
    review = proof["final_review"]
    result = PublishedConfiguration(
        UUID(value["publication_id"]),
        PublicationEvidence(
            ProfileAsset(**proof["profile"]),
            GenerationManifest(**{k: AssetReference(**v) for k, v in proof["manifest"].items()}),
            EvidenceReleaseIdentity(
                **{k: FrozenContractRef(**v) for k, v in proof["release"].items()}
            ),
            UUID(proof["run_id"]),
            proof["run_version"],
            FinalReview(
                review["actor"],
                review["reason"],
                datetime.fromisoformat(review["finalized_at"]),
                review["passed"],
            ),
            proof["evaluated_manifest_fingerprint"],
        ),
        PublicationAudit(audit["actor"], audit["reason"], datetime.fromisoformat(audit["at"])),
    )
    if raw != publication_json(result):
        raise ValueError("Publication record is not canonical or contains unknown fields")
    return result


def request_json(scope: PublicationScope, command: PublishConfiguration | MovePublication) -> str:
    # Server time is excluded so a retry returns the original accepted audit.
    return canonical({"scope": asdict(scope), "action": command.action, "command": asdict(command)})


def read_request(raw: str) -> tuple[PublicationScope, PublishConfiguration | MovePublication]:
    if len(raw.encode()) > 16 * 1024:
        raise ValueError("Publication request exceeds limit")
    data = json.loads(raw)
    scope = PublicationScope(**data["scope"])
    values = data["command"]
    values["command_id"] = UUID(values["command_id"])
    values["selector"] = ReleaseSelector(**values["selector"])
    if values["expected_active_id"] is not None:
        values["expected_active_id"] = UUID(values["expected_active_id"])
    command: PublishConfiguration | MovePublication
    if data["action"] == "publish":
        values["run_id"] = UUID(values["run_id"])
        command = PublishConfiguration(**values)
    else:
        if values["target_id"] is not None:
            values["target_id"] = UUID(values["target_id"])
        command = MovePublication(**values)
    if request_json(scope, command) != raw:
        raise ValueError("Publication request differs from canonical command")
    return scope, command
