"""Approval and executable assets must come from one transaction snapshot."""

import json
from collections.abc import Mapping
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.domain.evaluation.finalization import finalize_review
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef
from qs_ai.domain.governance.publication import PublicationEvidence
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import generation_snapshot
from qs_ai.infrastructure.persistence.mysql.evaluation_finalization import verified_final_snapshot


async def publication_evidence(
    db: AsyncSession, scope: ManagementScope, run: Mapping[str, Any], expected_release: str
) -> PublicationEvidence:
    snapshot = await verified_final_snapshot(db, scope, run)
    if not all(passed for _, passed in snapshot.preview.gate_passes):
        raise ValueError("Publication requires all five verified gates")
    definition = json.loads(run["definition_json"])
    release = EvidenceReleaseIdentity(
        **{name: FrozenContractRef(**ref) for name, ref in definition["release"].items()}
    )
    if expected_release != release.fingerprint():
        raise ValueError("Confirmed release differs from approved evaluation")
    profile, manifest = await generation_snapshot(db, release)
    if (
        definition.get("generation_manifest_json") != manifest.canonical_json()
        or definition.get("generation_manifest_fingerprint") != manifest.fingerprint()
    ):
        raise ValueError("Frozen evaluation manifest missing or different from current asset bytes")
    record = run["progress_json"]["gate_result"]
    review = finalize_review(snapshot.preview.quality, record["actor"], record["reason"])
    return PublicationEvidence(
        profile, manifest, release, scope.run_id, run["version"], review, manifest.fingerprint()
    )
