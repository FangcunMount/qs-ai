"""Resolve exact evaluation contract bytes from MySQL or verified historical snapshots."""

import asyncio
from dataclasses import dataclass
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.domain.evaluation.assets import PolicyKind
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity
from qs_ai.domain.evaluation.policy import ExecutionPolicy
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import AssetSnapshotReader
from qs_ai.infrastructure.persistence.mysql.evaluation_asset_registry import (
    read_policy,
    read_semantic_prompt,
)
from qs_ai.infrastructure.persistence.mysql.schema import schema_assets
from qs_ai.infrastructure.qs_server.evaluation_policies import (
    FrozenPolicyDocument,
    execution_policy,
)
from qs_ai.infrastructure.qs_server.semantic_assets import SemanticAssets, semantic_assets


@dataclass(frozen=True)
class EvaluationContracts:
    execution: ExecutionPolicy
    gate: FrozenPolicyDocument
    semantic: SemanticAssets


async def semantic_contract(
    db: AsyncSession,
    release: EvidenceReleaseIdentity,
    organization_id: int,
    *,
    owner_organization_id: int = 0,
    frozen: dict[str, Any] | None = None,
) -> SemanticAssets:
    frozen = frozen or {}
    keys = ("semantic_prompt_markdown", "semantic_output_schema_json")
    if any(key in frozen for key in keys):
        if not all(isinstance(frozen.get(key), str) and frozen[key] for key in keys):
            raise ValueError("Incomplete frozen semantic contract")
        raw_prompt, raw_schema = (frozen[key] for key in keys)
    else:
        # Historical Runs lacking the body can only recover exactly their original digest.
        prompt = await read_semantic_prompt(
            db,
            release.semantic_prompt,
            owner_organization_id=owner_organization_id,
            requesting_organization_id=organization_id,
        )
        schema_id, version = release.semantic_output_schema.version.rsplit("/", 1)
        schema = await AssetSnapshotReader(db, schema_assets, SchemaAsset).get(schema_id, version)
        if schema is None or schema.fingerprint != release.semantic_output_schema.fingerprint:
            raise ValueError("Frozen semantic schema unavailable or changed")
        raw_prompt, raw_schema = prompt.markdown, schema.definition_json
    return await asyncio.to_thread(
        semantic_assets,
        raw_prompt,
        raw_schema,
        release.semantic_prompt,
        release.semantic_output_schema,
    )


async def evaluation_contracts(
    db: AsyncSession,
    release: EvidenceReleaseIdentity,
    organization_id: int,
    *,
    semantic_owner_organization_id: int = 0,
) -> EvaluationContracts:
    execution = await read_policy(db, PolicyKind.EXECUTION, release.execution_policy)
    gate = await read_policy(db, PolicyKind.GATE, release.gate_policy)
    return EvaluationContracts(
        await asyncio.to_thread(
            execution_policy, FrozenPolicyDocument(execution.reference, execution.definition_json)
        ),
        FrozenPolicyDocument(gate.reference, gate.definition_json),
        await semantic_contract(
            db, release, organization_id, owner_organization_id=semantic_owner_organization_id
        ),
    )
