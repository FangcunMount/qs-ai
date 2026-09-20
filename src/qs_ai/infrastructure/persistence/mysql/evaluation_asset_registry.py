"""Exact version reads in the caller's transaction; no latest or filesystem fallback."""

from sqlalchemy import insert, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.evaluation.assets import PolicyAsset, PolicyKind, SemanticPromptAsset
from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.domain.governance.profile import AssetConflict
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_policy_assets as policies
from qs_ai.infrastructure.persistence.mysql.schema import semantic_prompt_assets as prompts
from qs_ai.infrastructure.qs_server.evaluation_policies import (
    FrozenPolicyDocument,
    execution_policy,
    quality_thresholds,
)


async def read_policy(
    db: AsyncSession, kind: PolicyKind, reference: FrozenContractRef
) -> PolicyAsset:
    row = (
        (
            await db.execute(
                select(policies).where(
                    policies.c.kind == kind.value,
                    policies.c.asset_id == reference.id,
                    policies.c.version == reference.version,
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise NotFound("Evaluation policy unavailable")
    asset = PolicyAsset(
        kind,
        FrozenContractRef(row["asset_id"], row["version"], row["fingerprint"]),
        row["definition_json"],
    )
    if asset.reference != reference:
        raise ValueError("Evaluation policy fingerprint mismatch")
    return asset


async def read_semantic_prompt(
    db: AsyncSession,
    reference: FrozenContractRef,
    *,
    owner_organization_id: int,
    requesting_organization_id: int,
) -> SemanticPromptAsset:
    # Owner is explicit: no organization shadowing of a shared asset or cross-org probing.
    if (
        owner_organization_id not in (0, requesting_organization_id)
        or requesting_organization_id <= 0
    ):
        raise NotFound("Semantic prompt unavailable")
    row = (
        (
            await db.execute(
                select(prompts).where(
                    prompts.c.organization_id == owner_organization_id,
                    prompts.c.asset_id == reference.id,
                    prompts.c.version == reference.version,
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise NotFound("Semantic prompt unavailable")
    asset = SemanticPromptAsset(
        FrozenContractRef(row["asset_id"], row["version"], row["fingerprint"]),
        row["markdown"],
        row["organization_id"],
    )
    if asset.reference != reference:
        raise ValueError("Semantic prompt fingerprint mismatch")
    return asset


def provenance(source_ref: str, imported_by: str) -> None:
    if any(not value.strip() or len(value) > 255 for value in (source_ref, imported_by)):
        raise ValueError("Evaluation asset provenance required")


def duplicate(error: IntegrityError) -> bool:
    return bool(error.orig is not None and error.orig.args and error.orig.args[0] == 1062)


class MySQLEvaluationAssets:
    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    async def put_policy(self, asset: PolicyAsset, source_ref: str, imported_by: str) -> bool:
        provenance(source_ref, imported_by)
        document = FrozenPolicyDocument(asset.reference, asset.definition_json)
        if asset.kind == PolicyKind.EXECUTION:
            execution_policy(document)
        else:
            quality_thresholds(document)
        try:
            async with self.transactions.open() as db:
                await db.execute(
                    insert(policies).values(
                        kind=asset.kind.value,
                        asset_id=asset.reference.id,
                        version=asset.reference.version,
                        fingerprint=asset.reference.fingerprint,
                        definition_json=asset.definition_json,
                        source_ref=source_ref,
                        imported_by=imported_by,
                    )
                )
                await db.commit()
            return True
        except IntegrityError as error:
            if not duplicate(error):
                raise
        async with self.transactions.open() as db:
            try:
                existing = await read_policy(db, asset.kind, asset.reference)
            except ValueError:
                raise AssetConflict(
                    "Evaluation policy version already has different content"
                ) from None
            if existing != asset:
                raise AssetConflict("Evaluation policy version already has different content")
        return False

    async def put_semantic_prompt(
        self, asset: SemanticPromptAsset, source_ref: str, imported_by: str
    ) -> bool:
        provenance(source_ref, imported_by)
        try:
            async with self.transactions.open() as db:
                await db.execute(
                    insert(prompts).values(
                        organization_id=asset.organization_id,
                        asset_id=asset.reference.id,
                        version=asset.reference.version,
                        fingerprint=asset.reference.fingerprint,
                        markdown=asset.markdown,
                        source_ref=source_ref,
                        imported_by=imported_by,
                    )
                )
                await db.commit()
            return True
        except IntegrityError as error:
            if not duplicate(error):
                raise
        async with self.transactions.open() as db:
            try:
                existing = await read_semantic_prompt(
                    db,
                    asset.reference,
                    owner_organization_id=asset.organization_id,
                    requesting_organization_id=asset.organization_id or 1,
                )
            except ValueError:
                raise AssetConflict(
                    "Semantic prompt version already has different content"
                ) from None
            if existing != asset:
                raise AssetConflict("Semantic prompt version already has different content")
        return False
