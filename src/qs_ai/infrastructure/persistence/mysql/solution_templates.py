"""Exact initialized MBTI templates; DB assets, never a file or latest fallback."""

import json
from dataclasses import asdict
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef
from qs_ai.domain.governance.profile import ProfileAsset
from qs_ai.domain.governance.prompt import PromptAsset
from qs_ai.domain.governance.route import RouteAsset
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import AssetSnapshotReader
from qs_ai.infrastructure.persistence.mysql.evaluation_contracts import evaluation_contracts
from qs_ai.infrastructure.persistence.mysql.evaluation_suites import load_registered_suite
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_suites,
    profile_assets,
    prompt_assets,
    route_assets,
    schema_assets,
)
from qs_ai.infrastructure.qs_server.evaluation_release import validate_release_assets
from qs_ai.infrastructure.qs_server.evaluation_suite import (
    MBTI_EXPLORATION_ROOT,
    MBTI_ROOT,
    MBTI_THEMES_ROOT,
)


async def template_release(
    db: AsyncSession, scope: DraftScope, reference: FrozenContractRef
) -> EvidenceReleaseIdentity:
    if reference not in (MBTI_ROOT, MBTI_THEMES_ROOT, MBTI_EXPLORATION_ROOT):
        raise NotFound("Template unavailable")
    suite = await load_registered_suite(db, reference, organization_id=scope.organization_id)
    document = json.loads(suite.definition_json)
    release = EvidenceReleaseIdentity(
        suite=reference,
        **{
            key: FrozenContractRef(**value)
            for key, value in document["template"]["release"].items()
        },
    )
    fixed = await evaluation_contracts(db, release, scope.organization_id)
    await validate_release_assets(
        release,
        AssetSnapshotReader(db, profile_assets, ProfileAsset),
        AssetSnapshotReader(db, prompt_assets, PromptAsset),
        AssetSnapshotReader(db, route_assets, RouteAsset),
        AssetSnapshotReader(db, schema_assets, SchemaAsset),
        execution_policy_json=fixed.execution.definition_json,
        gate_policy_json=fixed.gate.definition_json,
        semantic=fixed.semantic,
        frozen_suite=suite,
    )
    return release


async def template_catalog(db: AsyncSession, scope: DraftScope) -> list[dict[str, Any]]:
    # Each version is explicit. No guessed identity, latest selection or file fallback.
    result = []
    for reference, name in (
        (MBTI_ROOT, "MBTI 单次解读首版"),
        (MBTI_THEMES_ROOT, "MBTI 三主题单次解读首版"),
        (MBTI_EXPLORATION_ROOT, "MBTI 探索版三主题单次解读首版"),
    ):
        installed = await db.scalar(
            select(evaluation_suites.c.suite_id).where(
                evaluation_suites.c.organization_id == 0,
                evaluation_suites.c.suite_id == reference.id,
                evaluation_suites.c.suite_version == reference.version,
            )
        )
        if installed is None:
            continue
        release = await template_release(db, scope, reference)
        profile = await AssetSnapshotReader(db, profile_assets, ProfileAsset).get(
            release.profile.id, release.profile.version
        )
        assert profile is not None  # verified in this transaction
        definition = json.loads(profile.definition_json)
        result.append(
            {
                "name": name,
                "template_ref": asdict(reference),
                "scene_contract_version": definition["scene_contract_version"],
                "selector": definition["selector"],
                "published": False,
                "reason": "requires_evaluation_review_and_publication",
            }
        )
    return result
