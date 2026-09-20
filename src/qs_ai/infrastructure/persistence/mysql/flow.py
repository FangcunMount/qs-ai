"""Scoped, consistent, read-only flow descriptions from exact retained assets."""

import asyncio
import hashlib
import json
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.evaluation.assets import PolicyKind
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity
from qs_ai.domain.governance.flow import SCHEMA, describe
from qs_ai.domain.governance.prompt import PromptAsset
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import (
    AssetSnapshotReader,
    generation_snapshot,
)
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_asset_registry import read_policy
from qs_ai.infrastructure.persistence.mysql.evaluation_contracts import semantic_contract
from qs_ai.infrastructure.persistence.mysql.prompt_drafts import read_draft
from qs_ai.infrastructure.persistence.mysql.publication_records import load_publication
from qs_ai.infrastructure.persistence.mysql.schema import (
    configuration_publications,
    execution_configurations,
    prompt_assets,
    sessions,
)
from qs_ai.infrastructure.persistence.mysql.solution_assets import (
    model_values,
    release_from,
    selected_release,
)
from qs_ai.infrastructure.persistence.mysql.solutions import read_state
from qs_ai.infrastructure.persistence.mysql.suite_contracts import read as read_suite_contracts


async def assets(
    db: AsyncSession,
    release: EvidenceReleaseIdentity,
    org: int,
    *,
    semantic_owner: int | None = None,
) -> dict[str, Any]:
    if semantic_owner is None:
        binding = await read_suite_contracts(db, release.suite, org)
        semantic_owner = binding.semantic_owner_organization_id
    _, manifest = await generation_snapshot(db, release)
    asset = await AssetSnapshotReader(db, prompt_assets, PromptAsset).get(
        manifest.prompt.identity, manifest.prompt.version
    )
    if asset is None:
        raise ValueError("Fixed Prompt unavailable")
    raw = json.loads(asset.package_json)
    content = {
        name: raw[key]
        for name, key in (
            ("system_message", "SystemMessage"),
            ("task_template", "TaskTemplate"),
            ("data_preamble", "DataPreamble"),
            ("allowed_placeholders", "AllowedPlaceholders"),
        )
    }
    gaps = []
    semantic_prompt = None
    policy = None
    try:
        semantic = await semantic_contract(db, release, org, owner_organization_id=semantic_owner)
        semantic_prompt = semantic.prompt_markdown
    except NotFound:
        gaps.append("历史语义正文未登记；保留原指纹，不替换为当前版本。")
    try:
        policy_asset = await read_policy(db, PolicyKind.EXECUTION, release.execution_policy)
        policy = json.loads(policy_asset.definition_json)
    except NotFound:
        gaps.append("历史执行策略正文未登记；评测义务以原冻结证据为准。")
    return dict(
        content=content,
        models=await model_values(db, release),
        semantic_prompt=semantic_prompt,
        plan=policy,
        gaps=gaps,
    )


class MySQLFlowReader:
    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    async def solution(self, scope: DraftScope, identity: UUID) -> dict[str, Any]:
        async with asyncio.timeout(2), self.transactions.open() as db:
            await db.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            await db.execute(text("START TRANSACTION READ ONLY"))
            state = await read_state(db, scope, identity)
            prepared = state["prepared"]
            release = release_from(prepared["release"]) if prepared else selected_release(state)
            data = await assets(
                db,
                release,
                scope.organization_id,
                semantic_owner=None
                if prepared
                else state.get("evaluation_assets", {}).get("semantic_owner_organization_id", 0),
            )
            draft = await read_draft(db, scope, UUID(state["draft_id"]), state["draft_revision"])
            data["content"] = asdict(draft.content)
            data["models"] = {name: state[name] for name in ("generation", "semantic")}
            if prepared:
                data["plan"] = prepared["plan"]
            gaps = data.pop("gaps")
            return {
                "schema_version": SCHEMA,
                "source_kind": "solution",
                "source_id": str(identity),
                "version": state["revision"],
                "observed_at": datetime.now(UTC).isoformat(),
                "availability": "available",
                "partial": bool(gaps),
                "gaps": gaps,
                "immutable": bool(prepared),
                "draft": not bool(prepared),
                "asset_reference_mode": "frozen" if prepared else "source_with_draft_overrides",
                "source_publication_id": state["source"].get("publication_id"),
                "edit_solution_id": str(identity) if not prepared else None,
                **describe(release, editable=not bool(prepared), **data),
            }

    async def publication(self, scope: DraftScope, identity: UUID) -> dict[str, Any]:
        async with asyncio.timeout(2), self.transactions.open() as db:
            await db.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            await db.execute(text("START TRANSACTION READ ONLY"))
            org = await db.scalar(
                select(configuration_publications.c.organization_id).where(
                    configuration_publications.c.publication_id == str(identity)
                )
            )
            if org is None:
                raise NotFound("Publication unavailable")
            if org not in (0, scope.organization_id):
                raise NotFound("Publication unavailable")
            if org != scope.organization_id:
                # A global publication can be read only if it actually served this org.
                used = await db.scalar(
                    select(execution_configurations.c.session_id)
                    .join(sessions, sessions.c.id == execution_configurations.c.session_id)
                    .where(
                        execution_configurations.c.publication_id == str(identity),
                        sessions.c.org_id == scope.organization_id,
                    )
                    .limit(1)
                )
                if used is None:
                    raise NotFound("Publication unavailable")
            publication, _ = await load_publication(db, identity)
            row = (
                await db.execute(
                    select(
                        configuration_publications.c.content_json,
                        configuration_publications.c.content_sha256,
                    ).where(configuration_publications.c.publication_id == str(identity))
                )
            ).one()
            if hashlib.sha256(row.content_json.encode()).hexdigest() != row.content_sha256:
                raise ValueError("Publication fingerprint changed")
            data = await assets(db, publication.evidence.release, scope.organization_id)
            gaps = data.pop("gaps")
            return {
                "schema_version": SCHEMA,
                "source_kind": "publication",
                "source_id": str(identity),
                "version": row.content_sha256,
                "observed_at": datetime.now(UTC).isoformat(),
                "availability": "available",
                "partial": bool(gaps),
                "gaps": gaps,
                "immutable": True,
                "draft": False,
                "edit_solution_id": None,
                **describe(publication.evidence.release, editable=False, **data),
            }
