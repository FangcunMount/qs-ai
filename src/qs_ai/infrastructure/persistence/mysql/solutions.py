"""Atomic editing and preparation, with immutable idempotency receipts."""

import hashlib
import json
from dataclasses import asdict
from datetime import datetime
from typing import Any
from uuid import UUID, uuid5

from sqlalchemy import insert, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.application.governance.prompt_drafts import (
    CreatePromptDraft,
    DraftScope,
    RevisePromptDraft,
)
from qs_ai.application.governance.solutions import (
    CreateSolution,
    ModelSelection,
    PrepareSolution,
    SaveSolution,
)
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.config import Settings
from qs_ai.domain.governance.prompt import PromptAsset
from qs_ai.domain.governance.prompt_draft import DraftConflict
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import (
    AssetSnapshotReader,
    generation_snapshot,
)
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_contracts import semantic_contract
from qs_ai.infrastructure.persistence.mysql.evaluation_management import read_view
from qs_ai.infrastructure.persistence.mysql.prompt_drafts import apply_draft, read_draft
from qs_ai.infrastructure.persistence.mysql.schema import (
    prompt_assets,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    solution_commands as commands,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    solution_revisions as solutions,
)
from qs_ai.infrastructure.persistence.mysql.solution_assets import (
    model_values,
    prepare_assets,
    release_from,
    select_evaluation_assets,
    selected_release,
    source_release,
)
from qs_ai.infrastructure.persistence.mysql.suite_contracts import read as read_suite_contracts


def encode(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
        allow_nan=False,
    )


def checksum(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


def decode(raw: str, digest: str) -> dict[str, Any]:
    if len(raw.encode()) > 524288 or checksum(raw) != digest:
        raise ValueError("Solution snapshot checksum mismatch")
    value = json.loads(raw)
    if not isinstance(value, dict) or value.get("schema_version") != "qs-ai-solution/v1":
        raise ValueError("Solution snapshot format invalid")
    return value


async def read_state(
    db: AsyncSession,
    scope: DraftScope,
    solution_id: UUID,
    *,
    lock: bool = False,
) -> dict[str, Any]:
    query = select(solutions).where(
        solutions.c.solution_id == str(solution_id),
        solutions.c.organization_id == scope.organization_id,
    )
    row = (await db.execute(query.with_for_update() if lock else query)).mappings().one_or_none()
    if row is None:
        raise NotFound("Solution unavailable")
    state = decode(row["state_json"], row["state_sha256"])
    if (state["solution_id"], state["organization_id"], state["revision"], state["draft_id"]) != (
        str(solution_id),
        scope.organization_id,
        row["revision"],
        row["draft_id"],
    ):
        raise ValueError("Solution index changed")
    return state


async def prior(
    db: AsyncSession,
    scope: DraftScope,
    command_id: UUID,
    request: str | None = None,
) -> dict[str, Any] | None:
    row = (
        (await db.execute(select(commands).where(commands.c.command_id == str(command_id))))
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    if (row["organization_id"], row["operator_user_id"]) != (
        scope.organization_id,
        scope.operator_user_id,
    ):
        raise NotFound("Solution command unavailable")
    if request is not None and row["request_json"] != request:
        raise DraftConflict("Solution command identity already used")
    state = decode(row["receipt_json"], row["receipt_sha256"])
    if (
        state["solution_id"] != row["solution_id"]
        or state["organization_id"] != scope.organization_id
    ):
        raise ValueError("Solution command index changed")
    return state


async def hydrate(db: AsyncSession, scope: DraftScope, state: dict[str, Any]) -> dict[str, Any]:
    draft = await read_draft(db, scope, UUID(state["draft_id"]), state["draft_revision"])
    if draft.target_version != state["target_version"]:
        raise ValueError("Solution draft binding changed")
    release = release_from(state["source_release"])
    profile, manifest = await generation_snapshot(db, release)
    source = await AssetSnapshotReader(db, prompt_assets, PromptAsset).get(
        manifest.prompt.identity,
        manifest.prompt.version,
    )
    if source is None:
        raise ValueError("Source Prompt unavailable")
    raw = json.loads(source.package_json)
    original = {
        "system_message": raw["SystemMessage"],
        "task_template": raw["TaskTemplate"],
        "data_preamble": raw["DataPreamble"],
        "allowed_placeholders": raw["AllowedPlaceholders"],
    }
    selected = selected_release(state)
    semantic = await semantic_contract(
        db,
        selected,
        scope.organization_id,
        owner_organization_id=state.get("evaluation_assets", {}).get(
            "semantic_owner_organization_id", 0
        ),
    )
    if (selected.semantic_prompt, selected.semantic_output_schema) != (
        semantic.prompt,
        semantic.output_schema,
    ):
        raise ValueError("Frozen semantic assets unavailable")
    return {
        **state,
        "content": asdict(draft.content),
        "original_content": original,
        "original_models": await model_values(db, release),
        "policy": json.loads(profile.definition_json),
        "semantic_prompt": semantic.prompt_markdown,
    }


class MySQLSolutions:
    def __init__(self, transactions: Transactions, settings: Settings) -> None:
        self.transactions, self.settings = transactions, settings

    def capabilities(self) -> dict[str, Any]:
        catalog = []
        for entry in self.settings.models.catalog:
            binding = next(
                b
                for b in self.settings.models.bindings
                if (b.binding_id, b.revision) == (entry.binding_id, entry.binding_revision)
            )
            credential = (
                self.settings.effective_deepseek_api_key
                if binding.provider == "deepseek"
                else self.settings.zhipu_api_key
            )
            reason = (
                "model_unverified"
                if not entry.verified
                else "binding_unavailable"
                if not binding.enabled
                else "credential_unconfigured"
                if credential is None or not credential.get_secret_value().strip()
                else "model_v2_writes_disabled"
                if not self.settings.models.v2_writes_enabled
                else None
            )
            catalog.append(
                {
                    **entry.model_dump(),
                    "provider": binding.provider,
                    "protocol": binding.protocol,
                    "adapter_contract": binding.adapter_contract,
                    "available": reason is None,
                    "unavailable_reason": reason,
                }
            )
        return {
            "models": list(self.settings.governance_models),
            "v2_writes_enabled": self.settings.models.v2_writes_enabled,
            "catalog": catalog,
            "provider": "deepseek",
            "credential_configured": bool(self.settings.effective_deepseek_api_key),
            "endpoint_configured": bool(self.settings.generation.endpoint),
            "max_output_tokens": {"min": 1, "max": 12000},
            "timeout_milliseconds": {"min": 1000, "max": 180000},
            "reasoning_efforts": ["", "none", "minimal", "low", "medium", "high", "xhigh"],
            "unsupported_fields": ["temperature", "top_p"],
        }

    async def list(self, scope: DraftScope, cursor: str = "") -> dict[str, Any]:
        if cursor and (str(UUID(cursor)) != cursor or not UUID(cursor).int):
            raise ValueError("Invalid solution cursor")
        query = select(solutions).where(solutions.c.organization_id == scope.organization_id)
        if cursor:
            query = query.where(solutions.c.solution_id < cursor)
        async with self.transactions.open() as db:
            rows = (
                (await db.execute(query.order_by(solutions.c.solution_id.desc()).limit(21)))
                .mappings()
                .all()
            )
            items = []
            for row in rows[:20]:
                state = decode(row["state_json"], row["state_sha256"])
                items.append(
                    {
                        key: state[key]
                        for key in (
                            "solution_id",
                            "title",
                            "revision",
                            "reason",
                            "updated_at",
                            "created_by",
                            "target_version",
                            "prepared",
                            "source",
                        )
                    }
                )
            return {
                "items": items,
                "next_cursor": rows[19]["solution_id"] if len(rows) > 20 else "",
            }

    async def get(self, scope: DraftScope, solution_id: UUID) -> dict[str, Any]:
        async with self.transactions.open() as db:
            state = await read_state(db, scope, solution_id)
            return await hydrate(db, scope, state)

    async def receipt(self, scope: DraftScope, command_id: UUID) -> dict[str, Any]:
        async with self.transactions.open() as db:
            state = await prior(db, scope, command_id)
            if state is None:
                raise NotFound("Solution command unavailable")
            return await hydrate(db, scope, state)

    async def apply(
        self,
        scope: DraftScope,
        solution_id: UUID,
        command: CreateSolution | SaveSolution | PrepareSolution,
        at: datetime,
    ) -> dict[str, Any]:
        if not solution_id.int or at.tzinfo is None:
            raise ValueError("Solution identity and server time required")
        command_payload = asdict(command)
        if isinstance(command, SaveSolution):
            for purpose in ("generation", "semantic"):
                values = command_payload[purpose]
                for optional in (
                    "model_key",
                    "catalog_revision",
                    "thinking",
                    "temperature",
                    "top_p",
                ):
                    if values.get(optional) is None:
                        values.pop(optional, None)
            for key in ("evaluation_suite", "semantic_prompt", "semantic_owner_organization_id"):
                if command_payload[key] in (None, 0):
                    command_payload.pop(key)
        request = encode(
            {
                "scope": asdict(scope),
                "solution_id": str(solution_id),
                "action": type(command).__name__,
                "command": command_payload,
            }
        )
        try:
            async with self.transactions.open() as db:
                await db.connection(execution_options={"isolation_level": "READ COMMITTED"})
                previous = await prior(db, scope, command.command_id, request)
                if previous is not None:
                    return await hydrate(db, scope, previous)
                if isinstance(command, CreateSolution):
                    state = await self._create(db, scope, solution_id, command, at)
                else:
                    state = await read_state(db, scope, solution_id, lock=True)
                    previous = await prior(db, scope, command.command_id, request)
                    if previous is not None:
                        return await hydrate(db, scope, previous)
                    if state["revision"] != command.expected_revision or state["prepared"]:
                        raise DraftConflict("Solution changed or is already prepared")
                    current = await read_draft(db, scope, UUID(state["draft_id"]), lock=True)
                    if current.revision != state["draft_revision"]:
                        raise DraftConflict("Linked Prompt was edited outside this workspace")
                    state["reason"] = command.reason
                    if isinstance(command, SaveSolution):
                        if not command.title.strip():
                            raise ValueError("Title required")
                        await self._check_selections(
                            db, state, command.generation, command.semantic
                        )
                        selected_assets = await select_evaluation_assets(db, scope, state, command)
                        draft = await apply_draft(
                            db,
                            scope,
                            RevisePromptDraft(
                                current.draft_id,
                                uuid5(command.command_id, "prompt-save"),
                                current.revision,
                                command.content,
                                command.reason,
                            ),
                            at,
                        )
                        state.update(
                            title=command.title,
                            draft_revision=draft.revision,
                            generation=asdict(command.generation),
                            semantic=asdict(command.semantic),
                            evaluation_assets=selected_assets,
                        )
                    else:
                        await self._check_selections(
                            db,
                            state,
                            ModelSelection(**state["generation"]),
                            ModelSelection(**state["semantic"]),
                        )
                        state["prepared"] = await prepare_assets(
                            db,
                            scope,
                            state,
                            command.command_id,
                            at,
                            self.settings.governance_models,
                            self.settings.models,
                        )
                    state.update(revision=state["revision"] + 1, updated_at=at.isoformat())
                raw = encode(state)
                if not isinstance(command, CreateSolution):
                    await db.execute(
                        update(solutions)
                        .where(solutions.c.solution_id == str(solution_id))
                        .values(
                            revision=state["revision"], state_json=raw, state_sha256=checksum(raw)
                        )
                    )
                await db.execute(
                    insert(commands).values(
                        command_id=str(command.command_id),
                        solution_id=str(solution_id),
                        organization_id=scope.organization_id,
                        operator_user_id=scope.operator_user_id,
                        request_json=request,
                        receipt_json=raw,
                        receipt_sha256=checksum(raw),
                    )
                )
                result = await hydrate(db, scope, state)
                if len(encode(result).encode()) > 1048576:
                    raise ValueError("Solution response exceeds limit")
                await db.commit()
                return result
        except IntegrityError as error:
            if error.orig is None or not error.orig.args or error.orig.args[0] != 1062:
                raise
        async with self.transactions.open() as db:
            previous = await prior(db, scope, command.command_id, request)
            if previous is not None:
                return await hydrate(db, scope, previous)
        raise DraftConflict("Solution identity or immutable asset already exists")

    async def _check_selections(
        self,
        db: AsyncSession,
        state: dict[str, Any],
        generation: ModelSelection,
        semantic: ModelSelection,
    ) -> None:
        from qs_ai.application.governance.solution_models import edited_route
        from qs_ai.application.interpretation.route_assets import executable_route
        from qs_ai.infrastructure.persistence.mysql.solution_assets import route_for

        release = release_from(state["source_release"])
        for purpose, value in (("generation", generation), ("semantic", semantic)):
            source = executable_route(await route_for(db, getattr(release, purpose + "_route")))
            edited_route(
                source,
                value,
                source.revision,
                self.settings.governance_models,
                self.settings.models,
                purpose,
            )

    async def _create(
        self,
        db: AsyncSession,
        scope: DraftScope,
        solution_id: UUID,
        command: CreateSolution,
        at: datetime,
    ) -> dict[str, Any]:
        release = await source_release(db, scope, command)
        _, manifest = await generation_snapshot(db, release)
        models = await model_values(db, release)
        binding = await read_suite_contracts(db, release.suite, scope.organization_id)
        version = f"solution-{solution_id}"
        draft = await apply_draft(
            db,
            scope,
            CreatePromptDraft(
                uuid5(solution_id, "prompt"),
                uuid5(command.command_id, "prompt-create"),
                manifest.prompt,
                manifest.prompt.identity,
                version,
                command.reason,
            ),
            at,
        )
        source_reviews: list[Any] = []
        if command.source_run_id:
            view = await read_view(
                db,
                ManagementScope(
                    command.source_run_id, scope.organization_id, scope.operator_user_id
                ),
            )
            source_reviews = json.loads(view.reviews_json)
        state = {
            "schema_version": "qs-ai-solution/v1",
            "solution_id": str(solution_id),
            "organization_id": scope.organization_id,
            "revision": 1,
            "title": command.title,
            "reason": command.reason,
            "created_by": str(scope.operator_user_id),
            "updated_at": at.isoformat(),
            "target_version": version,
            "source": {
                "publication_id": str(command.publication_id) if command.publication_id else None,
                "run_id": str(command.source_run_id) if command.source_run_id else None,
            },
            "source_release": asdict(release),
            "evaluation_assets": {
                field: asdict(getattr(release, field))
                for field in (
                    "suite",
                    "execution_policy",
                    "gate_policy",
                    "semantic_prompt",
                    "semantic_output_schema",
                )
            }
            | {"semantic_owner_organization_id": binding.semantic_owner_organization_id},
            "source_reviews": source_reviews,
            "draft_id": str(draft.draft_id),
            "draft_revision": draft.revision,
            "prepared": None,
            **models,
        }
        raw = encode(state)
        await db.execute(
            insert(solutions).values(
                solution_id=str(solution_id),
                organization_id=scope.organization_id,
                revision=1,
                draft_id=str(draft.draft_id),
                state_json=raw,
                state_sha256=checksum(raw),
            )
        )
        return state
