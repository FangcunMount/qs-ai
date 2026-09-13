"""Derive Profile states in one snapshot; never write a second lifecycle state machine."""

import hashlib
import json
from datetime import UTC
from typing import Any
from uuid import UUID

from sqlalchemy import ColumnElement, RowMapping, and_, case, exists, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.governance.asset_catalog import validate_identity, validate_version
from qs_ai.application.governance.profile_lifecycle import (
    ProfileLifecycle,
    ProfileLifecyclePage,
    ProfileLifecycleQuery,
    ProfileStatus,
)
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.governance.manifest import AssetReference
from qs_ai.domain.governance.profile import ProfileAsset
from qs_ai.domain.governance.publication import ReleaseSelector
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.publication_records import (
    load_pointer,
    load_publication,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    configuration_publication_pointers as pointers,
)
from qs_ai.infrastructure.persistence.mysql.schema import configuration_publications as publications
from qs_ai.infrastructure.persistence.mysql.schema import profile_assets as profiles


def publication_matches() -> ColumnElement[bool]:
    def field(name: str) -> ColumnElement[Any]:
        return func.json_unquote(
            func.json_extract(publications.c.content_json, "$.publication.evidence.profile." + name)
        ).collate("utf8mb4_0900_bin")

    return and_(
        field("profile_id") == profiles.c.profile_id.collate("utf8mb4_0900_bin"),
        field("version") == profiles.c.version.collate("utf8mb4_0900_bin"),
    )


def lifecycle_status() -> ColumnElement[str]:
    historical = exists(select(1).select_from(publications).where(publication_matches()))
    active = exists(
        select(1)
        .select_from(
            publications.join(
                pointers, pointers.c.active_publication_id == publications.c.publication_id
            )
        )
        .where(publication_matches())
    )
    return case((active, "published"), (historical, "disabled"), else_="draft")


async def lifecycle(db: AsyncSession, row: RowMapping) -> ProfileLifecycle:
    asset = ProfileAsset(
        row["profile_id"], row["version"], row["fingerprint"], row["definition_json"]
    )
    selector = ReleaseSelector(**json.loads(asset.definition_json)["selector"])
    pointer_row = (
        (await db.execute(select(pointers).where(pointers.c.selector_key == selector.key())))
        .mappings()
        .one_or_none()
    )
    pointer = await load_pointer(db, pointer_row) if pointer_row is not None else None
    active = pointer.active if pointer else None
    if active and (active.evidence.profile.profile_id, active.evidence.profile.version) == (
        asset.profile_id,
        asset.version,
    ):
        if active.evidence.profile != asset:
            raise ValueError("Published Profile content differs from retained asset")
        status: ProfileStatus = "published"
    else:
        historical_id = await db.scalar(
            select(publications.c.publication_id)
            .where(
                publications.c.selector_key == selector.key(),
                func.json_unquote(
                    func.json_extract(
                        publications.c.content_json, "$.publication.evidence.profile.profile_id"
                    )
                ).collate("utf8mb4_0900_bin")
                == asset.profile_id,
                func.json_unquote(
                    func.json_extract(
                        publications.c.content_json, "$.publication.evidence.profile.version"
                    )
                ).collate("utf8mb4_0900_bin")
                == asset.version,
            )
            .order_by(publications.c.publication_id)
            .limit(1)
        )
        status = "draft"
        if historical_id is not None:
            historical, _ = await load_publication(db, UUID(historical_id))
            if historical.evidence.profile != asset or pointer is None:
                raise ValueError("Historical publication differs from Profile authority")
            status = "disabled"
    if status != row["lifecycle_status"]:
        raise ValueError("Profile lifecycle index differs from validated publication")
    return ProfileLifecycle(
        AssetReference(
            asset.profile_id,
            asset.version,
            asset.fingerprint,
            hashlib.sha256(asset.definition_json.encode()).hexdigest(),
        ),
        status,
        row["source_ref"],
        row["created_at"].replace(tzinfo=UTC).isoformat(),
        str(active.publication_id) if active and status == "published" else "",
        str(active.evidence.run_id) if active and status == "published" else "",
        pointer.version if pointer else 0,
        pointer.changed_at.isoformat() if pointer and pointer.changed_at else "",
        ("replaced" if active else "disabled") if status == "disabled" else "",
    )


class MySQLProfileLifecycle:
    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    async def list(self, scope: DraftScope, query: ProfileLifecycleQuery) -> ProfileLifecyclePage:
        DraftScope(scope.organization_id, scope.operator_user_id)
        identity, version = (
            c.collate("utf8mb4_0900_bin") for c in (profiles.c.profile_id, profiles.c.version)
        )
        status = lifecycle_status()
        statement = select(profiles, status.label("lifecycle_status"))
        if query.identity:
            statement = statement.where(identity == query.identity)
        if query.status:
            statement = statement.where(status == query.status)
        after = query.after()
        if after:
            statement = statement.where(
                or_(identity > after[0], and_(identity == after[0], version > after[1]))
            )
        statement = statement.order_by(identity, version).limit(query.limit + 1)
        async with self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            rows = (await db.execute(statement)).mappings().all()
            items = tuple([await lifecycle(db, row) for row in rows[: query.limit]])
        return ProfileLifecyclePage(
            items, query.next_cursor(items[-1].reference) if len(rows) > query.limit else ""
        )

    async def get(self, scope: DraftScope, identity: str, version: str) -> ProfileLifecycle:
        DraftScope(scope.organization_id, scope.operator_user_id)
        validate_identity(identity)
        validate_version(version)
        async with self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            row = (
                (
                    await db.execute(
                        select(profiles, lifecycle_status().label("lifecycle_status")).where(
                            profiles.c.profile_id.collate("utf8mb4_0900_bin") == identity,
                            profiles.c.version.collate("utf8mb4_0900_bin") == version,
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise NotFound
            return await lifecycle(db, row)
