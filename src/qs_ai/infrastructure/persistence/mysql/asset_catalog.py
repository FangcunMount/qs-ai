"""Read exact database assets; organization-derived suites remain scoped."""

import asyncio
import hashlib
from dataclasses import fields
from typing import Any

from sqlalchemy import Column, RowMapping, Table, and_, or_, select

from qs_ai.application.governance.asset_catalog import (
    KINDS,
    AssetKind,
    CatalogDetail,
    CatalogItem,
    CatalogPage,
    CatalogQuery,
    validate_identity,
    validate_version,
)
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.evaluation.assets import PolicyAsset, PolicyKind
from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.domain.governance.manifest import AssetReference
from qs_ai.domain.governance.profile import ProfileAsset
from qs_ai.domain.governance.prompt import PromptAsset
from qs_ai.domain.governance.route import RouteAsset
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_suites import load_registered_suite
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_policy_assets,
    evaluation_suites,
    profile_assets,
    prompt_assets,
    route_assets,
    schema_assets,
)

TABLES: dict[AssetKind, Table] = {
    "profile": profile_assets,
    "prompt": prompt_assets,
    "route": route_assets,
    "schema": schema_assets,
    "suite": evaluation_suites,
    "execution_policy": evaluation_policy_assets,
    "gate_policy": evaluation_policy_assets,
}
TYPES: dict[str, type[ProfileAsset] | type[PromptAsset] | type[RouteAsset] | type[SchemaAsset]] = {
    "profile": ProfileAsset,
    "prompt": PromptAsset,
    "route": RouteAsset,
    "schema": SchemaAsset,
}


def detail(
    kind: AssetKind, identity: str, version: str, fingerprint: str, raw: str
) -> CatalogDetail:
    reference = AssetReference(
        identity, version, fingerprint, hashlib.sha256(raw.encode()).hexdigest()
    )
    return CatalogDetail(CatalogItem(kind, reference), raw)


POLICY_KINDS = {"execution_policy": PolicyKind.EXECUTION, "gate_policy": PolicyKind.GATE}


def columns(kind: AssetKind) -> tuple[Column[Any], Column[Any]]:
    table = TABLES[kind]
    if kind in POLICY_KINDS:
        return table.c.asset_id, table.c.version
    first, second = list(table.primary_key.columns)
    return first, second


def decode(kind: AssetKind, row: RowMapping) -> CatalogDetail:
    if kind in POLICY_KINDS:
        policy = PolicyAsset(
            POLICY_KINDS[kind],
            FrozenContractRef(row["asset_id"], row["version"], row["fingerprint"]),
            row["definition_json"],
        )
        return detail(
            kind,
            policy.reference.id,
            policy.reference.version,
            policy.reference.fingerprint,
            policy.definition_json,
        )
    cls = TYPES[kind]
    asset = cls(**{field.name: row[field.name] for field in fields(cls)})
    keys = list(TABLES[kind].primary_key.columns)
    raw = asset.package_json if isinstance(asset, PromptAsset) else asset.definition_json
    return detail(kind, row[keys[0].name], row[keys[1].name], asset.fingerprint, raw)


async def suite_detail(db: Any, row: RowMapping, scope: DraftScope) -> CatalogDetail:
    reference = FrozenContractRef(row["suite_id"], row["suite_version"], row["fingerprint"])
    suite = await load_registered_suite(db, reference, organization_id=scope.organization_id)
    return detail(
        "suite", reference.id, reference.version, reference.fingerprint, suite.definition_json
    )


def key(item: CatalogItem) -> tuple[str, str]:
    return item.reference.identity, item.reference.version


class MySQLAssetCatalog:
    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    async def list(self, scope: DraftScope, query: CatalogQuery) -> CatalogPage:
        # Original assets are shared; suite derivatives belong to their author organization.
        DraftScope(scope.organization_id, scope.operator_user_id)
        table = TABLES[query.kind]
        identity, version = columns(query.kind)
        # utf8mb4_bin pads trailing spaces. Use the same NO PAD binary comparison for
        # SQL keyset ordering as Python when merging the two bundled suite entries.
        identity_key, version_key = (
            column.collate("utf8mb4_0900_bin") for column in (identity, version)
        )
        statement = select(table)
        if query.kind == "suite":
            statement = statement.where(table.c.organization_id.in_((0, scope.organization_id)))
        if query.kind in POLICY_KINDS:
            statement = statement.where(table.c.kind == POLICY_KINDS[query.kind].value)
        if query.identity:
            statement = statement.where(identity_key == query.identity)
        after = query.after()
        if after:
            statement = statement.where(
                or_(identity_key > after[0], and_(identity_key == after[0], version_key > after[1]))
            )
        statement = statement.order_by(identity_key, version_key).limit(query.limit + 1)
        async with self.transactions.open() as db:
            rows = (await db.execute(statement)).mappings().all()
            items = [
                (await suite_detail(db, row, scope)).item
                if query.kind == "suite"
                else (await asyncio.to_thread(decode, query.kind, row)).item
                for row in rows
            ]
        page = tuple(items[: query.limit])
        cursor = query.next_cursor(page[-1].reference) if len(items) > query.limit else ""
        return CatalogPage(page, cursor)

    async def get(
        self, scope: DraftScope, kind: AssetKind, identity: str, version: str
    ) -> CatalogDetail:
        DraftScope(scope.organization_id, scope.operator_user_id)
        if kind not in KINDS:
            raise ValueError("Invalid catalog kind")
        validate_identity(identity)
        validate_version(version)
        table = TABLES[kind]
        first, second = columns(kind)
        statement = select(table)
        if kind == "suite":
            statement = statement.where(table.c.organization_id.in_((0, scope.organization_id)))
        if kind in POLICY_KINDS:
            statement = statement.where(table.c.kind == POLICY_KINDS[kind].value)
        async with self.transactions.open() as db:
            row = (
                (
                    await db.execute(
                        statement.where(
                            first.collate("utf8mb4_0900_bin") == identity,
                            second.collate("utf8mb4_0900_bin") == version,
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is not None and kind == "suite":
                return await suite_detail(db, row, scope)
        if row is None:
            raise NotFound()
        return await asyncio.to_thread(decode, kind, row)
