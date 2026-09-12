"""Read immutable assets and bundled/registered suites without disclosing command audits."""

import hashlib
from dataclasses import fields

from sqlalchemy import RowMapping, Table, and_, or_, select

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
from qs_ai.domain.governance.manifest import AssetReference
from qs_ai.domain.governance.profile import ProfileAsset
from qs_ai.domain.governance.prompt import PromptAsset
from qs_ai.domain.governance.route import RouteAsset
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_suites import decode_record
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_suites,
    profile_assets,
    prompt_assets,
    route_assets,
    schema_assets,
)
from qs_ai.infrastructure.qs_server.evaluation_suite import SUITE_FILES, load_suite

TABLES: dict[AssetKind, Table] = {
    "profile": profile_assets,
    "prompt": prompt_assets,
    "route": route_assets,
    "schema": schema_assets,
    "suite": evaluation_suites,
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


def decode(kind: AssetKind, row: RowMapping) -> CatalogDetail:
    if kind == "suite":
        suite, _ = decode_record(row)
        return detail(
            kind,
            suite.reference.id,
            suite.reference.version,
            suite.reference.fingerprint,
            suite.definition_json,
        )
    cls = TYPES[kind]
    asset = cls(**{field.name: row[field.name] for field in fields(cls)})
    keys = list(TABLES[kind].primary_key.columns)
    raw = asset.package_json if isinstance(asset, PromptAsset) else asset.definition_json
    return detail(kind, row[keys[0].name], row[keys[1].name], asset.fingerprint, raw)


def bundled() -> tuple[CatalogDetail, ...]:
    return tuple(
        detail("suite", ref.id, ref.version, ref.fingerprint, load_suite(ref).definition_json)
        for ref in SUITE_FILES
    )


def key(item: CatalogItem) -> tuple[str, str]:
    return item.reference.identity, item.reference.version


class MySQLAssetCatalog:
    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    async def list(self, scope: DraftScope, query: CatalogQuery) -> CatalogPage:
        # Scope proves a trusted caller supplied actor context, not an organization partition:
        # definitions are shared just as in QS. Command receipts and Runs remain scoped.
        DraftScope(scope.organization_id, scope.operator_user_id)
        table = TABLES[query.kind]
        identity, version = list(table.primary_key.columns)
        # utf8mb4_bin pads trailing spaces. Use the same NO PAD binary comparison for
        # SQL keyset ordering as Python when merging the two bundled suite entries.
        identity_key, version_key = (
            column.collate("utf8mb4_0900_bin") for column in (identity, version)
        )
        statement = select(table)
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
            items = [decode(query.kind, row).item for row in rows]
        if query.kind == "suite":
            items += [
                value.item
                for value in bundled()
                if (not query.identity or value.item.reference.identity == query.identity)
                and (after is None or key(value.item) > after)
            ]
        items.sort(key=key)
        if len({key(item) for item in items}) != len(items):
            raise ValueError("Catalog has conflicting suite sources")
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
        first, second = list(table.primary_key.columns)
        async with self.transactions.open() as db:
            row = (
                (
                    await db.execute(
                        select(table).where(
                            first.collate("utf8mb4_0900_bin") == identity,
                            second.collate("utf8mb4_0900_bin") == version,
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
        built_in = (
            next((value for value in bundled() if key(value.item) == (identity, version)), None)
            if kind == "suite"
            else None
        )
        if built_in:
            if row is not None:
                raise ValueError("Catalog has conflicting suite sources")
            return built_in
        if row is None:
            raise NotFound()
        return decode(kind, row)
