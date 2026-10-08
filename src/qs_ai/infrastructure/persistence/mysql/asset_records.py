"""Typed asset projections and mutations over one immutable physical registry.

Projections are SQL expressions, never database views. Every mutation carries the
same closed type and owner predicate; domain identities remain outside row IDs.
"""

from dataclasses import dataclass
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from sqlalchemy.sql import visitors
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.selectable import Subquery

from qs_ai.infrastructure.persistence.mysql.schema import asset_versions


def text_bytes(column: Any) -> Any:
    return sa.cast(column, mysql.CHAR(charset="utf8mb4"))


def raw_bytes(value: Any) -> Any:
    if isinstance(value, str):
        return value.encode("utf-8")
    return value


@dataclass(frozen=True)
class AssetSpec:
    kind: str
    identity: str
    version: str
    body: str

    def predicate(self) -> Any:
        if self.kind == "policy":
            return sa.and_(
                asset_versions.c.asset_kind.in_(("execution_policy", "gate_policy")),
                asset_versions.c.owner_organization_id == 0,
            )
        predicates = [asset_versions.c.asset_kind == self.kind]
        if self.kind != "semantic_prompt":
            predicates.append(asset_versions.c.owner_organization_id == 0)
        return sa.and_(*predicates)

    def columns(self) -> dict[str, Any]:
        key = (
            asset_versions.c.scoped_id_key
            if self.kind in ("policy", "semantic_prompt")
            else asset_versions.c.native_id_key
        )
        result = {
            self.identity: key,
            self.version: asset_versions.c.version,
            "fingerprint": asset_versions.c.fingerprint,
            self.body: text_bytes(asset_versions.c.body_bytes),
            "source_ref": asset_versions.c.source_ref,
            "imported_by": asset_versions.c.imported_by,
            "created_at": asset_versions.c.created_at,
        }
        if self.kind == "prompt":
            result["package_sha256"] = asset_versions.c.package_sha256
        if self.kind == "policy":
            result["kind"] = sa.case(
                (asset_versions.c.asset_kind == "execution_policy", "execution"), else_="gate"
            ).collate("utf8mb4_bin")
        if self.kind == "semantic_prompt":
            result["organization_id"] = asset_versions.c.owner_organization_id
        return result

    def values(self, values: dict[str, Any], *, insert: bool = True) -> dict[str, Any]:
        source = dict(values)
        kind = self.kind
        if kind == "policy" and insert:
            subtype = source.pop("kind")
            if subtype not in ("execution", "gate"):
                raise ValueError("Unsupported policy kind")
            kind = subtype + "_policy"
        if not insert and any(name in source for name in ("kind", "organization_id")):
            raise ValueError("Asset kind and organization cannot be updated")
        owner = source.pop("organization_id", 0) if kind == "semantic_prompt" else 0
        mapping = {self.identity: "asset_id", self.version: "version", self.body: "body_bytes"}
        result = {
            "asset_kind": kind,
            "owner_organization_id": owner,
            "body_format": (
                "prompt_package_json"
                if kind == "prompt"
                else "semantic_markdown"
                if kind == "semantic_prompt"
                else "definition_json"
            ),
        }
        if not insert:
            result = {}
        for name, value in source.items():
            physical = mapping.get(name, name)
            if (
                physical not in asset_versions.c
                or physical
                in ("asset_row_id", "asset_kind", "owner_organization_id", "body_format")
                or asset_versions.c[physical].computed is not None
            ):
                raise ValueError(f"Unsupported asset field: {name}")
            result[physical] = raw_bytes(value) if physical == "body_bytes" else value
        return result


ASSET_SPECS: dict[Subquery, AssetSpec] = {}


def _projection(name: str, spec: AssetSpec) -> Subquery:
    projection = (
        sa.select(*(column.label(key) for key, column in spec.columns().items()))
        .where(spec.predicate())
        .subquery(name)
    )
    ASSET_SPECS[projection] = spec
    return projection


profile_assets = _projection(
    "typed_profile_assets", AssetSpec("profile", "profile_id", "version", "definition_json")
)
prompt_assets = _projection(
    "typed_prompt_assets", AssetSpec("prompt", "template_id", "version", "package_json")
)
route_assets = _projection(
    "typed_route_assets", AssetSpec("route", "route", "revision", "definition_json")
)
schema_assets = _projection(
    "typed_schema_assets", AssetSpec("schema", "schema_id", "version", "definition_json")
)
evaluation_policy_assets = _projection(
    "typed_evaluation_policy_assets", AssetSpec("policy", "asset_id", "version", "definition_json")
)
semantic_prompt_assets = _projection(
    "typed_semantic_prompt_assets", AssetSpec("semantic_prompt", "asset_id", "version", "markdown")
)


def asset_identity_columns(projection: Any) -> tuple[Any, ...]:
    spec = ASSET_SPECS[projection]
    names = [spec.identity, spec.version]
    if spec.kind == "policy":
        names.insert(0, "kind")
    if spec.kind == "semantic_prompt":
        names.insert(0, "organization_id")
    return tuple(projection.c[name] for name in names)


def replace_columns(
    expression: ColumnElement[Any], projection: Any, columns: dict[str, Any]
) -> ColumnElement[Any]:
    def replace(element: ColumnElement[Any], **kw: Any) -> ColumnElement[Any] | None:
        if getattr(element, "table", None) is projection:
            return columns.get(element.name)
        return None

    return visitors.replacement_traverse(expression, {}, replace)


class AssetInsert:
    def __init__(self, projection: Any) -> None:
        self.spec = ASSET_SPECS[projection]

    def values(self, *args: Any, **values: Any) -> Any:
        if args:
            if len(args) != 1 or not isinstance(args[0], dict) or values:
                raise ValueError("Asset insertion requires one field mapping")
            values = args[0]
        return sa.insert(asset_versions).values(**self.spec.values(values))


class AssetMutation:
    def __init__(self, projection: Any, statement: Any) -> None:
        self.projection, self.spec, self.statement = projection, ASSET_SPECS[projection], statement

    def __clause_element__(self) -> Any:
        return self.statement

    def _execute_on_connection(self, connection: Any, *args: Any, **kwargs: Any) -> Any:
        return self.statement._execute_on_connection(connection, *args, **kwargs)

    def where(self, *conditions: Any) -> "AssetMutation":
        statement = self.statement.where(
            *(
                replace_columns(condition, self.projection, self.spec.columns())
                for condition in conditions
            )
        )
        return AssetMutation(self.projection, statement)

    def values(self, *args: Any, **values: Any) -> "AssetMutation":
        if args:
            if len(args) != 1 or not isinstance(args[0], dict) or values:
                raise ValueError("Asset update requires one field mapping")
            values = args[0]
        mapped = self.spec.values(values, insert=False)
        # Kind, scope and format are fixed by the projection, never caller overrides.
        return AssetMutation(self.projection, self.statement.values(**mapped))

    def compile(self, *args: Any, **kwargs: Any) -> Any:
        return self.statement.compile(*args, **kwargs)


def asset_insert(projection: Any) -> AssetInsert:
    return AssetInsert(projection)


def asset_update(projection: Any) -> AssetMutation:
    return AssetMutation(
        projection, sa.update(asset_versions).where(ASSET_SPECS[projection].predicate())
    )


def asset_delete(projection: Any) -> AssetMutation:
    return AssetMutation(
        projection, sa.delete(asset_versions).where(ASSET_SPECS[projection].predicate())
    )


def asset_select(projection: Any, *conditions: Any) -> Any:
    """Read physical rows with typed labels, including canonical row locks."""
    spec = ASSET_SPECS[projection]
    columns = spec.columns()
    return sa.select(*(value.label(name) for name, value in columns.items())).where(
        spec.predicate(),
        *(replace_columns(condition, projection, columns) for condition in conditions),
    )
