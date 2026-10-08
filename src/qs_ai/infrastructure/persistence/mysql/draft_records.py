"""Closed Prompt/Semantic projections; heads and immutable byte history stay separate."""

from dataclasses import dataclass
from typing import Any

import sqlalchemy as sa
from sqlalchemy.sql.selectable import Subquery

from qs_ai.infrastructure.persistence.mysql.asset_records import (
    raw_bytes,
    replace_columns,
    text_bytes,
)
from qs_ai.infrastructure.persistence.mysql.schema import draft_heads, draft_versions


@dataclass(frozen=True)
class DraftSpec:
    kind: str
    history: bool = False

    def id_column(self) -> Any:
        return draft_heads.c[f"{self.kind}_draft_id_key"]

    def columns(self) -> dict[str, Any]:
        table = draft_versions if self.history else draft_heads
        result = {
            "draft_row_id": table.c.draft_row_id,
            "draft_id": self.id_column(),
            "organization_id": table.c.organization_id,
            "revision": table.c.revision,
        }
        if self.history:
            result.update(
                snapshot_json=text_bytes(draft_versions.c.snapshot_bytes),
                snapshot_sha256=draft_versions.c.snapshot_sha256,
            )
            if self.kind == "prompt":
                result.update(
                    command_id=draft_versions.c.prompt_command_id_key,
                    operator_user_id=draft_versions.c.operator_user_id,
                    request_json=text_bytes(draft_versions.c.request_bytes),
                )
        return result

    def predicate(self) -> Any:
        predicate = draft_heads.c.draft_kind == self.kind
        if self.history:
            predicate = sa.and_(
                predicate,
                draft_versions.c.draft_kind == self.kind,
                draft_versions.c.draft_row_id == draft_heads.c.draft_row_id,
                draft_versions.c.organization_id == draft_heads.c.organization_id,
            )
        return predicate

    def values(self, values: dict[str, Any], *, insert: bool) -> dict[str, Any]:
        result = dict(values)
        allowed = {"revision"}
        if self.history:
            allowed.update(("snapshot_json", "snapshot_sha256"))
            if self.kind == "prompt":
                allowed.update(("command_id", "operator_user_id", "request_json"))
        if insert:
            allowed.update(("draft_id", "organization_id"))
        if any(name not in allowed for name in result):
            raise ValueError("Draft kind, row identity and scope are fixed by the typed adapter")
        if not self.history:
            if insert:
                result["draft_kind"] = self.kind
            if any(
                name not in draft_heads.c or draft_heads.c[name].computed is not None
                for name in result
            ):
                raise ValueError("Unsupported draft head field")
            return result
        for old, new in (("snapshot_json", "snapshot_bytes"), ("request_json", "request_bytes")):
            if old in result:
                result[new] = raw_bytes(result.pop(old))
        if insert:
            identity = result.pop("draft_id")
            organization = result["organization_id"]
            result["draft_row_id"] = (
                sa.select(draft_heads.c.draft_row_id)
                .where(
                    draft_heads.c.draft_kind == self.kind,
                    draft_heads.c.organization_id == organization,
                    self.id_column() == identity,
                )
                .scalar_subquery()
            )
            result["draft_kind"] = self.kind
        if any(
            name not in draft_versions.c or draft_versions.c[name].computed is not None
            for name in result
        ):
            raise ValueError("Unsupported draft version field")
        return result


DRAFT_SPECS: dict[Subquery, DraftSpec] = {}


def _projection(name: str, spec: DraftSpec) -> Subquery:
    source: Any = draft_heads
    if spec.history:
        source = draft_versions.join(
            draft_heads,
            sa.and_(
                draft_versions.c.draft_row_id == draft_heads.c.draft_row_id,
                draft_versions.c.organization_id == draft_heads.c.organization_id,
            ),
        )
    projection = (
        sa.select(*(column.label(key) for key, column in spec.columns().items()))
        .select_from(source)
        .where(spec.predicate())
        .subquery(name)
    )
    DRAFT_SPECS[projection] = spec
    return projection


prompt_drafts = _projection("typed_prompt_drafts", DraftSpec("prompt"))
prompt_draft_revisions = _projection("typed_prompt_draft_revisions", DraftSpec("prompt", True))
semantic_draft_heads = _projection("typed_semantic_draft_heads", DraftSpec("semantic"))
semantic_draft_versions = _projection("typed_semantic_draft_versions", DraftSpec("semantic", True))


class DraftInsert:
    def __init__(self, projection: Any) -> None:
        self.spec = DRAFT_SPECS[projection]

    def values(self, *args: Any, **values: Any) -> Any:
        if args:
            if len(args) != 1 or not isinstance(args[0], dict) or values:
                raise ValueError("Draft insertion requires one field mapping")
            values = args[0]
        table = draft_versions if self.spec.history else draft_heads
        return sa.insert(table).values(**self.spec.values(values, insert=True))


class DraftMutation:
    def __init__(self, projection: Any, statement: Any) -> None:
        self.projection, self.spec, self.statement = projection, DRAFT_SPECS[projection], statement

    def __clause_element__(self) -> Any:
        return self.statement

    def _execute_on_connection(self, connection: Any, *args: Any, **kwargs: Any) -> Any:
        return self.statement._execute_on_connection(connection, *args, **kwargs)

    def where(self, *conditions: Any) -> "DraftMutation":
        statement = self.statement.where(
            *(
                replace_columns(condition, self.projection, self.spec.columns())
                for condition in conditions
            )
        )
        return DraftMutation(self.projection, statement)

    def values(self, *args: Any, **values: Any) -> "DraftMutation":
        if args:
            if len(args) != 1 or not isinstance(args[0], dict) or values:
                raise ValueError("Draft update requires one field mapping")
            values = args[0]
        return DraftMutation(
            self.projection, self.statement.values(**self.spec.values(values, insert=False))
        )

    def compile(self, *args: Any, **kwargs: Any) -> Any:
        return self.statement.compile(*args, **kwargs)


def draft_insert(projection: Any) -> DraftInsert:
    return DraftInsert(projection)


def draft_update(projection: Any) -> DraftMutation:
    spec = DRAFT_SPECS[projection]
    return DraftMutation(
        projection,
        sa.update(draft_versions if spec.history else draft_heads).where(spec.predicate()),
    )


def draft_delete(projection: Any) -> DraftMutation:
    spec = DRAFT_SPECS[projection]
    return DraftMutation(
        projection,
        sa.delete(draft_versions if spec.history else draft_heads).where(spec.predicate()),
    )


def draft_head_select(projection: Any, *conditions: Any, revision_only: bool = False) -> Any:
    """Select physical head rows so FOR UPDATE locks the canonical CAS row."""
    spec = DRAFT_SPECS[projection]
    if spec.history:
        raise ValueError("A draft head projection is required")
    columns = spec.columns()
    selected = (
        [columns["revision"]]
        if revision_only
        else [column.label(name) for name, column in columns.items()]
    )
    return sa.select(*selected).where(
        spec.predicate(),
        *(replace_columns(condition, projection, columns) for condition in conditions),
    )
