"""Bounded current-organization references from authoritative immutable records."""

import hashlib
from dataclasses import asdict
from typing import Any

from sqlalchemy import and_, func, literal, or_, select
from sqlalchemy.sql.elements import ColumnElement

from qs_ai.application.governance.asset_references import ReferenceQuery
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.domain.evaluation.assets import PolicyKind
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_asset_registry import read_policy
from qs_ai.infrastructure.persistence.mysql.schema import (
    configuration_publications,
    evaluation_runs,
    evaluation_suites,
)


class MySQLPolicyReferences:
    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    async def get(self, scope: DraftScope, query: ReferenceQuery) -> dict[str, Any]:
        version: ColumnElement[Any]
        visible: ColumnElement[bool]
        checksum: ColumnElement[Any]
        after = query.after(scope.organization_id)
        if query.usage_kind == "suite":
            table = evaluation_suites
            identity, version, raw = table.c.suite_id, table.c.suite_version, table.c.contracts_json
            checksum = table.c.contracts_sha256
            path = "$"
            visible = table.c.organization_id.in_((0, scope.organization_id))
        elif query.usage_kind == "evaluation":
            table = evaluation_runs
            identity, version, raw = table.c.run_id, literal(""), table.c.definition_json
            checksum = literal(None)
            path = "$.release"
            visible = table.c.organization_id == scope.organization_id
        else:
            table = configuration_publications
            identity, version, raw = table.c.publication_id, literal(""), table.c.content_json
            checksum = table.c.content_sha256
            path = "$.evidence.release"
            visible = table.c.organization_id == scope.organization_id
        conditions: list[ColumnElement[bool]] = [visible]
        for field, expected in asdict(query.reference).items():
            conditions.append(
                func.json_unquote(func.json_extract(raw, f"{path}.{query.kind}.{field}"))
                == expected
            )
        if after:
            conditions.append(
                or_(identity > after[0], and_(identity == after[0], version > after[1]))
            )
        async with self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            await read_policy(
                db,
                PolicyKind.EXECUTION if query.kind == "execution_policy" else PolicyKind.GATE,
                query.reference,
            )
            rows = (
                (
                    await db.execute(
                        select(
                            identity.label("identity"),
                            version.label("version"),
                            raw.label("raw"),
                            checksum.label("checksum"),
                        )
                        .where(*conditions)
                        .order_by(identity, version)
                        .limit(query.limit + 1)
                    )
                )
                .mappings()
                .all()
            )
        for row in rows:
            digest = hashlib.sha256(row["raw"].encode()).hexdigest()
            if query.usage_kind != "evaluation" and row["checksum"] != digest:
                raise ValueError("referenced asset checksum mismatch")
        page = rows[: query.limit]
        return {
            "kind": query.kind,
            "reference": asdict(query.reference),
            "usage_kind": query.usage_kind,
            "items": [
                {
                    "identity": row["identity"],
                    "version": row["version"],
                    "content_sha256": hashlib.sha256(row["raw"].encode()).hexdigest(),
                }
                for row in page
            ],
            "next_cursor": query.next_cursor(
                scope.organization_id, page[-1]["identity"], page[-1]["version"]
            )
            if len(rows) > query.limit
            else "",
        }
