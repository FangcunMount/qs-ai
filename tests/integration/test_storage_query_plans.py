"""Real typed SQL keeps indexed access after physical table consolidation."""

import os
import re
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa

from qs_ai.application.governance.asset_catalog import CatalogQuery
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.domain.evaluation.assets import PolicyKind
from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.domain.governance.manifest import AssetReference
from qs_ai.domain.governance.profile import ProfileAsset
from qs_ai.domain.governance.prompt import PromptAsset
from qs_ai.domain.governance.route import RouteAsset
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.infrastructure.persistence.mysql import asset_records as assets
from qs_ai.infrastructure.persistence.mysql import completion_records as completions
from qs_ai.infrastructure.persistence.mysql import draft_records as drafts
from qs_ai.infrastructure.persistence.mysql import governance_records as typed
from qs_ai.infrastructure.persistence.mysql import schema
from qs_ai.infrastructure.persistence.mysql.asset_catalog import MySQLAssetCatalog
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import AssetSnapshotReader
from qs_ai.infrastructure.persistence.mysql.evaluation_asset_registry import (
    read_policy,
    read_semantic_prompt,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_completion import (
    _blocked_execution,
)
from qs_ai.infrastructure.persistence.mysql.prompt_drafts import read_draft, replay
from qs_ai.infrastructure.persistence.mysql.semantic_drafts import read as read_semantic_draft
from qs_ai.maintenance.schema_refactor import contracts
from qs_ai.maintenance.schema_refactor.layouts import NEW_HEAD, identifier

pytestmark = pytest.mark.integration
COUNT = 1200
OWNER = 17
FINGERPRINT = "sha256:" + "a" * 64
RUN = UUID("00000000-0000-4000-8000-000000000001")
DRAFT = UUID("00000000-0000-4000-8000-000000000600")


class Captured(Exception):
    def __init__(self, statement):
        self.statement = statement


class CaptureDatabase:
    """Stop at real adapter SQL; avoid introducing a second query implementation."""

    def __init__(self, head=None):
        self.head = head

    async def execute(self, statement):
        if self.head is not None:
            head, self.head = self.head, None
            return SimpleNamespace(
                mappings=lambda: SimpleNamespace(one_or_none=lambda: head),
                scalar_one_or_none=lambda: head["revision"],
            )
        raise Captured(statement)

    @asynccontextmanager
    async def open(self):
        yield self


async def capture(call):
    with pytest.raises(Captured) as result:
        await call
    return result.value.statement


@pytest.fixture(params=["utf8mb4_general_ci", "utf8mb4_0900_ai_ci"])
async def plans(request):
    dsn = os.getenv("QS_AI_SCHEMA_TEST_SERVER")
    if not dsn:
        pytest.skip("Requires this run's disposable schema-conversion MySQL service")
    engine = sa.create_engine(dsn)
    name = "ai_query_plans_" + uuid4().hex[:12]
    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(
            sa.text(
                f"CREATE DATABASE {identifier(name)} CHARACTER SET utf8mb4 "
                "COLLATE utf8mb4_0900_ai_ci"
            )
        )
        try:
            conn.info["legacy_prompt_collations"] = {
                "draft_id": request.param,
                "command_id": request.param,
            }
            contracts.create_final(conn, name)
            conn.execute(sa.text(f"USE {identifier(name)}"))
            conn.execute(
                sa.text(
                    "CREATE TABLE alembic_version "
                    "(version_num VARCHAR(32) NOT NULL PRIMARY KEY) ENGINE=InnoDB"
                )
            )
            conn.execute(sa.text("INSERT INTO alembic_version VALUES (:head)"), {"head": NEW_HEAD})
            seed(conn)
            yield conn
        finally:
            conn.execute(sa.text("USE mysql"))
            conn.execute(sa.text(f"DROP DATABASE {identifier(name)}"))
    engine.dispose()


def seed(conn):
    kinds = ("profile", "prompt", "route", "schema", "execution_policy", "gate_policy")
    kinds += ("semantic_prompt",)
    rows = []
    for kind in kinds:
        for index in range(COUNT):
            rows.append(
                dict(
                    asset_kind=kind,
                    owner_organization_id=OWNER if kind == "semantic_prompt" else 0,
                    asset_id=f"asset-{index:06}",
                    version="v1",
                    fingerprint=FINGERPRINT,
                    body_format="prompt_package_json"
                    if kind == "prompt"
                    else "semantic_markdown"
                    if kind == "semantic_prompt"
                    else "definition_json",
                    body_bytes=b"{}",
                    package_sha256="a" * 64 if kind == "prompt" else None,
                    source_ref="query-plan-fixture",
                    imported_by="query-plan-fixture",
                )
            )
    conn.execute(sa.insert(schema.asset_versions), rows)
    for kind in ("prompt", "semantic"):
        conn.execute(
            sa.insert(schema.draft_heads),
            [
                dict(
                    draft_kind=kind,
                    organization_id=OWNER,
                    draft_id=f"00000000-0000-4000-8000-{index:012}",
                    revision=3,
                )
                for index in range(COUNT)
            ],
        )
    histories = []
    for row_id, kind in conn.execute(
        sa.select(schema.draft_heads.c.draft_row_id, schema.draft_heads.c.draft_kind)
    ):
        for revision in range(1, 4):
            histories.append(
                dict(
                    draft_row_id=row_id,
                    revision=revision,
                    draft_kind=kind,
                    organization_id=OWNER,
                    snapshot_bytes=b"{}",
                    snapshot_sha256="a" * 64,
                    command_id=str(UUID(int=row_id * 10 + revision)) if kind == "prompt" else None,
                    operator_user_id=42 if kind == "prompt" else None,
                    request_bytes=b"{}" if kind == "prompt" else None,
                )
            )
    conn.execute(sa.insert(schema.draft_versions), histories)
    rows = []
    for kind in ("generation", "semantic"):
        for index in range(COUNT):
            rows.append(
                dict(
                    kind=kind,
                    run_id=str(RUN),
                    execution_id=f"execution-{index:06}",
                    invocation_id=f"invocation-{index:06}",
                    case_id=f"case-{index:06}" if kind == "generation" else None,
                    slot_ordinal=1 if kind == "generation" else None,
                    execution_ordinal=1 if kind == "generation" else index % 2 + 1,
                    candidate_id=f"candidate-{index if kind == 'generation' else index // 2:06}",
                    candidate_json={} if kind == "generation" else sa.null(),
                    evidence_json={},
                    result_json=sa.null() if kind == "generation" else {},
                    raw_output=b"raw",
                    normalized_output=b"normalized",
                )
            )
    conn.execute(sa.insert(schema.evaluation_completions), rows)
    for table in (
        schema.asset_versions,
        schema.draft_heads,
        schema.draft_versions,
        schema.evaluation_completions,
    ):
        conn.execute(sa.text("ANALYZE TABLE " + identifier(table.name)))


def explain(conn, statement):
    sql = str(statement.compile(dialect=conn.dialect, compile_kwargs={"literal_binds": True}))
    result = conn.exec_driver_sql("EXPLAIN " + sql).mappings().all()
    print([(row["table"], row["type"], row["key"], row["rows"], row["Extra"]) for row in result])
    assert all(row["type"] not in ("ALL", "index") for row in result), (sql, result)
    assert all(row["key"] for row in result), (sql, result)
    return result


def read_index(table, *columns):
    return next(
        index.name
        for index in table.indexes
        if tuple(column.name for column in index.columns) == columns
    )


async def test_typed_asset_exact_reads_use_natural_key_indexes(plans):
    for projection, asset_type in (
        (assets.profile_assets, ProfileAsset),
        (assets.prompt_assets, PromptAsset),
        (assets.route_assets, RouteAsset),
        (assets.schema_assets, SchemaAsset),
    ):
        statement = await capture(
            AssetSnapshotReader(CaptureDatabase(), projection, asset_type).get("asset-000600", "v1")
        )
        rows = explain(plans, statement)
        assert all(int(row["rows"]) <= 2 for row in rows)
    reference = FrozenContractRef("asset-000600", "v1", FINGERPRINT)
    for kind in (PolicyKind.EXECUTION, PolicyKind.GATE):
        rows = explain(plans, await capture(read_policy(CaptureDatabase(), kind, reference)))
        assert all(int(row["rows"]) <= 2 for row in rows)
    rows = explain(
        plans,
        await capture(
            read_semantic_prompt(
                CaptureDatabase(),
                reference,
                owner_organization_id=OWNER,
                requesting_organization_id=OWNER,
            )
        ),
    )
    assert all(int(row["rows"]) <= 2 for row in rows)


async def test_real_catalog_exact_and_cursor_reads_use_ordered_catalog_index(plans):
    scope = DraftScope(OWNER, 42)
    index = read_index(
        schema.asset_versions,
        "asset_kind",
        "owner_organization_id",
        "asset_id",
        "catalog_version_key",
    )
    for kind in ("profile", "prompt", "route", "schema", "execution_policy", "gate_policy"):
        catalog = MySQLAssetCatalog(CaptureDatabase())
        rows = explain(plans, await capture(catalog.get(scope, kind, "asset-000600", "v1")))
        assert all(row["key"] == index for row in rows)
        assert all(int(row["rows"]) <= 2 for row in rows)
        query = CatalogQuery(kind)
        cursor = query.next_cursor(AssetReference("asset-000599", "v1", FINGERPRINT, "a" * 64))
        for page in (query, CatalogQuery(kind, cursor=cursor)):
            statement = await capture(catalog.list(scope, page))
            rows = explain(plans, statement)
            assert all(row["key"] == index for row in rows)
            assert all("filesort" not in (row["Extra"] or "").lower() for row in rows)
            if kind == "profile":
                sql = str(
                    statement.compile(dialect=plans.dialect, compile_kwargs={"literal_binds": True})
                )
                actual = plans.exec_driver_sql("EXPLAIN ANALYZE " + sql).scalar_one()
                assert "Table scan" not in actual
                assert re.search(r"\(actual time=[^\n]+ rows=21 loops=1\)", actual), actual
                print(actual)


async def test_real_draft_locks_history_and_cas_use_identity_and_primary_keys(plans):
    scope = DraftScope(OWNER, 42)
    for kind, projection in (
        ("prompt", drafts.prompt_drafts),
        ("semantic", drafts.semantic_draft_heads),
    ):
        read = read_draft if kind == "prompt" else read_semantic_draft
        rows = explain(plans, await capture(read(CaptureDatabase(), scope, DRAFT, lock=True)))
        assert all(int(row["rows"]) <= 1 for row in rows)
        head = {"revision": 3, "organization_id": OWNER}
        rows = explain(plans, await capture(read(CaptureDatabase(head), scope, DRAFT, 2)))
        assert all(int(row["rows"]) <= 1 for row in rows)
        statement = (
            typed.update(projection)
            .where(
                projection.c.organization_id == OWNER,
                projection.c.draft_id == str(DRAFT),
                projection.c.revision == 3,
            )
            .values(revision=4)
        )
        rows = explain(plans, statement)
        assert all(int(row["rows"]) <= 1 for row in rows)
    statement = await capture(replay(CaptureDatabase(), scope, UUID(int=6011), "{}"))
    rows = explain(plans, statement)
    assert all(int(row["rows"]) <= 1 for row in rows)


async def test_completion_candidate_and_slot_reads_use_kind_prefixed_read_indexes(plans):
    for table in (completions.generation_completions, completions.semantic_completions):
        statement = (
            table.select()
            .where(table.c.run_id == str(RUN), table.c.candidate_id == "candidate-000300")
            .order_by(table.c.execution_ordinal)
        )
        rows = explain(plans, statement)
        assert all(
            row["key"]
            == read_index(
                schema.evaluation_completions,
                "kind",
                "run_id",
                "candidate_id",
                "execution_ordinal",
            )
            for row in rows
        )
        assert all(int(row["rows"]) <= 2 for row in rows)
        assert all("filesort" not in (row["Extra"] or "").lower() for row in rows)
    for cause in ("generation_budget_exhausted", "semantic_budget_exhausted"):
        action = SimpleNamespace(
            cause=cause,
            case_id="case-000600",
            slot_ordinal=1,
            candidate_id="candidate-000300",
            execution_ordinal=2,
        )
        statement = await capture(_blocked_execution(CaptureDatabase(), RUN, {}, cause, (action,)))
        rows = explain(plans, statement)
        columns = (
            ("kind", "run_id", "case_id", "slot_ordinal", "execution_ordinal")
            if cause.startswith("generation")
            else ("kind", "run_id", "candidate_id", "execution_ordinal")
        )
        assert all(
            row["key"] == read_index(schema.evaluation_completions, *columns) for row in rows
        )
        assert all(int(row["rows"]) <= 1 for row in rows)
