"""0039 isolated consolidation and 0040 naming. These functions are versioned."""

from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy.engine import Connection

from qs_ai.maintenance.schema_refactor import contracts, v0038
from qs_ai.maintenance.schema_refactor.conversion import copy_data, verify
from qs_ai.maintenance.schema_refactor.layouts import (
    INTERMEDIATE_HEAD,
    OLD_HEAD,
    RENAMES,
    identifier,
    physical,
    qualified,
)


def consolidate(conn: Connection) -> None:
    source = str(conn.scalar(sa.text("SELECT DATABASE()")))
    if source == "ai":
        raise ValueError("Production ai requires isolated schema_refactor maintenance cutover")
    # All DDL is on an isolated clone; never on the active production namespace.
    suffix = uuid4().hex[:12]
    stage, archive = "ai_refactor_stage_" + suffix, "ai_refactor_old_" + suffix
    if not conn.info.get("source_column_collations"):
        conn.info["source_column_collations"] = contracts.source_column_collations(
            conn, source, OLD_HEAD
        )
    inherited = conn.info.get("legacy_prompt_collations")
    conn.info["legacy_prompt_collations"] = inherited or contracts.legacy_collations(
        conn, source, OLD_HEAD
    )
    contracts.create_schema(conn, stage, source)
    contracts.create_schema(conn, archive, source)
    contracts.create_final(conn, stage, INTERMEDIATE_HEAD)
    copy_data(conn, source, stage, OLD_HEAD, INTERMEDIATE_HEAD)
    verify(conn, source, stage, OLD_HEAD, INTERMEDIATE_HEAD)
    source_names = sorted(v0038.metadata.tables)
    target_names = sorted({physical(name, INTERMEDIATE_HEAD) for name in source_names})
    pairs = [f"{qualified(source, t)} TO {qualified(archive, t)}" for t in source_names]
    pairs += [f"{qualified(stage, t)} TO {qualified(source, t)}" for t in target_names]
    conn.execute(sa.text("RENAME TABLE " + ",".join(pairs)))
    # The source's Alembic version table stays in place; Alembic advances it itself.
    verify(conn, archive, source, OLD_HEAD, INTERMEDIATE_HEAD)
    conn.execute(sa.text(f"DROP DATABASE {identifier(stage)}"))
    conn.execute(sa.text(f"DROP DATABASE {identifier(archive)}"))


def module_names(conn: Connection) -> None:
    schema = str(conn.scalar(sa.text("SELECT DATABASE()")))
    pairs = [
        f"{qualified(schema, old)} TO {qualified(schema, new)}" for old, new in RENAMES.items()
    ]
    conn.execute(sa.text("RENAME TABLE " + ",".join(pairs)))
