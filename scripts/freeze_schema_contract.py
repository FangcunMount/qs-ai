"""Explicit development-only generation of the immutable 0040 contract."""

import json
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.dialects import mysql

from qs_ai.infrastructure.persistence.mysql.messaging import metadata as messaging
from qs_ai.infrastructure.persistence.mysql.schema import metadata as business
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences

owned = sa.MetaData()
for source in (business, messaging):
    for table in source.tables.values():
        table.to_metadata(owned)
assert evaluation_sequences.name in owned.tables
assert len(owned.tables) == 43
dialect = mysql.dialect()
tables = []
for table in owned.sorted_tables:
    indexes = [
        {"name": i.name, "columns": [c.name for c in i.columns], "unique": bool(i.unique)}
        for i in table.indexes
    ]
    indexes += [
        {"name": c.name, "columns": [col.name for col in c.columns], "unique": True}
        for c in table.constraints
        if isinstance(c, sa.UniqueConstraint)
    ]
    covered = [i["columns"] for i in indexes] + [[c.name for c in table.primary_key]]
    foreign_keys = []
    for fk in table.foreign_key_constraints:
        columns = [c.name for c in fk.columns]
        if not any(existing[: len(columns)] == columns for existing in covered):
            indexes.append({"name": fk.name, "columns": columns, "unique": False})
            covered.append(columns)
        foreign_keys.append(
            {
                "name": fk.name,
                "columns": columns,
                "table": fk.elements[0].column.table.name,
                "referred_columns": [f.column.name for f in fk.elements],
                "ondelete": fk.ondelete,
            }
        )
    tables.append(
        {
            "name": table.name,
            "create_sql": str(sa.schema.CreateTable(table).compile(dialect=dialect)),
            "index_sql": [
                str(sa.schema.CreateIndex(i).compile(dialect=dialect))
                for i in sorted(table.indexes, key=lambda i: i.name)
            ],
            "columns": [
                {
                    "name": c.name,
                    "type": str(c.type.compile(dialect=dialect)),
                    "nullable": c.nullable,
                    "collation": getattr(c.type, "collation", None),
                    "default": str(c.server_default.arg)
                    if c.server_default is not None and c.computed is None
                    else None,
                    "computed": str(c.computed.sqltext) if c.computed is not None else None,
                    "persisted": c.computed.persisted if c.computed is not None else None,
                }
                for c in table.columns
            ],
            "primary_key": [c.name for c in table.primary_key],
            "indexes": indexes,
            "foreign_keys": foreign_keys,
            "checks": [
                {"name": c.name, "sql": str(c.sqltext)}
                for c in table.constraints
                if isinstance(c, sa.CheckConstraint)
            ],
        }
    )
destination = Path("src/qs_ai/maintenance/schema_refactor/v0040.json")
destination.write_text(
    json.dumps({"head": "0040_module_table_names", "tables": tables}, indent=2) + "\n"
)
print("Frozen 43 owned tables")
