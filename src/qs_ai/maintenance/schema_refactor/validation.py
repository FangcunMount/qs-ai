"""Fail-closed full owned-schema validation without creating or repairing tables."""

import re
from collections import Counter
from collections.abc import Mapping
from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine import Connection

from qs_ai.maintenance.schema_refactor import contracts, v0038
from qs_ai.maintenance.schema_refactor.layouts import (
    INTERMEDIATE_HEAD,
    NEW_HEAD,
    OLD_HEAD,
    physical,
)
from qs_ai.maintenance.schema_refactor.validation_contract import auto_increment_columns


def _normal(value: Any) -> str:
    result = str(value or "").lower().replace("`", "")
    result = re.sub(r"_(ascii|utf8mb4|utf8mb3|utf8)'", "'", result)
    result = re.sub(r"\b(integer)\b", "int", result)
    result = re.sub(r"\b(bigint|int|smallint)\(\d+\)", r"\1", result)
    return re.sub(r"\s+|[()]", "", result)


class _SQLExpression:
    """Canonicalize the frozen CHECK/CASE grammar without erasing grouping."""

    def __init__(self, sql: str) -> None:
        token = re.compile(
            r"\s+|'(?:''|\\.|[^'])*'|`(?:``|[^`])*`|[a-z_][a-z0-9_]*|\d+|<>|!=|<=|>=|[=<>(),]", re.I
        )
        self.tokens: list[str] = []
        position = 0
        for match in token.finditer(sql):
            if match.start() != position:
                raise ValueError("Unknown SQL expression syntax")
            position = match.end()
            value = match[0]
            if value.isspace() or re.fullmatch(r"_(ascii|utf8mb4|utf8mb3|utf8)", value, re.I):
                continue
            self.tokens.append(value if value.startswith("'") else value.replace("`", "").lower())
        if position != len(sql):
            raise ValueError("Unknown SQL expression syntax")
        self.position = 0

    def peek(self) -> str | None:
        return self.tokens[self.position] if self.position < len(self.tokens) else None

    def take(self, expected: str | None = None) -> str:
        value = self.peek()
        if value is None or expected is not None and value != expected:
            raise ValueError("Unknown SQL expression structure")
        self.position += 1
        return value

    def boolean(self, operation: str) -> Any:
        read = self.predicate if operation == "and" else lambda: self.boolean("and")
        values = [read()]
        while self.peek() == operation:
            self.take()
            values.append(read())
        flattened = []
        for value in values:
            if isinstance(value, tuple) and value[0] == operation:
                flattened.extend(value[1])
            else:
                flattened.append(value)
        return flattened[0] if len(flattened) == 1 else (operation, tuple(flattened))

    def predicate(self) -> Any:
        if self.peek() == "not":
            self.take()
            return ("not", self.predicate())
        value = self.value()
        operator = self.peek()
        if operator in ("=", "<>", "!=", ">=", "<=", ">", "<"):
            self.take()
            return ("<>" if operator == "!=" else operator, value, self.value())
        if operator == "is":
            self.take()
            negated = self.peek() == "not"
            if negated:
                self.take()
            self.take("null")
            return ("is_not_null" if negated else "is_null", value)
        if operator in ("in", "not"):
            negated = operator == "not"
            if negated:
                self.take()
            self.take("in")
            self.take("(")
            values = [self.value()]
            while self.peek() == ",":
                self.take()
                values.append(self.value())
            self.take(")")
            return ("not_in" if negated else "in", value, tuple(sorted(values, key=repr)))
        return value

    def value(self) -> Any:
        value = self.take()
        if value == "(":
            result = self.boolean("or")
            self.take(")")
            return result
        if value.startswith("'"):
            return ("literal", value[1:-1].replace("''", "'").replace("\\'", "'"))
        if value.isdigit():
            return ("number", int(value))
        if value == "case":
            branches = []
            while self.peek() == "when":
                self.take()
                condition = self.boolean("or")
                self.take("then")
                branches.append((condition, self.value()))
            self.take("else")
            otherwise = self.value()
            self.take("end")
            return ("case", tuple(branches), otherwise)
        if self.peek() == "(":
            self.take()
            values = [self.boolean("or")]
            while self.peek() == ",":
                self.take()
                values.append(self.boolean("or"))
            self.take(")")
            return ("call", value, tuple(values))
        return ("identifier", value)


def _expression(value: Any) -> Any:
    if not value:
        return None
    parser = _SQLExpression(str(value))
    result = parser.boolean("or")
    if parser.peek() is not None:
        raise ValueError("Unknown SQL expression suffix")
    return result


def _type(value: str) -> str:
    value = re.sub(r"CHARACTER SET \w+|COLLATE \w+", "", value, flags=re.I)
    value = re.sub(r"\bBOOL(?:EAN)?\b", "TINYINT(1)", value, flags=re.I)
    return _normal(value)


def _default(value: Any) -> str | None:
    if value is None or str(value).upper() == "NULL":
        return None
    result = str(value)
    if result.startswith("b'"):
        result = result[1:]
    if result.startswith("'") and result.endswith("'"):
        return result[1:-1].replace("''", "'")
    if re.fullmatch(r"current_timestamp(?:\(\d*\))?", result, re.I):
        return result.lower().replace("()", "")
    return result


def _action(value: str | None) -> str:
    result = str(value or "RESTRICT").upper()
    return "RESTRICT" if result == "NO ACTION" else result


def _fk_signature(fk: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        tuple(fk["constrained_columns"]),
        fk["referred_table"],
        tuple(fk["referred_columns"]),
        _action(fk.get("options", {}).get("ondelete")),
        _action(fk.get("options", {}).get("onupdate")),
    )


def _frozen_fks(table: sa.Table) -> set[tuple[Any, ...]]:
    return {
        (
            tuple(column.name for column in fk.columns),
            fk.elements[0].column.table.name,
            tuple(element.column.name for element in fk.elements),
            _action(fk.ondelete),
            _action(fk.onupdate),
        )
        for fk in table.foreign_key_constraints
    }


def _effective_collations(conn: Connection, schema: str) -> dict[tuple[str, str], str]:
    return {
        (table, column): collation
        for table, column, collation in conn.execute(
            sa.text(
                "SELECT TABLE_NAME,COLUMN_NAME,COLLATION_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=:schema AND COLLATION_NAME IS NOT NULL"
            ),
            {"schema": schema},
        )
    }


def _require_index_options(conn: Connection, schema: str) -> None:
    invalid = conn.scalar(
        sa.text(
            "SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=:schema "
            "AND (SUB_PART IS NOT NULL OR IS_VISIBLE<>'YES' OR "
            "INDEX_TYPE<>'BTREE' OR COLLATION='D')"
        ),
        {"schema": schema},
    )
    if invalid:
        raise ValueError("Unknown prefix, invisible, descending or non-BTREE index drift")


def _require_auto_increment(conn: Connection, schema: str, head: str) -> None:
    # EXTRA describes the column attribute; TABLES.AUTO_INCREMENT is a mutable
    # next-value counter and cannot prove whether the identity attribute exists.
    expected = auto_increment_columns(head)
    actual = {
        (table, column)
        for table, column, extra in conn.execute(
            sa.text(
                "SELECT TABLE_NAME,COLUMN_NAME,EXTRA FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=:schema"
            ),
            {"schema": schema},
        )
        if "auto_increment" in extra.lower().split()
    }
    if actual != expected:
        raise ValueError("AUTO_INCREMENT column attributes have drifted")


def _legacy_indexes(table: sa.Table) -> Counter[tuple[tuple[str, ...], bool]]:
    values = [
        (tuple(column.name for column in index.columns), bool(index.unique))
        for index in table.indexes
    ]
    values += [
        (tuple(column.name for column in constraint.columns), True)
        for constraint in table.constraints
        if isinstance(constraint, sa.UniqueConstraint)
    ]
    covered = [columns for columns, _ in values] + [tuple(c.name for c in table.primary_key)]
    for fk in table.foreign_key_constraints:
        columns = tuple(column.name for column in fk.columns)
        if not any(existing[: len(columns)] == columns for existing in covered):
            values.append((columns, False))
            covered.append(columns)
    return Counter(values)


def require_schema(
    conn: Connection,
    head: str = NEW_HEAD,
    schema: str | None = None,
    *,
    column_collations: contracts.ColumnCollations | None = None,
    allow_external_incoming: bool = False,
) -> None:
    schema = schema or str(conn.scalar(sa.text("SELECT DATABASE()")))
    actual = contracts.tables(conn, schema)
    expected = {physical(name, head) for name in v0038.metadata.tables} | {"alembic_version"}
    if actual != expected:
        raise ValueError("Database table set differs from its versioned layout")
    if contracts.head(conn, schema) != head:
        raise ValueError("Database head differs from its versioned layout")
    objects = conn.execute(
        sa.text(
            "SELECT TABLE_NAME,ENGINE,TABLE_TYPE FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA=:schema"
        ),
        {"schema": schema},
    ).all()
    if any(engine != "InnoDB" or kind != "BASE TABLE" for _, engine, kind in objects):
        raise ValueError("Conversion requires only InnoDB base tables")
    _require_auto_increment(conn, schema, head)
    triggers = conn.scalar(
        sa.text("SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA=:schema"),
        {"schema": schema},
    )
    if triggers:
        raise ValueError("Cross-schema exchange cannot move triggers")
    inspector = sa.inspect(conn)
    version_columns = inspector.get_columns("alembic_version", schema=schema)
    if (
        len(version_columns) != 1
        or version_columns[0]["name"] != "version_num"
        or _type(str(version_columns[0]["type"].compile(dialect=conn.dialect))) != "varchar32"
        or version_columns[0]["nullable"]
        or version_columns[0].get("default") is not None
        or inspector.get_pk_constraint("alembic_version", schema=schema)["constrained_columns"]
        != ["version_num"]
        or inspector.get_indexes("alembic_version", schema=schema)
        or inspector.get_foreign_keys("alembic_version", schema=schema)
        or inspector.get_check_constraints("alembic_version", schema=schema)
    ):
        raise ValueError("Migration version table definition has drifted")
    effective = _effective_collations(conn, schema)
    bound = (
        conn.info.get("source_column_collations")
        if column_collations is None
        else column_collations
    )
    bound = contracts.validate_column_collations(bound) if bound else None
    _require_index_options(conn, schema)
    disabled_checks = conn.scalar(
        sa.text(
            "SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS "
            "WHERE CONSTRAINT_SCHEMA=:schema AND CONSTRAINT_TYPE='CHECK' AND ENFORCED<>'YES'"
        ),
        {"schema": schema},
    )
    if disabled_checks:
        raise ValueError("Check constraint enforcement has drifted")
    for name in actual:
        for fk in inspector.get_foreign_keys(name, schema=schema):
            if fk.get("referred_schema") not in (None, schema):
                raise ValueError("Cross-schema foreign key prevents exchange")
    external = conn.scalar(
        sa.text(
            "SELECT COUNT(*) FROM information_schema.KEY_COLUMN_USAGE "
            "WHERE REFERENCED_TABLE_SCHEMA=:schema AND TABLE_SCHEMA<>:schema"
        ),
        {"schema": schema},
    )
    if external and not allow_external_incoming:
        raise ValueError("External foreign key prevents exchange")
    if head == OLD_HEAD:
        # Historical migrations use inherited defaults and unnamed constraints.
        for table in v0038.metadata.tables.values():
            columns = inspector.get_columns(table.name, schema=schema)
            if {c["name"] for c in columns} != set(table.columns.keys()):
                raise ValueError("Legacy column set has drifted")
            for column in columns:
                expected_column = table.c[column["name"]]
                actual_type = str(column["type"].compile(dialect=conn.dialect))
                expected_type = str(expected_column.type.compile(dialect=conn.dialect))
                if (
                    _type(actual_type) != _type(expected_type)
                    or column["nullable"] != expected_column.nullable
                ):
                    raise ValueError("Legacy column definition has drifted")
                expected_collation = getattr(expected_column.type, "collation", None)
                if bound and column["name"] in bound[table.name]:
                    expected_collation = bound[table.name][column["name"]]
                if (
                    expected_collation
                    and effective.get((table.name, column["name"])) != expected_collation
                ):
                    raise ValueError("Legacy explicit collation has drifted")
                default = (
                    str(getattr(expected_column.server_default, "arg", None))
                    if expected_column.server_default is not None
                    else None
                )
                if _default(column.get("default")) != _default(default):
                    raise ValueError(
                        "Legacy default has drifted: " + table.name + "." + column["name"]
                    )
            if inspector.get_pk_constraint(table.name, schema=schema)["constrained_columns"] != [
                c.name for c in table.primary_key
            ]:
                raise ValueError("Legacy primary key has drifted")
            expected_unique = {
                tuple(c.name for c in constraint.columns)
                for constraint in table.constraints
                if isinstance(constraint, sa.UniqueConstraint)
            }
            actual_unique = {
                tuple(index["column_names"])
                for index in inspector.get_indexes(table.name, schema=schema)
                if index["unique"]
            }
            if actual_unique != expected_unique:
                raise ValueError("Legacy uniqueness has drifted")
            legacy_actual_indexes = Counter(
                (tuple(index["column_names"]), bool(index["unique"]))
                for index in inspector.get_indexes(table.name, schema=schema)
            )
            if legacy_actual_indexes != _legacy_indexes(table):
                raise ValueError("Legacy index semantics have drifted: " + table.name)
            if {
                _fk_signature(fk) for fk in inspector.get_foreign_keys(table.name, schema=schema)
            } != _frozen_fks(table):
                raise ValueError("Legacy foreign key has drifted: " + table.name)
            checks = inspector.get_check_constraints(table.name, schema=schema)
            if {_expression(c["sqltext"]) for c in checks} != {
                _expression(str(c.sqltext))
                for c in table.constraints
                if isinstance(c, sa.CheckConstraint)
            }:
                raise ValueError("Legacy check semantics have drifted: " + table.name)
        contracts.legacy_collations(conn, schema, head)
        contracts.source_column_collations(conn, schema, head)
        return
    if head not in (NEW_HEAD, INTERMEDIATE_HEAD):
        raise ValueError("Unsupported schema head")
    legacy = contracts.legacy_collations(conn, schema, head)
    inherited = contracts.final_column_collations(bound, legacy)
    for table in contracts.load()["tables"]:
        name = contracts.renamed(table["name"], head)
        columns = inspector.get_columns(name, schema=schema)
        expected_columns = table["columns"]
        if set(c["name"] for c in columns) != set(c["name"] for c in expected_columns):
            raise ValueError("Column set drift: " + name)
        by_name = {c["name"]: c for c in columns}
        for expected_column in expected_columns:
            column = by_name[expected_column["name"]]
            if column["nullable"] != expected_column["nullable"]:
                raise ValueError("Column nullability drift: " + name)
            actual_type = str(column["type"].compile(dialect=conn.dialect))
            if _type(actual_type) != _type(expected_column["type"]):
                raise ValueError("Column type drift: " + name + "." + column["name"])
            collation = effective.get((name, column["name"]))
            expected_collation = inherited.get(
                (table["name"], expected_column["name"]), expected_column["collation"]
            )
            if expected_collation and collation != expected_collation:
                raise ValueError("Column collation drift: " + name)
            computed = column.get("computed", {})
            if _expression(computed.get("sqltext")) != _expression(expected_column.get("computed")):
                raise ValueError("Generated column drift: " + name)
            if (
                expected_column.get("computed")
                and computed.get("persisted") != expected_column["persisted"]
            ):
                raise ValueError("Generated column storage drift: " + name)
            if not expected_column.get("computed") and _default(column.get("default")) != _default(
                expected_column["default"]
            ):
                raise ValueError("Column default drift: " + name)
        pk = inspector.get_pk_constraint(name, schema=schema)["constrained_columns"]
        if pk != table["primary_key"]:
            raise ValueError("Primary key drift: " + name)
        indexes = inspector.get_indexes(name, schema=schema)
        actual_indexes = {i["name"]: (i["column_names"], bool(i["unique"])) for i in indexes}
        expected_indexes = {i["name"]: (i["columns"], i["unique"]) for i in table["indexes"]}
        if actual_indexes != expected_indexes:
            raise ValueError("Index drift: " + name)
        fks = inspector.get_foreign_keys(name, schema=schema)
        actual_fks = {
            (
                f["name"],
                tuple(f["constrained_columns"]),
                f["referred_table"],
                tuple(f["referred_columns"]),
                str(f.get("options", {}).get("ondelete", "RESTRICT")).upper(),
                _action(f.get("options", {}).get("onupdate")),
            )
            for f in fks
        }
        expected_fks = {
            (
                f["name"],
                tuple(f["columns"]),
                contracts.renamed(f["table"], head),
                tuple(f["referred_columns"]),
                f["ondelete"] or "RESTRICT",
                "RESTRICT",
            )
            for f in table["foreign_keys"]
        }
        if actual_fks != expected_fks:
            raise ValueError("Foreign key drift: " + name)
        checks = inspector.get_check_constraints(name, schema=schema)
        if {c["name"]: _expression(c["sqltext"]) for c in checks} != {
            c["name"]: _expression(c["sql"]) for c in table["checks"]
        }:
            raise ValueError("Check constraint drift: " + name)
