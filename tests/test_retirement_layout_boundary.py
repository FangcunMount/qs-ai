"""Historical asset retirement never expands its whitelist to merged storage."""

import pytest

from scripts.retirement.core import Stop
from scripts.retirement.stores import MySQLStore


def test_merged_asset_storage_is_rejected_before_rows_or_deletions():
    statements = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def execute(self, sql, *_params):
            statements.append(sql)

        def fetchall(self):
            return [{"TABLE_NAME": "governance_asset_versions"}]

    class Connection:
        def cursor(self):
            return Cursor()

    store = MySQLStore.__new__(MySQLStore)
    store.name, store.database, store.connection = "ai_mysql", "isolated", Connection()
    with pytest.raises(Stop, match="does not support merged storage"):
        store.snapshot()
    assert len(statements) == 1 and "information_schema.TABLES" in statements[0]


def test_missing_alembic_head_cannot_be_inferred_as_legacy_storage():
    statements = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def execute(self, sql, *_params):
            statements.append(sql)

        def fetchall(self):
            return [{"TABLE_NAME": "prompt_assets"}]

    class Connection:
        def cursor(self):
            return Cursor()

    store = MySQLStore.__new__(MySQLStore)
    store.name, store.database, store.connection = "ai_mysql", "isolated", Connection()
    with pytest.raises(Stop, match="recognized 0038"):
        store.snapshot()
    assert len(statements) == 1


@pytest.mark.parametrize("heads", [[], ["0040_module_table_names"], ["unknown"], ["one", "two"]])
def test_unknown_or_new_head_never_uses_legacy_physical_asset_cleanup(heads):
    statements = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def execute(self, sql, *_params):
            statements.append(sql)

        def fetchall(self):
            if len(statements) == 1:
                return [{"TABLE_NAME": "alembic_version"}, {"TABLE_NAME": "prompt_assets"}]
            return [{"version_num": head} for head in heads]

    class Connection:
        def cursor(self):
            return Cursor()

    store = MySQLStore.__new__(MySQLStore)
    store.name, store.database, store.connection = "ai_mysql", "isolated", Connection()
    with pytest.raises(Stop, match="recognized 0038"):
        store.snapshot()
    assert len(statements) == 2 and statements[-1] == "SELECT version_num FROM alembic_version"
