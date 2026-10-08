"""DML dispatcher used by typed storage and isolated corruption/cleanup fixtures.

A typed SQL projection is not writable itself. Mutation dispatch always selects
its closed physical kind/scope and replaces projection columns with physical ones.
"""

from typing import Any

import sqlalchemy as sa

from qs_ai.infrastructure.persistence.mysql.asset_records import (
    ASSET_SPECS,
    asset_delete,
    asset_insert,
    asset_update,
)
from qs_ai.infrastructure.persistence.mysql.draft_records import (
    DRAFT_SPECS,
    draft_delete,
    draft_insert,
    draft_update,
)


def insert(target: Any) -> Any:
    if target in ASSET_SPECS:
        return asset_insert(target)
    if target in DRAFT_SPECS:
        return draft_insert(target)
    return sa.insert(target)


def update(target: Any) -> Any:
    if target in ASSET_SPECS:
        return asset_update(target)
    if target in DRAFT_SPECS:
        return draft_update(target)
    return sa.update(target)


def delete(target: Any) -> Any:
    if target in ASSET_SPECS:
        return asset_delete(target)
    if target in DRAFT_SPECS:
        return draft_delete(target)
    return sa.delete(target)


def identity_columns(target: Any) -> tuple[Any, ...]:
    """Logical legacy keys, excluding physical row IDs and discriminators."""
    if target in ASSET_SPECS:
        from qs_ai.infrastructure.persistence.mysql.asset_records import asset_identity_columns

        return asset_identity_columns(target)
    return tuple(target.primary_key.columns)
