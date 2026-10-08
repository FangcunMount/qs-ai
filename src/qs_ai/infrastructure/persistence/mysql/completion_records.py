"""Typed access to terminal evidence; every operation retains its original kind scope."""

from dataclasses import dataclass
from typing import Any, Literal

import sqlalchemy as sa

from qs_ai.infrastructure.persistence.mysql.schema import evaluation_completions


@dataclass(frozen=True)
class CompletionRecords:
    kind: Literal["generation", "semantic"]

    @property
    def c(self) -> Any:
        return evaluation_completions.c

    def select(self, *columns: Any) -> Any:
        return sa.select(*(columns or (evaluation_completions,))).where(
            evaluation_completions.c.kind == self.kind
        )

    def insert(self) -> Any:
        return sa.insert(evaluation_completions).values(kind=self.kind)

    def update(self) -> Any:
        return sa.update(evaluation_completions).where(evaluation_completions.c.kind == self.kind)

    def delete(self) -> Any:
        return sa.delete(evaluation_completions).where(evaluation_completions.c.kind == self.kind)


generation_completions = CompletionRecords("generation")
semantic_completions = CompletionRecords("semantic")


def select_records(table: sa.Table | CompletionRecords, *columns: Any) -> Any:
    """Mixed dispatch/completion readers retain their ordered locking and bounded reads."""
    if isinstance(table, CompletionRecords):
        return table.select(*columns)
    return sa.select(*(columns or (table,)))


def delete_records(table: sa.Table | CompletionRecords) -> Any:
    """Fixture cleanup remains scoped when a list includes both evidence kinds."""
    if isinstance(table, CompletionRecords):
        return table.delete()
    return sa.delete(table)
