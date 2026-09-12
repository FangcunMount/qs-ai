from typing import Protocol

from qs_ai.domain.governance.schema import SchemaAsset


class SchemaReader(Protocol):
    async def get(self, schema_id: str, version: str, /) -> SchemaAsset | None: ...


class SchemaAssets(SchemaReader, Protocol):
    async def put(self, asset: SchemaAsset, source_ref: str, imported_by: str) -> bool:
        """Insert immutable content; return False for an identical replay."""
        ...
