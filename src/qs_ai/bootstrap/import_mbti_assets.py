"""Atomically initialize the pinned MBTI root bytes; never approve or publish."""

import argparse
import asyncio
import json
import re
from dataclasses import asdict
from typing import Any

from sqlalchemy import Table, insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.bootstrap.import_evaluation_suites import install_baseline
from qs_ai.config import Settings
from qs_ai.domain.evaluation.assets import PolicyKind
from qs_ai.domain.governance.profile import AssetConflict, ProfileAsset
from qs_ai.domain.governance.prompt import PromptAsset
from qs_ai.domain.governance.route import RouteAsset
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import AssetSnapshotReader
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_asset_registry import read_policy
from qs_ai.infrastructure.persistence.mysql.schema import (
    profile_assets,
    prompt_assets,
    route_assets,
    schema_assets,
    semantic_prompt_assets,
)
from qs_ai.infrastructure.qs_server.evaluation_release import validate_release_assets
from qs_ai.infrastructure.qs_server.mbti_assets import MBTIRootAssets, load_mbti_root
from qs_ai.infrastructure.qs_server.semantic_assets import semantic_assets


async def insert_exact(
    db: AsyncSession, table: Table, values: dict[str, Any], source: str, imported_by: str
) -> bool:
    condition = [column == values[column.name] for column in table.primary_key.columns]
    row = (
        (await db.execute(select(table).where(*condition).with_for_update()))
        .mappings()
        .one_or_none()
    )
    if row is not None:
        if any(row[key] != value for key, value in values.items()):
            raise AssetConflict("MBTI root identity already has different content")
        return False
    await db.execute(insert(table).values(**values, source_ref=source, imported_by=imported_by))
    return True


async def install(
    db: AsyncSession, assets: MBTIRootAssets, source_commit: str, imported_by: str
) -> int:
    """Caller owns one serializable transaction; no partially imported root escapes."""
    if (
        not re.fullmatch(r"[0-9a-f]{40}", source_commit)
        or not imported_by.strip()
        or len(imported_by) > 128
    ):
        raise ValueError("Exact source commit and initializer identity required")
    source = f"qs-ai:{source_commit}:mbti-root:{assets.manifest_sha256}"
    execution = await read_policy(db, PolicyKind.EXECUTION, assets.contracts.execution_policy)
    gate = await read_policy(db, PolicyKind.GATE, assets.contracts.gate_policy)
    schemas = AssetSnapshotReader(db, schema_assets, SchemaAsset)
    ref = assets.contracts.semantic_output_schema
    schema_id, version = ref.version.rsplit("/", 1)
    semantic_schema = await schemas.get(schema_id, version)
    if semantic_schema is None or semantic_schema.fingerprint != ref.fingerprint:
        raise ValueError("Shared semantic output schema unavailable")
    # Reuse the exact existing approved model routes. Initialization cannot create or
    # silently replace them, change quotas, or activate this unreviewed MBTI configuration.
    routes = AssetSnapshotReader(db, route_assets, RouteAsset)
    for ref in (assets.release.generation_route, assets.release.semantic_route):
        route = await routes.get(ref.id, ref.version)
        if route is None or route.fingerprint != ref.fingerprint:
            raise ValueError("MBTI initial model route unavailable or changed")
    entries = (
        (prompt_assets, asdict(assets.prompt)),
        (profile_assets, asdict(assets.profile)),
        (schema_assets, asdict(assets.input_schema)),
        (
            semantic_prompt_assets,
            {
                "organization_id": 0,
                "asset_id": assets.semantic.reference.id,
                "version": assets.semantic.reference.version,
                "fingerprint": assets.semantic.reference.fingerprint,
                "markdown": assets.semantic.markdown,
            },
        ),
    )
    inserted = 0
    for table, values in entries:
        inserted += await insert_exact(db, table, values, source, imported_by)
    inserted += await install_baseline(db, source, assets.suite, assets.contracts, imported_by)
    await validate_release_assets(
        assets.release,
        AssetSnapshotReader(db, profile_assets, ProfileAsset),
        AssetSnapshotReader(db, prompt_assets, PromptAsset),
        routes,
        schemas,
        execution_policy_json=execution.definition_json,
        gate_policy_json=gate.definition_json,
        semantic=semantic_assets(
            assets.semantic.markdown,
            semantic_schema.definition_json,
            assets.semantic.reference,
            assets.contracts.semantic_output_schema,
        ),
        frozen_suite=assets.suite,
    )
    return inserted


async def run(source_commit: str, imported_by: str) -> int:
    assets = await asyncio.to_thread(load_mbti_root)
    settings = Settings()
    if settings.database_url is None:
        raise ValueError("Database required")
    database = Database(settings.database_url.get_secret_value())
    try:
        async with Transactions(database).open() as db:
            await db.connection(execution_options={"isolation_level": "SERIALIZABLE"})
            inserted = await install(db, assets, source_commit, imported_by)
            await db.commit()
        return inserted
    finally:
        await database.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--imported-by", required=True)
    args = parser.parse_args()
    try:
        inserted = asyncio.run(run(args.source_commit, args.imported_by))
    except Exception as error:
        print(json.dumps({"import": "failed", "error_type": type(error).__name__}))
        raise SystemExit(1) from None
    print(json.dumps({"import": "complete", "inserted": inserted, "activated": False}))


if __name__ == "__main__":
    main()
