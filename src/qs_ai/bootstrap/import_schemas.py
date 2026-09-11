"""Import the verified QS baseline as assets only, without approving or activating it."""

import argparse
import asyncio
import json

from qs_ai.application.interpretation.schema_assets import SchemaAssets
from qs_ai.config import Settings
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.schema_assets import MySQLSchemaAssets
from qs_ai.infrastructure.qs_server.input_schema import load_input_schema
from qs_ai.infrastructure.qs_server.output import QSOutputParser, schema_directory


def baseline_assets() -> tuple[str, tuple[SchemaAsset, ...]]:
    directory = schema_directory()
    manifest = json.loads((directory / "manifest.json").read_bytes())
    load_input_schema(directory=directory)
    QSOutputParser(directory=directory)
    assets = []
    for kind, checksum in (("input", manifest["input"]["sha256"]), ("output", manifest["sha256"])):
        raw = (directory / f"ai-explanation-{kind}-v1.schema.json").read_bytes().decode("utf-8")
        assets.append(SchemaAsset(f"ai-explanation-{kind}", "v1", "sha256:" + checksum, raw))
    return f"qs-server:{manifest['commit']}", tuple(assets)


async def run(imported_by: str) -> int:
    source, assets = baseline_assets()
    settings = Settings()
    if settings.database_url is None:
        raise ValueError("Database is not configured")
    database = Database(settings.database_url.get_secret_value())
    try:
        store: SchemaAssets = MySQLSchemaAssets(Transactions(database))
        inserted = 0
        for asset in assets:
            inserted += await store.put(asset, source, imported_by)
        return inserted
    finally:
        await database.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--imported-by", required=True)
    args = parser.parse_args()
    try:
        inserted = asyncio.run(run(args.imported_by))
    except Exception as error:
        print(json.dumps({"import": "failed", "error_type": type(error).__name__}))
        raise SystemExit(1) from None
    print(json.dumps({"import": "complete", "inserted": inserted, "activated": False}))


if __name__ == "__main__":
    main()
