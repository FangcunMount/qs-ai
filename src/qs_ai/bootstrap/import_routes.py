"""Import the verified QS baseline as assets only, without approving or activating it."""

import argparse
import asyncio
import json

from qs_ai.application.interpretation.route_assets import RouteAssets
from qs_ai.config import Settings
from qs_ai.domain.governance.route import RouteAsset
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.route_assets import MySQLRouteAssets
from qs_ai.infrastructure.qs_server.routes import load_route


def baseline_assets() -> tuple[str, tuple[RouteAsset, ...]]:
    route = load_route("balanced_text_v1", "v8")
    return "qs-server:runtime-observed-2026-09-11", (
        RouteAsset(route.route, route.revision, route.fingerprint(), route.definition_json()),
    )


async def run(imported_by: str) -> int:
    source, assets = baseline_assets()
    settings = Settings()
    if settings.database_url is None:
        raise ValueError("Database is not configured")
    database = Database(settings.database_url.get_secret_value())
    try:
        store: RouteAssets = MySQLRouteAssets(Transactions(database))
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
