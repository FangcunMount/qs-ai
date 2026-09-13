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


def baseline_assets(*, include_evaluation: bool = False) -> tuple[str, tuple[RouteAsset, ...]]:
    routes = [load_route("balanced_text_v1", "v8")]
    if include_evaluation:
        routes.append(load_route("semantic_judge_v1", "v5"))
    observed = "2026-09-13" if include_evaluation else "2026-09-11"
    return f"qs-server:runtime-observed-{observed}", tuple(
        RouteAsset(route.route, route.revision, route.fingerprint(), route.definition_json())
        for route in routes
    )


async def run(imported_by: str, *, include_evaluation: bool = False) -> int:
    source, assets = baseline_assets(include_evaluation=include_evaluation)
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
    parser.add_argument("--include-evaluation", action="store_true")
    args = parser.parse_args()
    try:
        inserted = asyncio.run(run(args.imported_by, include_evaluation=args.include_evaluation))
    except Exception as error:
        print(json.dumps({"import": "failed", "error_type": type(error).__name__}))
        raise SystemExit(1) from None
    print(json.dumps({"import": "complete", "inserted": inserted, "activated": False}))


if __name__ == "__main__":
    main()
