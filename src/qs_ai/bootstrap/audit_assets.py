"""Read-only reconciliation of the fixed migration baseline. Not a publication gate."""

import asyncio
import json
from dataclasses import asdict

from qs_ai.application.interpretation.asset_audit import audit_assets, audit_routes
from qs_ai.bootstrap.import_profiles import baseline_assets as profile_baseline
from qs_ai.bootstrap.import_prompts import baseline_assets as prompt_baseline
from qs_ai.bootstrap.import_routes import baseline_assets as route_baseline
from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.profile_assets import MySQLProfileAssets
from qs_ai.infrastructure.persistence.mysql.prompt_assets import MySQLPromptAssets
from qs_ai.infrastructure.persistence.mysql.route_assets import MySQLRouteAssets


async def run() -> dict:
    settings = Settings()
    if settings.database_url is None:
        raise ValueError("Database is not configured")
    profile_source, profiles = profile_baseline()
    prompt_source, prompts = prompt_baseline()
    route_source, routes = route_baseline()
    database = Database(settings.database_url.get_secret_value())
    try:
        transactions = Transactions(database)
        mismatches = await audit_assets(
            MySQLProfileAssets(transactions), MySQLPromptAssets(transactions), profiles, prompts
        )
        mismatches += await audit_routes(MySQLRouteAssets(transactions), routes, profiles)
        return {
            "audit": "mismatch" if mismatches else "matched",
            "scope": "fixed_profile_prompt_route_baseline",
            "profile_source": profile_source,
            "prompt_source": prompt_source,
            "profiles_checked": len(profiles),
            "prompts_checked": len(prompts),
            "routes_checked": len(routes),
            "route_source": route_source,
            "mismatches": [asdict(item) for item in mismatches],
            "activated": False,
        }
    finally:
        await database.close()


def main() -> None:
    try:
        result = asyncio.run(run())
    except Exception as error:
        print(json.dumps({"audit": "failed", "error_type": type(error).__name__}))
        raise SystemExit(1) from None
    print(json.dumps(result))
    if result["audit"] != "matched":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
