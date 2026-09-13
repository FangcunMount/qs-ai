"""Read-only reconciliation of the fixed migration baseline. Not a publication gate."""

import argparse
import asyncio
import json
from dataclasses import asdict

from qs_ai.application.interpretation.asset_audit import audit_assets, audit_routes, audit_schemas
from qs_ai.bootstrap.import_profiles import baseline_assets as profile_baseline
from qs_ai.bootstrap.import_prompts import baseline_assets as prompt_baseline
from qs_ai.bootstrap.import_routes import baseline_assets as route_baseline
from qs_ai.bootstrap.import_schemas import baseline_assets as schema_baseline
from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.profile_assets import MySQLProfileAssets
from qs_ai.infrastructure.persistence.mysql.prompt_assets import MySQLPromptAssets
from qs_ai.infrastructure.persistence.mysql.route_assets import MySQLRouteAssets
from qs_ai.infrastructure.persistence.mysql.schema_assets import MySQLSchemaAssets


async def run(*, referenced_only: bool = False) -> dict:
    settings = Settings()
    if settings.database_url is None:
        raise ValueError("Database is not configured")
    profile_source, profiles = profile_baseline()
    prompt_source, prompts = prompt_baseline()
    route_source, routes = route_baseline()
    schema_source, schemas = schema_baseline()
    if referenced_only:
        policies = [
            json.loads(profile.definition_json)["generation_policy"] for profile in profiles
        ]
        prompt_refs = {(p["prompt_template_id"], p["prompt_version"]) for p in policies}
        route_refs = {p["provider_route"] for p in policies}
        schema_refs = {
            p[key] for p in policies for key in ("input_schema_version", "output_schema_version")
        }
        prompts = tuple(p for p in prompts if (p.template_id, p.version) in prompt_refs)
        routes = tuple(r for r in routes if r.route in route_refs)
        schemas = tuple(s for s in schemas if f"{s.schema_id}/{s.version}" in schema_refs)
        # The fixed baseline pins route revisions; do not choose among competing versions.
        if len({r.route for r in routes}) != len(routes):
            raise ValueError("Selected baseline has ambiguous route revisions")
    database = Database(settings.database_url.get_secret_value())
    try:
        transactions = Transactions(database)
        mismatches = await audit_assets(
            MySQLProfileAssets(transactions), MySQLPromptAssets(transactions), profiles, prompts
        )
        mismatches += await audit_routes(MySQLRouteAssets(transactions), routes, profiles)
        mismatches += await audit_schemas(MySQLSchemaAssets(transactions), schemas, profiles)
        return {
            "audit": "mismatch" if mismatches else "matched",
            "scope": (
                "fixed_published_profile_references"
                if referenced_only
                else "fixed_profile_prompt_route_schema_baseline"
            ),
            "current_production_inventory_verified": False,
            "profile_source": profile_source,
            "prompt_source": prompt_source,
            "profiles_checked": len(profiles),
            "prompts_checked": len(prompts),
            "routes_checked": len(routes),
            "schemas_checked": len(schemas),
            "schema_source": schema_source,
            "route_source": route_source,
            "mismatches": [asdict(item) for item in mismatches],
            "activated": False,
        }
    finally:
        await database.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--referenced-only",
        action="store_true",
        help="Audit only fixed published Profiles and their referenced dependencies; no deletion",
    )
    args = parser.parse_args()
    try:
        result = asyncio.run(run(referenced_only=args.referenced_only))
    except Exception as error:
        print(json.dumps({"audit": "failed", "error_type": type(error).__name__}))
        raise SystemExit(1) from None
    print(json.dumps(result))
    if result["audit"] != "matched":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
