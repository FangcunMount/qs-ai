"""Build an unapproved generation manifest from imported assets, without writes."""

import argparse
import asyncio
import json

from qs_ai.application.interpretation.manifest import build_generation_manifest
from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.profile_assets import MySQLProfileAssets
from qs_ai.infrastructure.persistence.mysql.prompt_assets import MySQLPromptAssets
from qs_ai.infrastructure.persistence.mysql.route_assets import MySQLRouteAssets
from qs_ai.infrastructure.persistence.mysql.schema_assets import MySQLSchemaAssets


async def run(profile_id: str, profile_version: str, route_revision: str) -> dict:
    settings = Settings()
    if settings.database_url is None:
        raise ValueError("Database is not configured")
    database = Database(settings.database_url.get_secret_value())
    try:
        transactions = Transactions(database)
        manifest = await build_generation_manifest(
            MySQLProfileAssets(transactions),
            MySQLPromptAssets(transactions),
            MySQLRouteAssets(transactions),
            MySQLSchemaAssets(transactions),
            profile_id=profile_id,
            profile_version=profile_version,
            route_revision=route_revision,
        )
        return {
            "manifest": json.loads(manifest.canonical_json()),
            "fingerprint": manifest.fingerprint(),
            "approved": False,
            "activated": False,
        }
    finally:
        await database.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile-id", required=True)
    parser.add_argument("--profile-version", required=True)
    parser.add_argument("--route-revision", required=True)
    args = parser.parse_args()
    try:
        result = asyncio.run(run(args.profile_id, args.profile_version, args.route_revision))
    except Exception as error:
        print(json.dumps({"manifest": "failed", "error_type": type(error).__name__}))
        raise SystemExit(1) from None
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
