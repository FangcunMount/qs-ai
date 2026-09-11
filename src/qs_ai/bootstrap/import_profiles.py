"""Import the verified QS baseline as assets only, without approving or activating it."""

import argparse
import asyncio
import json

from qs_ai.application.interpretation.profile_assets import ProfileAssets
from qs_ai.config import Settings
from qs_ai.domain.governance.profile import ProfileAsset
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.profile_assets import MySQLProfileAssets
from qs_ai.infrastructure.qs_server.profiles import decode_published_profile
from qs_ai.infrastructure.qs_server.prompts import prompt_directory


def baseline_assets() -> tuple[str, tuple[ProfileAsset, ...]]:
    baseline = json.loads((prompt_directory() / "published-profile-baseline.json").read_bytes())
    source_ref = f"qs-server:{baseline['qs_deployed_sha']}"
    assets = []
    for envelope in baseline["profiles"]:
        release = decode_published_profile(envelope)
        policy = release.input_policy
        assets.append(
            ProfileAsset(
                policy.profile_id,
                policy.profile_version,
                policy.profile_fingerprint,
                release.definition_json,
            )
        )
    return source_ref, tuple(assets)


async def run(imported_by: str) -> int:
    source, assets = baseline_assets()
    settings = Settings()
    if settings.database_url is None:
        raise ValueError("Database is not configured")
    database = Database(settings.database_url.get_secret_value())
    try:
        store: ProfileAssets = MySQLProfileAssets(Transactions(database))
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
