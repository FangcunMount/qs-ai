"""Import the verified QS baseline as assets only, without approving or activating it."""

import argparse
import asyncio
import json

from qs_ai.application.interpretation.prompt_assets import PromptAssets
from qs_ai.config import Settings
from qs_ai.domain.governance.prompt import PromptAsset
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.prompt_assets import MySQLPromptAssets
from qs_ai.infrastructure.qs_server.prompts import load_prompt, prompt_directory


def baseline_assets() -> tuple[str, tuple[PromptAsset, ...]]:
    directory = prompt_directory()
    manifest = json.loads((directory / "manifest.json").read_bytes())
    assets = []
    template_id = "cross-dimension-participant-scale"
    for version in ("v6",):
        package = load_prompt(template_id, version, directory=directory)
        filename = f"{version}.json"
        raw = (directory / filename).read_bytes().decode("utf-8")
        assets.append(
            PromptAsset(template_id, version, package.fingerprint, manifest["files"][filename], raw)
        )
    return f"qs-server:{manifest['commit']}", tuple(assets)


async def run(imported_by: str) -> int:
    source, assets = baseline_assets()
    settings = Settings()
    if settings.database_url is None:
        raise ValueError("Database is not configured")
    database = Database(settings.database_url.get_secret_value())
    try:
        store: PromptAssets = MySQLPromptAssets(Transactions(database))
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
