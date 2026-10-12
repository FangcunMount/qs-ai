"""Initialize the cap6 root only; never approve, evaluate or activate a release."""

import argparse
import asyncio
import json

from qs_ai.bootstrap.import_evaluation_suites import baseline, install_baseline
from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.qs_server.evaluation_suite import CAP6_ROOT, load_suite


async def run(imported_by: str) -> bool:
    if not imported_by.strip() or len(imported_by) > 128:
        raise ValueError("Initializer identity required")
    _, _, contracts = baseline()
    suite = load_suite(CAP6_ROOT)
    settings = Settings()
    if settings.database_url is None:
        raise ValueError("Database required")
    database = Database(settings.database_url.get_secret_value())
    try:
        async with Transactions(database).open() as db:
            await db.connection(execution_options={"isolation_level": "SERIALIZABLE"})
            inserted = await install_baseline(
                db, "qs-ai:scale-reference-cap6-root/v1", suite, contracts, imported_by
            )
            await db.commit()
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
