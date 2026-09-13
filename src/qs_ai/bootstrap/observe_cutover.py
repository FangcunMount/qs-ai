"""Read selected-Profile operational statistics; does not establish business acceptance."""

import argparse
import asyncio
import json
from datetime import datetime

from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.observation import observe


async def run(profile: str, since: datetime, until: datetime) -> dict:
    settings = Settings()
    if settings.database_url is None:
        raise ValueError("Database is not configured")
    database = Database(settings.database_url.get_secret_value(), pool_size=1, max_overflow=0)
    try:
        return await observe(Transactions(database), profile, since, until)
    finally:
        await database.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile-fingerprint", required=True)
    parser.add_argument("--since", required=True, type=datetime.fromisoformat)
    parser.add_argument("--until", required=True, type=datetime.fromisoformat)
    args = parser.parse_args()
    try:
        value = asyncio.run(run(args.profile_fingerprint, args.since, args.until))
    except Exception as error:
        print(json.dumps({"observation": "failed", "error_type": type(error).__name__}))
        raise SystemExit(1) from None
    print(json.dumps(value))


if __name__ == "__main__":
    main()
