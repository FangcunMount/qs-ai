"""Release probe. Print database versions only; never connection details."""

import argparse
import asyncio
import json

from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from qs_ai.config import Settings


async def check(require_head: bool) -> dict:
    settings = Settings()
    if settings.database_url is None:
        raise RuntimeError("Database configuration is missing")
    scripts = ScriptDirectory.from_config(Config("alembic.ini"))
    expected = sorted(scripts.get_heads())
    engine = create_async_engine(settings.database_url.get_secret_value())
    try:
        async with engine.connect() as connection:
            version = str(await connection.scalar(text("SELECT VERSION()")))
            if int(version.split(".")[0]) < 8 or "mariadb" in version.lower():
                raise RuntimeError("Production requires compatible MySQL 8 or newer")
            current = sorted(
                await connection.run_sync(
                    lambda conn: MigrationContext.configure(conn).get_current_heads()
                )
            )
            for revision in current:
                scripts.get_revision(revision)
            if require_head and current != expected:
                raise RuntimeError("Database migration version does not match the image")
            return {"mysql_version": version, "current": current, "expected": expected}
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--require-head", action="store_true")
    args = parser.parse_args()
    try:
        result = asyncio.run(check(args.require_head))
    except Exception as error:
        # SQLAlchemy/driver errors can include credentials and SQL parameters.
        driver = getattr(error, "orig", error)
        code = driver.args[0] if driver.args and isinstance(driver.args[0], int) else None
        print(
            json.dumps(
                {
                    "database_check": "failed",
                    "error_type": type(error).__name__,
                    "driver_code": code,
                }
            )
        )
        raise SystemExit(1) from None
    print(json.dumps(result))


if __name__ == "__main__":
    main()
