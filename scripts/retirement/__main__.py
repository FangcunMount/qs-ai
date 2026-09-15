"""Run with uv run --group maintenance python -m scripts.retirement --help."""

import argparse
import json
import os
from pathlib import Path

from scripts.retirement import core
from scripts.retirement.stores import MongoStore, MySQLStore


def main():
    parser = argparse.ArgumentParser(description="M5 fixed-whitelist data retirement")
    parser.add_argument("command", choices=("plan", "backup", "apply", "verify", "restore"))
    parser.add_argument("--directory", required=True, type=Path)
    parser.add_argument("--plan-sha256", default="")
    parser.add_argument("--maintenance-evidence", type=Path)
    args = parser.parse_args()
    adapters = []
    try:
        adapters.append(
            MongoStore(
                os.environ["M5_QS_MONGO_URI"],
                os.environ["M5_QS_MONGO_DATABASE"],
                restore_uri=os.environ.get("M5_RESTORE_MONGO_URI"),
            )
        )
        for name, variable in (("qs_mysql", "M5_QS_MYSQL_URL"), ("ai_mysql", "M5_AI_MYSQL_URL")):
            adapters.append(
                MySQLStore(
                    name,
                    os.environ[variable],
                    restore_url=os.environ.get("M5_RESTORE_MYSQL_URL"),
                )
            )
        if args.command == "plan":
            result = {"plan_sha256": core.plan(args.directory, adapters)}
        elif args.command == "backup":
            result = core.backup(args.directory, adapters, args.plan_sha256)
        elif args.command == "apply":
            if args.maintenance_evidence is None:
                raise core.Stop("maintenance evidence file required")
            result = core.apply(
                args.directory,
                adapters,
                args.plan_sha256,
                core.read(args.maintenance_evidence),
            )
            result = {"status": result["status"], "completed": result["completed"]}
        else:
            _, archive = core.checked_backup(
                args.directory, adapters, args.plan_sha256, require_current=False
            )
            if args.command == "verify":
                for adapter in adapters:
                    adapter.verify_deleted(archive["snapshots"][adapter.name])
                result = {"status": "deleted_and_protected_data_unchanged"}
            else:
                # Restore into new isolated databases only; never overwrite live data.
                result = {
                    a.name: a.verify_restore(archive["snapshots"][a.name], keep=True)
                    for a in adapters
                }
        print(json.dumps(result, sort_keys=True))
        return 0
    except core.Stop as error:
        print(json.dumps({"status": "stopped", "reason": str(error)}))
        return 2
    except Exception as error:
        # Driver exceptions can contain passwords, queries and assessment content.
        print(json.dumps({"status": "stopped", "reason": type(error).__name__}))
        return 2
    finally:
        for adapter in adapters:
            adapter.close()


if __name__ == "__main__":
    raise SystemExit(main())
