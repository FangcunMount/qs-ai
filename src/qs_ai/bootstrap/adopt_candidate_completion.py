"""Trusted-host acceptance rule change: dry-run rollback by default, explicit apply under CAS."""

import argparse
import asyncio
import json
import os
import pwd
from datetime import UTC, datetime
from uuid import UUID

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.config import Settings
from qs_ai.domain.evaluation.acceptance import RULE_FINGERPRINT
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_acceptance import adopt_acceptance_rule


async def run(args: argparse.Namespace) -> dict:
    if args.apply and (
        not args.confirm_rule_change or args.expected_rule_fingerprint != RULE_FINGERPRINT
    ):
        raise ValueError("Apply requires explicit rule change confirmation")
    url = Settings().database_url
    if url is None:
        raise ValueError("Database is not configured")
    database = Database(url.get_secret_value())
    try:
        async with Transactions(database).open() as db:
            result = await adopt_acceptance_rule(
                db,
                ManagementScope(args.run_id, args.organization_id, args.operator_user_id),
                args.expected_version,
                args.expected_release_fingerprint,
                "operator:" + pwd.getpwuid(os.geteuid()).pw_name,
                args.reason,
                datetime.now(UTC),
                confirm=True,
            )
            if args.apply:
                await db.commit()
            return {"mode": "applied" if args.apply else "preview_rolled_back", **result}
    finally:
        await database.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", type=UUID, required=True)
    parser.add_argument("--organization-id", type=int, required=True)
    parser.add_argument(
        "--operator-user-id",
        type=int,
        required=True,
        help="Existing authorized QS operator id; does not sign a human review",
    )
    parser.add_argument("--expected-version", type=int, required=True)
    parser.add_argument("--expected-release-fingerprint", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-rule-change", action="store_true")
    parser.add_argument("--expected-rule-fingerprint")
    args = parser.parse_args()
    try:
        print(json.dumps(asyncio.run(run(args)), ensure_ascii=False))
    except Exception as error:
        print(json.dumps({"status": "rejected", "error_type": type(error).__name__}))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
