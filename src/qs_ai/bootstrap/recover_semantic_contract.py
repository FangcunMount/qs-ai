"""Trusted-host maintenance command: preview, then explicitly authorize one bounded retry."""

import argparse
import asyncio
import json
import os
import pwd
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select

from qs_ai.config import Settings
from qs_ai.domain.evaluation.contract_recovery import ContractRecovery
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_contract_recovery import (
    authorize_contract_recovery,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_projection import decode_semantic_completion
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_semantic_completions


async def run(args: argparse.Namespace) -> dict:
    url = Settings().database_url
    if url is None:
        raise ValueError("Database is not configured")
    database = Database(url.get_secret_value())
    try:
        async with Transactions(database).open() as db:
            row = (
                (
                    await db.execute(
                        select(evaluation_semantic_completions).where(
                            evaluation_semantic_completions.c.run_id == str(args.run_id),
                            evaluation_semantic_completions.c.execution_id == args.execution_id,
                        )
                    )
                )
                .mappings()
                .one()
            )
            target = decode_semantic_completion(row)
            if args.apply and (
                not args.acknowledge_policy_exception_and_cost
                or args.expected_output_fingerprint != target.output_fingerprint
            ):
                raise ValueError("Apply requires acknowledgement and preview output fingerprint")
            value = ContractRecovery(
                target.execution_id,
                target.candidate_id,
                target.candidate_output_fingerprint,
                target.output_fingerprint,
                "operator:" + pwd.getpwuid(os.geteuid()).pw_name,
                args.reason,
                datetime.now(UTC),
                True,
            )
            state = await authorize_contract_recovery(
                db,
                args.run_id,
                args.expected_version,
                args.organization_id,
                args.expected_release_fingerprint,
                value,
                confirm=True,
            )
            result = {
                "mode": "applied" if args.apply else "preview_rolled_back",
                "run_id": str(args.run_id),
                "expected_version": args.expected_version,
                "result_version": state.version,
                "execution_id": value.execution_id,
                "candidate_id": value.candidate_id,
                "output_fingerprint": value.output_fingerprint,
                "release_fingerprint": args.expected_release_fingerprint,
                "actor": value.actor,
                "recovery_version": value.recovery_version,
                "instruction_fingerprint": value.instruction_fingerprint,
                "reason": value.reason,
                "next_action": "retry_same_candidate_semantic_once",
                "existing_generation_outputs": "preserved",
            }
            if args.apply:
                await db.commit()
            return result
    finally:
        await database.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", type=UUID, required=True)
    parser.add_argument("--organization-id", type=int, required=True)
    parser.add_argument("--expected-version", type=int, required=True)
    parser.add_argument("--execution-id", required=True)
    parser.add_argument("--expected-release-fingerprint", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-output-fingerprint")
    parser.add_argument("--acknowledge-policy-exception-and-cost", action="store_true")
    args = parser.parse_args()
    try:
        print(json.dumps(asyncio.run(run(args)), ensure_ascii=False))
    except Exception as error:
        # Driver errors can contain credentials/SQL. No raw traceback in operator logs.
        print(json.dumps({"status": "rejected", "error_type": type(error).__name__}))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
