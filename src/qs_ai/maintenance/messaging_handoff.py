"""Explicit dry-run/apply CLI; no service, Broker, or schema lifecycle."""

import argparse
import asyncio
import json
import os
import stat
import sys
from pathlib import Path
from typing import Any

from qs_ai.bootstrap.messaging import read_key
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.messaging import MessagingStore
from qs_ai.infrastructure.workflow_transport.state_events import StateEventRecorder
from qs_ai.maintenance.legacy_results import (
    ApplyError,
    HandoffError,
    ResultHandoff,
    identities,
    validate_manifest,
)
from qs_ai.maintenance.schema_layout import handoff_tables


def read_manifest(path: str) -> dict[str, Any]:
    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise HandoffError("Duplicate manifest field")
            result[key] = value
        return result

    fd = os.open(Path(path), os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise HandoffError("Manifest must be a regular file")
        raw = stream.read(1_048_577)
    if len(raw) > 1_048_576:
        raise HandoffError("Manifest exceeds maintenance input limit")
    result = json.loads(raw, object_pairs_hook=object_pairs)
    if not isinstance(result, dict):
        raise HandoffError("Manifest must contain one object")
    return result


async def run(args: argparse.Namespace) -> int:
    source_sha = os.environ.get("QS_AI_RELEASE_SHA", "development")
    database: Database | None = None
    try:
        if args.action == "dry-run":
            ids = identities(args.event_id)
            if args.manifest or args.reviewed_digest:
                raise HandoffError("Dry-run reads only selected original identities")
            manifest = None
        else:
            if args.event_id or not args.manifest or not args.reviewed_digest:
                raise HandoffError("Apply requires the reviewed manifest and digest")
            manifest = validate_manifest(read_manifest(args.manifest), args.reviewed_digest)
            if not args.all_claimers_stopped_and_admission_closed:
                raise HandoffError("Stopped claimers and closed admission must be attested")
            if not args.signing_key_file or not args.qs_recipient_key_file:
                raise HandoffError("Original configured messaging keys required")
        url = os.environ.get("QS_AI_MESSAGING_HANDOFF_DATABASE_URL")
        if not url or not url.startswith("mysql+asyncmy://"):
            raise HandoffError("Explicit original MySQL maintenance database required")
        # The one-shot host CLI owns its pool and loop. ResultHandoff borrows them.
        database = Database(url)
        transactions = Transactions(database)
        if args.action == "dry-run":
            outcome = await ResultHandoff(transactions).dry_run(ids)
        else:
            if manifest is None:
                raise HandoffError("Reviewed manifest required")
            signing = read_key(args.signing_key_file, private=True)
            recipient = read_key(args.qs_recipient_key_file, private=False)
            result_table, outbox_table = handoff_tables(manifest["header"]["schema_head"])
            recorder = StateEventRecorder(
                MessagingStore(outbox_table=outbox_table),
                signing,
                recipient,
                result_table=result_table,
            )
            outcome = await ResultHandoff(transactions, recorder).apply(
                manifest,
                args.reviewed_digest,
                all_claimers_stopped_and_admission_closed=True,
            )
        print(json.dumps({"source_sha": source_sha, "action": args.action, "result": outcome}))
        return 0
    except ApplyError as error:
        print(json.dumps({"source_sha": source_sha, "action": args.action, "result": error.result}))
        print(
            "Legacy result apply stopped; retain partial or commit-unknown outcome", file=sys.stderr
        )
        return 1
    except Exception:
        # Database/key/body diagnostics can contain secrets; never emit the exception.
        print(
            "Legacy result maintenance failed; no complete transfer can be inferred",
            file=sys.stderr,
        )
        return 1
    finally:
        if database is not None:
            try:
                await database.close()
            except Exception:
                print("Owned maintenance database cleanup failed", file=sys.stderr)
                return 1


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reviewed original result transfer; no Broker or task execution"
    )
    parser.add_argument("--action", choices=("dry-run", "apply"), required=True)
    parser.add_argument("--event-id", action="append", default=[])
    parser.add_argument("--manifest")
    parser.add_argument("--reviewed-digest")
    parser.add_argument("--all-claimers-stopped-and-admission-closed", action="store_true")
    parser.add_argument("--signing-key-file")
    parser.add_argument("--qs-recipient-key-file")
    raise SystemExit(asyncio.run(run(parser.parse_args())))


if __name__ == "__main__":
    main()
