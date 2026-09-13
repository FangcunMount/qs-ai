"""Fail the workflow unless every test passed exactly once on each MySQL version."""

import argparse
import json
from collections import Counter
from pathlib import Path


def verify(records: list[dict], revision: str, versions: list[str], count: int) -> int:
    if len(records) != len(versions) * count or len(set(versions)) != len(versions):
        raise ValueError("Missing, duplicate or unexpected shard manifest")
    expected = None
    for mysql in versions:
        shards = [record for record in records if record["mysql"] == mysql]
        if sorted(record["index"] for record in shards) != list(range(count)):
            raise ValueError("Missing or duplicated shard index")
        combined: Counter = Counter()
        for record in shards:
            collected = record["collected"]
            if not collected or len(collected) != len(set(collected)):
                raise ValueError("Empty or duplicated collection")
            if expected is None:
                expected = set(collected)
            if (
                set(collected) != expected
                or record["revision"] != revision
                or record["count"] != count
                or record["exitstatus"] != 0
                or record["skipped"]
                or not record["selected"]
                or Counter(record["selected"]) != Counter(record["passed"])
            ):
                raise ValueError("Shard revision, collection or execution proof differs")
            combined.update(record["passed"])
        if combined != Counter(expected):
            raise ValueError("Tests missing or executed more than once on a MySQL version")
    return len(expected or ())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--mysql", nargs="+", required=True)
    args = parser.parse_args()
    records = [json.loads(path.read_text()) for path in args.directory.rglob("*.json")]
    total = verify(records, args.revision, args.mysql, args.count)
    print(f"Verified {total} tests exactly once on each of {len(args.mysql)} MySQL versions")


if __name__ == "__main__":
    main()
