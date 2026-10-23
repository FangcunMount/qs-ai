"""Fail the workflow unless every test passed exactly once on each MySQL version."""

import argparse
import json
from collections import Counter
from pathlib import Path


def verify(
    records: list[dict], revision: str, versions: list[str], count: int, max_attempt: int = 1
) -> int:
    if (
        type(max_attempt) is not int
        or max_attempt < 1
        or not records
        or not versions
        or len(set(versions)) != len(versions)
    ):
        raise ValueError("Invalid coverage attempt or version inventory")
    grouped: dict[tuple[str, int], dict[int, dict]] = {}
    original_collection = None
    for record in records:
        attempt = record.get("run_attempt")
        index = record.get("index")
        if (
            record.get("revision") != revision
            or record.get("mysql") not in versions
            or type(index) is not int
            or not 0 <= index < count
            or type(record.get("count")) is not int
            or record.get("count") != count
            or type(attempt) is not int
            or not 1 <= attempt <= max_attempt
        ):
            raise ValueError("Shard source, version, index or attempt differs")
        collected, selected = record.get("collected"), record.get("selected")
        if (
            not isinstance(collected, list)
            or not collected
            or len(collected) != len(set(collected))
            or not isinstance(selected, list)
            or not selected
            or len(selected) != len(set(selected))
            or not set(selected) <= set(collected)
        ):
            raise ValueError("Invalid historical collection or partition")
        if original_collection is None:
            original_collection = set(collected)
        if set(collected) != original_collection:
            raise ValueError("Collection changed between frozen source attempts")
        history = grouped.setdefault((record["mysql"], index), {})
        if attempt in history or (
            history and Counter(selected) != Counter(next(iter(history.values()))["selected"])
        ):
            raise ValueError("Duplicate attempt or changed historical partition")
        history[attempt] = record
    if len(grouped) != len(versions) * count:
        raise ValueError("Missing, duplicate or unexpected shard manifest")
    # Choose the newest attempt, never the newest passing attempt. Old failures
    # remain in separate artifacts; a newer failure must still block this run.
    records = [history[max(history)] for history in grouped.values()]
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
    parser.add_argument("--attempt", type=int, default=1)
    args = parser.parse_args()
    records = [json.loads(path.read_text()) for path in args.directory.rglob("*.json")]
    total = verify(records, args.revision, args.mysql, args.count, args.attempt)
    print(f"Verified {total} tests exactly once on each of {len(args.mysql)} MySQL versions")


if __name__ == "__main__":
    main()
