"""Deterministic CI partitions; each runner owns its own disposable databases."""

import argparse
import hashlib
import json
import os
from pathlib import Path

import pytest


def partitions(nodeids: list[str], count: int) -> list[list[str]]:
    if not 1 <= count <= 32 or len(nodeids) != len(set(nodeids)):
        raise ValueError("Invalid shard count or duplicate test identities")
    # Spread adjacent parameter cases without relying on Python's randomized hash.
    ordered = sorted(nodeids, key=lambda node: (hashlib.sha256(node.encode()).digest(), node))
    return [sorted(ordered[index::count]) for index in range(count)]


class Shard:
    def __init__(self, index: int, count: int, manifest: Path, revision: str, mysql: str):
        if not 0 <= index < count <= 32:
            raise ValueError("Invalid shard index/count")
        self.index, self.count, self.manifest = index, count, manifest
        self.revision, self.mysql = revision, mysql
        self.collected: list[str] = []
        self.selected: list[str] = []
        self.passed: set[str] = set()
        self.skipped: set[str] = set()

    @pytest.hookimpl(trylast=True)
    def pytest_collection_modifyitems(self, config, items):
        self.collected = sorted(item.nodeid for item in items)
        self.selected = partitions(self.collected, self.count)[self.index]
        selected = set(self.selected)
        excluded = [item for item in items if item.nodeid not in selected]
        items[:] = [item for item in items if item.nodeid in selected]
        config.hook.pytest_deselected(items=excluded)

    def pytest_runtest_logreport(self, report):
        if report.when == "call" and report.passed:
            self.passed.add(report.nodeid)
        if report.skipped:
            self.skipped.add(report.nodeid)

    def pytest_sessionfinish(self, session, exitstatus):
        complete = bool(self.selected) and self.passed == set(self.selected) and not self.skipped
        if exitstatus == 0 and not complete:
            session.exitstatus = pytest.ExitCode.TESTS_FAILED
        self.manifest.parent.mkdir(parents=True, exist_ok=True)
        self.manifest.write_text(
            json.dumps(
                {
                    "revision": self.revision,
                    "mysql": self.mysql,
                    "index": self.index,
                    "count": self.count,
                    "collected": self.collected,
                    "selected": self.selected,
                    "passed": sorted(self.passed),
                    "skipped": sorted(self.skipped),
                    "exitstatus": int(session.exitstatus),
                },
                indent=2,
            )
            + "\n"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index", type=int, required=True)
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--mysql", required=True)
    args = parser.parse_args()
    shard = Shard(args.index, args.count, args.manifest, os.environ["GITHUB_SHA"], args.mysql)
    return int(
        pytest.main(
            ["--durations=25", "--junitxml=" + str(args.manifest.with_suffix(".xml"))],
            plugins=[shard],
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
