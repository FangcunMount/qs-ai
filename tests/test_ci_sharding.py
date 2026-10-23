"""Prove partitions conserve coverage and fail closed on missing execution evidence."""

import copy
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts/ci" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


runner, auditor = load("run_tests"), load("verify_test_shards")


def records():
    return [
        dict(
            revision="a" * 40,
            run_attempt=1,
            mysql=mysql,
            index=index,
            count=2,
            collected=["test:a", "test:b"],
            selected=[node],
            passed=[node],
            skipped=[],
            exitstatus=0,
        )
        for mysql in ("8.0.36", "8.4")
        for index, node in enumerate(("test:a", "test:b"))
    ]


def test_partition_is_complete_disjoint_stable_and_balanced():
    ids = [f"tests/test_case.py::test_input[case:{i}]" for i in range(1371)]
    groups = runner.partitions(ids, 4)
    assert groups == runner.partitions(list(reversed(ids)), 4)
    assert sorted(node for group in groups for node in group) == sorted(ids)
    assert max(map(len, groups)) - min(map(len, groups)) <= 1
    assert auditor.verify(records(), "a" * 40, ["8.0.36", "8.4"], 2) == 2


@pytest.mark.parametrize("count", [0, -1, 33])
def test_invalid_partition_rejected(count):
    with pytest.raises(ValueError):
        runner.partitions(["a"], count)


def test_duplicate_collection_rejected():
    with pytest.raises(ValueError):
        runner.partitions(["a", "a"], 2)


@pytest.mark.parametrize(
    "case",
    [
        "missing",
        "duplicate",
        "revision",
        "collection",
        "unexecuted",
        "skipped",
        "failed",
        "count",
        "overlap",
        "unknown_version",
        "duplicate_pass",
    ],
)
def test_gate_rejects_incomplete_or_mixed_proofs(case):
    values = copy.deepcopy(records())
    if case == "missing":
        values.pop()
    elif case == "duplicate":
        values[1] = copy.deepcopy(values[0])
    elif case == "revision":
        values[0]["revision"] = "b" * 40
    elif case == "collection":
        values[0]["collected"] = ["test:a"]
    elif case == "unexecuted":
        values[0]["passed"] = []
    elif case == "skipped":
        values[0]["skipped"] = ["test:a"]
    elif case == "failed":
        values[0]["exitstatus"] = 1
    elif case == "count":
        values[0]["count"] = 3
    elif case == "overlap":
        values[1]["selected"] = values[1]["passed"] = ["test:a"]
    elif case == "unknown_version":
        values[0]["mysql"] = "other"
    else:
        values[0]["passed"] *= 2
    with pytest.raises(ValueError):
        auditor.verify(values, "a" * 40, ["8.0.36", "8.4"], 2)


def run_partition(directory, index, count, run_attempt=1):
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/ci/run_tests.py"),
            "--index",
            str(index),
            "--count",
            str(count),
            "--mysql",
            "8.0.36",
            "--manifest",
            str(directory / f"shard-{index}.json"),
        ],
        cwd=directory,
        env={
            **os.environ,
            "GITHUB_SHA": "a" * 40,
            "GITHUB_RUN_ATTEMPT": str(run_attempt),
            "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        },
        capture_output=True,
        text=True,
        timeout=30,
    )
    return result, json.loads((directory / f"shard-{index}.json").read_text())


def test_actual_pytest_hook_runs_every_case_once_and_records_proof(tmp_path):
    (tmp_path / "test_sample.py").write_text(
        "import pytest\n@pytest.mark.parametrize('value', range(7))\n"
        "def test_sample(value): assert value >= 0\n"
    )
    proofs = []
    for index in range(2):
        result, proof = run_partition(tmp_path, index, 2)
        assert result.returncode == 0, result.stdout + result.stderr
        proofs.append(proof)
        assert (tmp_path / f"shard-{index}.xml").is_file()
    assert auditor.verify(proofs, "a" * 40, ["8.0.36"], 2) == 7


@pytest.mark.parametrize(
    "body",
    [
        "import pytest\ndef test_missing(): pytest.skip('missing runtime')\n",
        "def test_fails(): assert False\n",
    ],
)
def test_actual_missing_or_failed_execution_cannot_pass_gate(tmp_path, body):
    (tmp_path / "test_sample.py").write_text(body)
    result, proof = run_partition(tmp_path, 0, 1)
    assert result.returncode != 0
    with pytest.raises(ValueError):
        auditor.verify([proof], "a" * 40, ["8.0.36"], 1)


def test_latest_frozen_attempt_wins_without_discarding_failed_history():
    values = records()
    older = copy.deepcopy(values[0])
    older.update(passed=[], exitstatus=1)
    values[0]["run_attempt"] = 2
    values.append(older)
    assert auditor.verify(values, "a" * 40, ["8.0.36", "8.4"], 2, 2) == 2
    assert auditor.verify(list(reversed(values)), "a" * 40, ["8.0.36", "8.4"], 2, 2) == 2
    assert values[-1]["exitstatus"] == 1  # history was not mutated or replaced


@pytest.mark.parametrize(
    "case",
    [
        "newer_failure",
        "newer_skip",
        "duplicate_attempt",
        "mixed_source",
        "changed_collection",
        "changed_partition",
        "future",
        "zero",
        "boolean",
        "missing",
    ],
)
def test_history_cannot_hide_newer_failure_or_change_frozen_inventory(case):
    values = records()
    later = copy.deepcopy(values[0])
    later["run_attempt"] = 2
    if case == "newer_failure":
        later.update(passed=[], exitstatus=1)
    elif case == "newer_skip":
        later["skipped"] = later["selected"]
    elif case == "duplicate_attempt":
        values.append(copy.deepcopy(later))
    elif case == "mixed_source":
        later["revision"] = "b" * 40
    elif case == "changed_collection":
        later["collected"].append("test:unexpected")
    elif case == "changed_partition":
        later["selected"] = later["passed"] = ["test:b"]
    elif case == "future":
        later["run_attempt"] = 3
    elif case == "zero":
        later["run_attempt"] = 0
    elif case == "boolean":
        later["run_attempt"] = True
    else:
        del later["run_attempt"]
    values.append(later)
    with pytest.raises(ValueError):
        auditor.verify(values, "a" * 40, ["8.0.36", "8.4"], 2, 2)


def test_actual_pytest_producer_records_each_attempt_without_overwriting_history(tmp_path):
    proofs = []
    for attempt in (1, 2):
        directory = tmp_path / f"attempt-{attempt}"
        directory.mkdir()
        (directory / "test_sample.py").write_text("def test_sample(): assert True\n")
        result, proof = run_partition(directory, 0, 1, run_attempt=attempt)
        assert result.returncode == 0
        assert proof["run_attempt"] == attempt
        proofs.append(proof)
    assert auditor.verify(proofs, "a" * 40, ["8.0.36"], 1, 2) == 1
    assert (tmp_path / "attempt-1/shard-0.json").is_file()
    assert (tmp_path / "attempt-2/shard-0.json").is_file()


def test_real_binding_case_ids_are_stable_across_independent_collections():
    collections = []
    for _ in range(2):
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "--collect-only",
                "-q",
                "tests/test_transaction_binding.py",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0
        nodeids = [
            line
            for line in result.stdout.splitlines()
            if line.startswith("tests/test_transaction_binding.py::")
        ]
        assert nodeids
        collections.append(nodeids)
    assert collections[0] == collections[1]
    assert any("expires_at-expired" in node for node in collections[0])
    assert any("expires_at-far_future" in node for node in collections[0])
