import json
from dataclasses import asdict, fields
from uuid import uuid4

import pytest

from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef
from qs_ai.infrastructure.persistence.mysql.evaluation_creation_receipt import creation_receipt


@pytest.fixture
def stored():
    release = EvidenceReleaseIdentity(
        **{
            f.name: FrozenContractRef(f.name, "v1", "sha256:" + "a" * 64)
            for f in fields(EvidenceReleaseIdentity)
        }
    )
    row = {"run_id": str(uuid4()), "organization_id": 1, "requested_by": "user:42"}
    definition = {
        "schema_version": "qs-ai-evaluation-run-creation/v1",
        "run_id": row["run_id"],
        "release": asdict(release),
        "release_fingerprint": release.fingerprint(),
        "audit": {
            "organization_id": 1,
            "requested_by": "user:42",
            "request_reason": "原始创建原因",
            "created_at": "2026-09-13T00:00:00+00:00",
        },
        "generation_manifest_json": "private executable Prompt content",
        "suite_json": "private test cases",
        "execution_policy_json": "policy bytes",
    }
    return row, definition


def test_receipt_projects_only_frozen_identity_and_creation_audit(stored):
    row, definition = stored
    receipt = json.loads(creation_receipt({**row, "definition_json": json.dumps(definition)}))
    assert set(receipt) == {
        "schema_version",
        "run_id",
        "release",
        "release_fingerprint",
        "requested_by",
        "request_reason",
        "created_at",
    }
    assert receipt["release"] == definition["release"]
    assert receipt["release_fingerprint"] == definition["release_fingerprint"]
    assert receipt["requested_by"] == "user:42"
    assert "private" not in json.dumps(receipt)


@pytest.mark.parametrize(
    "change",
    ["schema", "run", "org", "bool_org", "actor", "reason", "time", "digest", "ref", "missing_ref"],
)
def test_inconsistent_creation_is_not_presented_as_a_recoverable_receipt(stored, change):
    row, definition = stored
    if change == "schema":
        definition["schema_version"] = "other/v1"
    elif change == "run":
        definition["run_id"] = str(uuid4())
    elif change in ("org", "bool_org"):
        definition["audit"]["organization_id"] = True if change == "bool_org" else 2
    elif change == "actor":
        definition["audit"]["requested_by"] = "user:99"
    elif change == "reason":
        definition["audit"]["request_reason"] = "中" * 334
    elif change == "time":
        definition["audit"]["created_at"] = "2026-09-13T00:00:00"
    elif change == "digest":
        definition["release_fingerprint"] = "sha256:" + "0" * 64
    elif change == "ref":
        definition["release"]["suite"]["version"] = "v2"
    else:
        del definition["release"]["semantic_route"]
    with pytest.raises((ValueError, TypeError)):
        creation_receipt({**row, "definition_json": json.dumps(definition)})
