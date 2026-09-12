import json
from dataclasses import replace
from uuid import UUID, uuid4

import pytest

from qs_ai.application.governance.publication import (
    MovePublication,
    PublicationScope,
    PublishConfiguration,
)
from qs_ai.application.governance.publication_codec import (
    canonical,
    publication_json,
    read_publication,
)
from tests.test_generation_manifest import assets as assets
from tests.test_generation_manifest import complete_release as complete_release
from tests.test_generation_manifest import evaluation_release as evaluation_release
from tests.test_publication import evidence as evidence
from tests.test_publication import published


def test_publication_record_roundtrip_preserves_full_proof_and_original_audit(evidence):
    value = published(evidence)
    assert read_publication(publication_json(value)) == value


@pytest.mark.parametrize("field", ["version", "schema", "passed", "checksum", "extra", "oversize"])
def test_record_rejects_corrupt_or_noncanonical_proof(evidence, field):
    data = json.loads(publication_json(published(evidence)))
    proof = data["publication"]["evidence"]
    if field == "version":
        proof["run_version"] = True
    elif field == "schema":
        data["schema_version"] = "unknown"
    elif field == "passed":
        proof["final_review"]["passed"] = 1
    elif field == "checksum":
        proof["manifest"]["prompt"]["content_sha256"] = "0" * 64
    elif field == "extra":
        data["unbound"] = True
    else:
        data["extra"] = "x" * (256 * 1024)
    with pytest.raises(ValueError):
        read_publication(canonical(data))


@pytest.mark.parametrize(
    "field,value",
    [
        ("command_id", UUID(int=0)),
        ("expected_version", True),
        ("expected_version", -1),
        ("expected_active_id", UUID(int=0)),
        ("confirm", 1),
        ("reason", " "),
        ("run_version", 0),
        ("release_fingerprint", "latest"),
    ],
)
def test_commands_require_explicit_versions_scope_and_confirmation(evidence, field, value):
    command = PublishConfiguration(
        uuid4(),
        evidence.selector,
        0,
        None,
        "批准后发布",
        True,
        evidence.run_id,
        evidence.run_version,
        evidence.release.fingerprint(),
    )
    with pytest.raises(ValueError):
        replace(command, **{field: value})


def test_scope_and_move_targets_reject_invalid_identity(evidence):
    with pytest.raises(ValueError):
        PublicationScope(True, 42)
    with pytest.raises(ValueError):
        MovePublication(uuid4(), evidence.selector, 1, None, "回退", True, UUID(int=0))
