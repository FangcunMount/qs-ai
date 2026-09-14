import json
import shutil
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from qs_ai.application.interpretation.input import assemble_input
from qs_ai.infrastructure.qs_server.input_schema import load_input_schema
from qs_ai.infrastructure.qs_server.output import schema_directory
from qs_ai.infrastructure.qs_server.profiles import load_migrated_release


def test_prepared_input_satisfies_frozen_schema_with_empty_reference_arrays():
    release = load_migrated_release("participant-scale-score-range-default", "v6")
    raw = (Path(__file__).parent / "fixtures/report_snapshot.json").read_text()
    result = assemble_input(raw, release.input_policy)
    validator = Draft202012Validator(load_input_schema())
    document = json.loads(result.canonical_json)
    assert document["facts"]["dimensions"][0]["standard_suggestion_refs"] == []
    assert document["facts"]["dimensions"][1]["standard_suggestion_refs"] == []
    validator.validate(document)
    for dimension in document["facts"]["dimensions"]:
        if dimension["standard_suggestion_refs"] == []:
            dimension["standard_suggestion_refs"] = None
    errors = list(validator.iter_errors(document))
    assert [(list(e.absolute_path), e.validator) for e in errors] == [
        (["facts", "dimensions", 0, "standard_suggestion_refs"], "type"),
        (["facts", "dimensions", 1, "standard_suggestion_refs"], "type"),
    ]
    # The provider projection intentionally lacks server-only source and Profile metadata.
    assert not validator.is_valid(json.loads(result.provider_payload))


def test_modified_schema_fails_verification(tmp_path):
    shutil.copytree(schema_directory(), tmp_path, dirs_exist_ok=True)
    path = tmp_path / "ai-explanation-input-v1.schema.json"
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ValueError):
        load_input_schema(directory=tmp_path)
