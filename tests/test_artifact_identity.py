"""First Artifact identity is validated outside the executor; all content gates remain."""

from dataclasses import replace
from uuid import uuid4

import pytest

from qs_ai.application.execution import artifact as module
from qs_ai.application.interpretation.output import InvalidOutput
from qs_ai.infrastructure.qs_server.output import QSOutputParser
from tests.test_artifact import case


def test_prevalidated_first_identity_uses_original_bytes_without_uuid_generation_in_builder(
    monkeypatch,
):
    claim, evidence, generated = case()
    first = module.build_artifact(claim, evidence, generated, QSOutputParser())
    identity = module.ArtifactIdentity(claim.run_id, generated.response.invocation_id, first.id)

    def no_uuid(*args):
        raise AssertionError("Identity validation belongs outside the DB executor")

    monkeypatch.setattr(module, "uuid5", no_uuid)
    assert (
        module.build_artifact(claim, evidence, generated, QSOutputParser(), identity=identity)
        == first
    )


def test_wrong_existing_artifact_id_is_rejected_before_entering_executor():
    claim, _, generated = case()
    with pytest.raises(ValueError):
        module.ArtifactIdentity(claim.run_id, generated.response.invocation_id, str(uuid4()))


@pytest.mark.parametrize("foreign", ["run", "invocation"])
def test_valid_identity_of_another_execution_cannot_be_bound_to_original_response(foreign):
    claim, evidence, generated = case()
    other_claim = replace(claim, run_id="other-run") if foreign == "run" else claim
    other_generated = (
        replace(generated, response=replace(generated.response, invocation_id="other-invocation"))
        if foreign == "invocation"
        else generated
    )
    other = module.build_artifact(other_claim, evidence, other_generated, QSOutputParser())
    identity = module.ArtifactIdentity(other.run_id, other.invocation_id, other.id)
    with pytest.raises(InvalidOutput):
        module.build_artifact(claim, evidence, generated, QSOutputParser(), identity=identity)
