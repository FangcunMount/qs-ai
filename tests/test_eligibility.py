"""Capability checks do not imply authorization, capacity or publication."""

import json
from contextlib import asynccontextmanager
from dataclasses import replace

import grpc
import pytest

from qs_ai.application.interpretation.eligibility import (
    Eligibility,
    EligibilityReader,
    eligibility_source,
)
from qs_ai.application.interpretation.ports import AccessDenied, DependencyUnavailable
from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.domain.interpretation.model import Actor, Fact, RuleViolation
from qs_ai.transport.grpc.commands import Commands
from tests.test_grpc_commands import Aborted, Context
from tests.test_input_binding import bound_case
from tests.test_mbti_runtime import mbti_case


@pytest.mark.parametrize("mbti", [False, True])
def test_source_reuses_bound_snapshot_decoder(mbti):
    if mbti:
        claim, evidence, _ = mbti_case()
        session = claim.session
    else:
        session, evidence, _ = bound_case()
    result = eligibility_source(
        session.actor, session.testee_id, session.assessment_ids, evidence.items
    )
    transient, fixed, selector = result
    assert transient.evidence_set_id == fixed.id
    assert transient.active_run_id is None
    assert transient.workflow_version == f"qs-published-snapshot-v{2 if mbti else 1}"
    if mbti:
        assert selector.admission_candidates() == (selector,)


@pytest.mark.parametrize(
    "change,reason",
    [
        ({"testee_id": "8"}, "source_conflict"),
        ({"report_id": "100"}, "source_conflict"),
        ({"source_version": "other:101"}, "source_conflict"),
        ({"facts": ()}, "source_incomplete"),
        ({"facts": (Fact("standard_report", "secret-not-json"),)}, "source_incomplete"),
        ({"facts": (Fact("standard_report", '{"model":{},"model":{}}'),)}, "source_incomplete"),
    ],
)
def test_inconsistent_evidence_is_not_eligible(change, reason):
    session, evidence, _ = bound_case()
    result = eligibility_source(
        session.actor, "7", ("42",), (replace(evidence.items[0], **change),)
    )
    assert result == Eligibility("unavailable", reason)


@pytest.mark.parametrize(
    "section,key,value,reason",
    [
        ("model", "kind", "ability", "unsupported_scene"),
        ("model", "code", "OTHER_MBTI", "unsupported_model_version"),
        ("model", "version", "v-next", "unsupported_model_version"),
        ("runtime", "decision_kind", "score_range", "unsupported_scene"),
        ("model_extra", "type_code", "ENTP", "source_incomplete"),
    ],
)
def test_mbti_is_a_finite_exact_model_contract(section, key, value, reason):
    claim, evidence, _ = mbti_case()
    snapshot = json.loads(evidence.items[0].facts[0].value)
    snapshot[section][key] = value
    item = replace(evidence.items[0], facts=(Fact("standard_report", json.dumps(snapshot)),))
    assert eligibility_source(claim.session.actor, "7", ("42",), (item,)) == Eligibility(
        "unavailable", reason
    )


def test_multiple_reports_and_invalid_actor_rejected():
    assert eligibility_source(Actor("1", "parent"), "7", ("42", "43"), ()) == Eligibility(
        "unavailable", "unsupported_scene"
    )
    with pytest.raises(RuleViolation):
        eligibility_source(Actor("not-org", "parent"), "7", ("42",), ())


@pytest.mark.parametrize(
    "failure,code,detail",
    [
        (AccessDenied(), grpc.StatusCode.PERMISSION_DENIED, "Resource access denied"),
        (ValueError("secret"), grpc.StatusCode.INVALID_ARGUMENT, "Invalid eligibility query"),
        (RuleViolation("secret"), grpc.StatusCode.INVALID_ARGUMENT, "Invalid eligibility query"),
        (
            DependencyUnavailable("secret"),
            grpc.StatusCode.UNAVAILABLE,
            "Eligibility temporarily unavailable",
        ),
        (
            RuntimeError("secret"),
            grpc.StatusCode.UNAVAILABLE,
            "Eligibility temporarily unavailable",
        ),
    ],
)
async def test_grpc_read_failure_does_not_instruct_command_replay(failure, code, detail):
    class Reader:
        async def check(self, *args):
            raise failure

    class Scope:
        async def get(self, dependency):
            assert dependency is EligibilityReader
            return Reader()

    @asynccontextmanager
    async def container():
        yield Scope()

    query = pb.EligibilityQuery(actor=pb.Actor(org_id="1", subject_id="parent"))
    with pytest.raises(Aborted) as result:
        await Commands(container).CheckEligibility(query, Context())
    assert result.value.args == (code, detail)


async def test_untrusted_workload_never_opens_scope():
    class Untrusted(Context):
        def auth_context(self):
            return {"transport_security_type": [b"ssl"], "x509_common_name": [b"other.svc"]}

    def forbidden():
        pytest.fail("untrusted request resolved dependencies")

    with pytest.raises(Aborted) as result:
        await Commands(forbidden).CheckEligibility(pb.EligibilityQuery(), Untrusted())
    assert result.value.args[0] == grpc.StatusCode.PERMISSION_DENIED


async def test_container_resolves_read_only_capability_with_existing_dependencies():
    container = create_container(Settings(_env_file=None, database_url=None))
    try:
        async with container() as scope:
            reader = await scope.get(EligibilityReader)
            session, evidence, _ = bound_case()
            with pytest.raises(DependencyUnavailable):
                await reader.check(session.actor, "7", ("42",), evidence.items)
    finally:
        await container.close()
