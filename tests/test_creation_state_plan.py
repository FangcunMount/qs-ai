"""First creation mode, raw definition and offset survive durable plan round trips."""

import hashlib
from dataclasses import replace
from datetime import datetime
from uuid import uuid5

import pytest
from jwcrypto import jwk
from reliable_messaging.transaction import decode_plan, encode_plan

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.persistence.mysql.evaluation_runs import plan_run_creation
from qs_ai.infrastructure.workflow_transport.creation_state_plan import (
    CreationPlanFacts,
    CreationStateInput,
    read_creation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateFacts,
    freeze_evaluation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.messaging import prepare
from tests.route_fixtures import legacy_routes
from tests.test_generation_manifest import assets as assets
from tests.test_generation_manifest import complete_release as complete_release
from tests.test_generation_manifest import evaluation_release as evaluation_release
from tests.test_run_creation_planning import context as context


@pytest.fixture
async def frozen(context):
    f = context
    incoming = CreationStateInput(ManagementScope(f.run_id, 1, 42), f.release, "原创建意图")
    plan = await plan_run_creation(
        f.db,
        f.run_id,
        f.release,
        1,
        "user:42",
        incoming.reason,
        f.at,
        execution_mode="candidate_v2",
    )
    original = CreationPlanFacts.from_creation(incoming, plan)
    facts = EvaluationStateFacts(incoming.intent, f.at, 0, 0, original.encode())
    state = pb.EvaluationRuntimeState(
        run_id=str(f.run_id),
        organization_id="1",
        version=1,
        event_sequence=1,
        status="requested",
        release_fingerprint=f.release.fingerprint(),
    )
    signing = jwk.JWK.generate(kty="EC", crv="P-256", kid="ai.sign")
    recipient = jwk.JWK.generate(kty="EC", crv="P-256", kid="qs.encrypt")

    def seal(value=state):
        return prepare(
            pb.EVALUATION_STATE,
            str(uuid5(f.run_id, f"state:{value.version}")),
            str(f.run_id),
            pb.MessagingBody(evaluation_state=value),
            organization_id="1",
            signing_key=signing,
            recipient_key=recipient,
            original_occurred_at=facts.at.isoformat(timespec="microseconds"),
        )

    first = seal()
    winner = freeze_evaluation_state_plan(
        facts, str(f.run_id), first, route_resolver=legacy_routes()
    )
    return locals()


async def test_creation_round_trip_preserves_first_bytes_mode_and_offset(frozen):
    f = frozen
    facts, original, archived = read_creation_state_plan(
        f["incoming"], decode_plan(encode_plan(f["winner"])), route_resolver=legacy_routes()
    )
    assert facts == f["facts"] and original == f["original"]
    assert original.created_at.isoformat() == f["f"].at.isoformat()
    assert original.execution_mode == "candidate_v2"
    assert original.definition_digest == hashlib.sha256(f["plan"].definition_json.encode()).digest()
    original.validate(f["incoming"], f["plan"])
    assert archived.wire == f["first"].wire
    assert len(original.encode()) < 8192 and "原创建意图".encode() not in original.encode()
    assert "原创建意图" not in repr(original)


@pytest.mark.parametrize("damage", ["mode", "definition", "actor", "policy"])
async def test_root_rejects_substituted_creation_before_any_business_write(frozen, damage):
    f = frozen
    plan = f["plan"]
    if damage == "mode":
        changed = replace(plan, execution_mode="serial_v1")
    elif damage == "definition":
        changed = replace(plan, definition_json=plan.definition_json + " ")
    elif damage == "actor":
        changed = replace(plan, requested_by="user:43")
    else:
        changed = replace(plan, policy=replace(plan.policy, definition_json="{}"))
    with pytest.raises(ValueError):
        f["original"].validate(f["incoming"], changed)


@pytest.mark.parametrize("damage", ["duplicate", "unknown", "noncanonical", "version"])
async def test_creation_context_is_explicit_canonical_and_bounded(frozen, damage):
    raw = frozen["original"].encode()
    if damage == "duplicate":
        raw = raw[:-1] + b',"execution_mode":"candidate_v2"}'
    elif damage == "unknown":
        raw = raw[:-1] + b',"extra":true}'
    elif damage == "noncanonical":
        raw = raw.replace(b'"created_at":', b'"created_at": ')
    else:
        raw = raw.replace(b"/v1", b"/v2", 1)
    with pytest.raises(ValueError):
        CreationPlanFacts.decode(raw)


@pytest.mark.parametrize("damage", ["mode", "naive", "digest"])
async def test_invalid_creation_fact_rejected_without_io(frozen, damage):
    facts = frozen["original"]
    with pytest.raises(ValueError):
        replace(
            facts,
            **{
                "mode": {"execution_mode": "other"},
                "naive": {"created_at": datetime(2026, 10, 21)},
                "digest": {"definition_digest": b"short"},
            }[damage],
        )


@pytest.mark.parametrize("damage", ["target", "status", "operation", "release", "clock"])
async def test_frozen_creation_requires_exact_initial_state_and_original_identity(frozen, damage):
    f = frozen
    state = pb.EvaluationRuntimeState.FromString(f["state"].SerializeToString())
    if damage == "target":
        state.version = 2
    elif damage == "status":
        state.status = "collecting"
    elif damage == "release":
        state.release_fingerprint = "sha256:" + "f" * 64
    elif damage == "clock":
        from datetime import timedelta

        original = replace(
            f["original"], created_at=f["original"].created_at + timedelta(seconds=1)
        )
        facts = replace(f["facts"], business_context=original.encode())
        winner = freeze_evaluation_state_plan(
            facts, str(f["f"].run_id), f["first"], route_resolver=legacy_routes()
        )
        with pytest.raises(ValueError):
            read_creation_state_plan(f["incoming"], winner, route_resolver=legacy_routes())
        return
    operation = (
        "22222222-2222-4222-8222-222222222222" if damage == "operation" else str(f["f"].run_id)
    )
    winner = freeze_evaluation_state_plan(
        f["facts"], operation, f["seal"](state), route_resolver=legacy_routes()
    )
    with pytest.raises(ValueError):
        read_creation_state_plan(f["incoming"], winner, route_resolver=legacy_routes())
