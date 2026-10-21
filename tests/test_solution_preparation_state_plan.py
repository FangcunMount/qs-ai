"""Durable first Solution state binds input/snapshots/assets without carrying raw source bodies."""

import hashlib
from dataclasses import replace
from datetime import timedelta
from uuid import uuid4, uuid5

import pytest
from jwcrypto import jwk
from reliable_messaging.transaction import decode_plan, encode_plan

from qs_ai.application.governance.solutions import PrepareSolution
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.persistence.mysql.solution_preparation import plan_prepare_assets
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateFacts,
    freeze_evaluation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.messaging import prepare
from qs_ai.infrastructure.workflow_transport.solution_preparation_plan import (
    SolutionPreparationFacts,
    SolutionPreparationInput,
    read_preparation_plan,
)
from tests.test_generation_manifest import assets as assets
from tests.test_generation_manifest import complete_release as complete_release
from tests.test_generation_manifest import evaluation_release as evaluation_release
from tests.test_native_prompts import draft as draft
from tests.test_solution_preparation_plan import original as original


@pytest.fixture
async def frozen(original):
    p = original
    incoming = SolutionPreparationInput(
        p.scope,
        p.ids.solution_id,
        PrepareSolution(
            command_id=p.ids.command_id,
            expected_revision=1,
            reason=p.state["reason"],
        ),
    )
    assets = await plan_prepare_assets(
        p.db,
        p.scope,
        p.state,
        p.ids.command_id,
        p.draft.saved_at,
        (executable_model(p),),
        execution_mode="candidate_v2",
        identities=p.ids,
    )
    original_facts = SolutionPreparationFacts(
        incoming.solution_id,
        incoming.command.command_id,
        hashlib.sha256(b"original source").digest(),
        hashlib.sha256(b"original draft").digest(),
        assets.digest,
        hashlib.sha256(b"original target").digest(),
        p.draft.saved_at,
        "candidate_v2",
    )
    facts = EvaluationStateFacts(incoming.intent, p.draft.saved_at, 0, 0, original_facts.encode())
    state = pb.EvaluationRuntimeState(
        run_id=str(p.ids.run_id),
        organization_id="1",
        version=1,
        event_sequence=1,
        status="requested",
        release_fingerprint=assets.release.fingerprint(),
    )
    signing = jwk.JWK.generate(kty="EC", crv="P-256", kid="ai.sign")
    recipient = jwk.JWK.generate(kty="EC", crv="P-256", kid="qs.encrypt")

    def seal(value=state):
        return prepare(
            pb.EVALUATION_STATE,
            str(uuid5(p.ids.run_id, f"state:{value.version}")),
            str(p.ids.run_id),
            pb.MessagingBody(evaluation_state=value),
            organization_id="1",
            signing_key=signing,
            recipient_key=recipient,
            original_occurred_at=facts.at.isoformat(timespec="microseconds"),
        )

    first = seal()
    winner = freeze_evaluation_state_plan(facts, str(incoming.command.command_id), first)
    return locals()


def executable_model(p):
    from qs_ai.application.interpretation.route_assets import executable_route

    return executable_route(p.source_route).model


async def test_exact_first_solution_plan_round_trip_keeps_wire_and_bounded_opaque_facts(frozen):
    f = frozen
    facts, original, first = read_preparation_plan(
        f["incoming"], decode_plan(encode_plan(f["winner"]))
    )
    assert facts == f["facts"] and original == f["original_facts"]
    original.validate_assets(f["incoming"], f["assets"])
    assert hashlib.sha256(first.wire).digest() == hashlib.sha256(f["first"].wire).digest()
    assert len(original.encode()) < 8192
    assert f["p"].state["reason"].encode() not in original.encode()
    assert f["p"].state["reason"] not in repr(f["incoming"])
    assert f["winner"].operation_id == str(f["incoming"].command.command_id)
    assert f["winner"].operation_id != f["incoming"].intent.run_id


@pytest.mark.parametrize("change", ["reason", "actor", "revision", "solution", "command"])
async def test_original_public_intent_never_adopts_a_different_preparation(frozen, change):
    f = frozen
    incoming = f["incoming"]
    if change == "actor":
        changed = replace(incoming, scope=replace(incoming.scope, operator_user_id=43))
    elif change == "solution":
        changed = replace(incoming, solution_id=uuid4())
    else:
        changed = replace(
            incoming,
            command=replace(
                incoming.command,
                **{
                    "reason": {"reason": "不同原意图"},
                    "revision": {"expected_revision": 2},
                    "command": {"command_id": uuid4()},
                }[change],
            ),
        )
    assert changed.intent.request.input_hash != incoming.intent.request.input_hash
    with pytest.raises(ValueError):
        read_preparation_plan(changed, f["winner"])


@pytest.mark.parametrize("damage", ["mode", "first_at", "assets_digest", "scope"])
async def test_first_asset_graph_and_original_authority_are_verified_before_execution(
    frozen, damage
):
    f = frozen
    incoming, assets, facts = f["incoming"], f["assets"], f["original_facts"]
    if damage == "mode":
        assets = replace(assets, creation=replace(assets.creation, execution_mode="serial_v1"))
    elif damage == "first_at":
        facts = replace(facts, first_at=facts.first_at + timedelta(seconds=1))
    elif damage == "assets_digest":
        facts = replace(facts, assets_digest=b"x" * 32)
    else:
        incoming = replace(incoming, scope=replace(incoming.scope, organization_id=2))
    with pytest.raises(ValueError):
        facts.validate_assets(incoming, assets)


@pytest.mark.parametrize(
    "damage", ["duplicate", "unknown", "version", "noncanonical", "short_digest", "naive"]
)
async def test_preparation_codec_refuses_ambiguous_or_unbounded_facts(frozen, damage):
    facts = frozen["original_facts"]
    raw = facts.encode()
    if damage == "duplicate":
        raw = raw[:-1] + b',"execution_mode":"candidate_v2"}'
    elif damage == "unknown":
        raw = raw[:-1] + b',"extra":true}'
    elif damage == "version":
        raw = raw.replace(b"/v1", b"/v2", 1)
    elif damage == "noncanonical":
        raw = raw.replace(b'"first_at":', b'"first_at": ')
    else:
        with pytest.raises(ValueError):
            replace(
                facts,
                **(
                    {"source_digest": b"short"}
                    if damage == "short_digest"
                    else {"first_at": facts.first_at.replace(tzinfo=None)}
                ),
            )
        return
    with pytest.raises(ValueError):
        SolutionPreparationFacts.decode(raw)


@pytest.mark.parametrize("damage", ["target", "status", "operation", "clock"])
async def test_durable_plan_requires_exact_initial_state_and_original_operation(frozen, damage):
    f = frozen
    state = pb.EvaluationRuntimeState.FromString(f["state"].SerializeToString())
    facts, operation = f["facts"], str(f["incoming"].command.command_id)
    if damage == "target":
        state.version = 2
    elif damage == "status":
        state.status = "collecting"
    elif damage == "operation":
        operation = str(uuid4())
    else:
        facts = replace(
            facts,
            business_context=replace(
                f["original_facts"], first_at=f["original_facts"].first_at + timedelta(seconds=1)
            ).encode(),
        )
    winner = freeze_evaluation_state_plan(facts, operation, f["seal"](state))
    with pytest.raises(ValueError):
        read_preparation_plan(f["incoming"], winner)
