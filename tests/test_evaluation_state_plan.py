"""Non-command worker state plans keep first wire, input and sequence independently."""

import hashlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID, uuid4, uuid5

import pytest
from jwcrypto import jwk
from reliable_messaging.transaction import decode_plan, encode_plan

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateFacts,
    EvaluationStateIntent,
    freeze_evaluation_state_plan,
    read_evaluation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.messaging import prepare


def fixture():
    intent = EvaluationStateIntent(
        "7", str(uuid4()), "preflight/2", 2, hashlib.sha256(b"authorized original input").digest()
    )
    facts = EvaluationStateFacts(intent, datetime(2026, 10, 21, tzinfo=UTC), 8, 2)
    state = pb.EvaluationRuntimeState(
        run_id=intent.run_id,
        organization_id="7",
        version=3,
        event_sequence=9,
        status="collecting",
        release_fingerprint="sha256:" + "a" * 64,
    )
    keys = dict(
        signing_key=jwk.JWK.generate(kty="EC", crv="P-256", kid="ai.sign"),
        recipient_key=jwk.JWK.generate(kty="EC", crv="P-256", kid="qs.encrypt"),
    )

    def seal(value=state, at=facts.at):
        return prepare(
            pb.EVALUATION_STATE,
            str(uuid5(UUID(value.run_id), f"state:{value.version}")),
            value.run_id,
            pb.MessagingBody(evaluation_state=value),
            organization_id=value.organization_id,
            original_occurred_at=at.isoformat(timespec="microseconds"),
            **keys,
        )

    prepared = seal()
    plan = freeze_evaluation_state_plan(facts, str(uuid4()), prepared)
    return SimpleNamespace(**locals())


def test_durable_winner_owns_exact_first_wire_clock_sequence_and_input_without_keys():
    f = fixture()
    restored = decode_plan(encode_plan(f.plan))
    facts, value = read_evaluation_state_plan(f.intent, restored)
    assert facts == f.facts and EvaluationStateFacts.decode(facts.encode()) == facts
    assert value.wire == f.prepared.wire and value.body == f.prepared.body
    assert value.sequence == 9 and value.envelope().message_id == f.prepared.envelope.message_id
    assert not value.ordered and value.requires_receipt
    f.prepared.envelope.message_id = str(uuid4())
    assert read_evaluation_state_plan(f.intent, restored)[1] == value


def test_new_cipher_and_candidate_clock_do_not_change_the_original_request_identity():
    f = fixture()
    facts = replace(f.facts, at=f.facts.at + timedelta(seconds=1))
    other = freeze_evaluation_state_plan(facts, str(uuid4()), f.seal(at=facts.at))
    assert other.digest != f.plan.digest and other.branches[0].wire != f.plan.branches[0].wire
    f.intent.request.match(other)
    assert f.intent.request.key_digest == f.facts.intent.request.key_digest
    with pytest.raises(ValueError):
        replace(f.intent, source_digest=hashlib.sha256(b"different input").digest()).request.match(
            f.plan
        )


@pytest.mark.parametrize("damage", ["owner", "scope", "physical", "branch", "revision", "wire"])
def test_wrong_route_identity_or_wire_never_becomes_an_original_state_plan(damage):
    f = fixture()
    branch = f.plan.branches[0]
    with pytest.raises(ValueError):
        if damage in ("owner", "scope"):
            changed = replace(f.plan, **{damage: "qs.mysql" if damage == "owner" else "org:8"})
        else:
            values = {
                "physical": {"physical_topic": "foreign-topic"},
                "branch": {"branch_id": str(uuid4())},
                "revision": {"route_revision": "other"},
                "wire": {"wire": b"different"},
            }
            changed = replace(f.plan, branches=(replace(branch, **values[damage]),))
        read_evaluation_state_plan(f.intent, changed)


@pytest.mark.parametrize("damage", ["organization_id", "run_id", "version", "event_sequence"])
def test_frozen_state_must_match_original_target_and_exact_next_version_sequence(damage):
    f = fixture()
    changed = pb.EvaluationRuntimeState.FromString(f.state.SerializeToString())
    setattr(
        changed,
        damage,
        {"organization_id": "8", "run_id": str(uuid4()), "version": 4, "event_sequence": 10}[
            damage
        ],
    )
    with pytest.raises(ValueError):
        freeze_evaluation_state_plan(f.facts, str(uuid4()), f.seal(changed))


@pytest.mark.parametrize("damage", ["duplicate", "unknown", "noncanonical", "version"])
def test_host_context_rejects_ambiguous_or_unrecognized_codec_without_normalizing(damage):
    f = fixture()
    raw = f.facts.encode()
    if damage == "duplicate":
        raw = raw[:-1] + b',"previous_version":2}'
    elif damage == "unknown":
        raw = raw[:-1] + b',"unrecognized":true}'
    elif damage == "noncanonical":
        raw = raw.replace(b'"previous_version":2', b'"previous_version": 2')
    else:
        raw = raw.replace(b"/v1", b"/v2", 1)
    with pytest.raises(ValueError):
        EvaluationStateFacts.decode(raw)


@pytest.mark.parametrize("value", [True, -1, 2**63 - 1])
def test_expected_version_cannot_overflow_target_or_accept_boolean(value):
    f = fixture()
    with pytest.raises(ValueError):
        replace(f.intent, expected_version=value)


def test_missing_regressed_or_overflowing_sequence_evidence_is_refused_before_freeze():
    f = fixture()
    for sequence, version in ((0, 2), (8, 0), (8, 3), (2**63 - 1, 2)):
        with pytest.raises(ValueError):
            replace(f.facts, previous_sequence=sequence, previous_version=version)
    with pytest.raises(ValueError):
        replace(f.facts, at=f.facts.at.replace(tzinfo=None))
    with pytest.raises(ValueError):
        freeze_evaluation_state_plan(
            f.facts, str(uuid4()), f.seal(at=f.facts.at + timedelta(seconds=1))
        )
