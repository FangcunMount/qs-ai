"""Large authorized review inputs bind by digest; first clocks and wire remain frozen."""

import hashlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from uuid import uuid4, uuid5

import pytest
from jwcrypto import jwk
from reliable_messaging.transaction import decode_plan, encode_plan

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateFacts,
    freeze_evaluation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.messaging import prepare
from qs_ai.infrastructure.workflow_transport.review_state_plan import (
    ReviewPlanFacts,
    ReviewStateInput,
    read_review_state_plan,
)
from tests.route_fixtures import legacy_routes
from tests.test_evaluation_review import review


def fixture(count=35):
    scope = ManagementScope(uuid4(), 7, 42)
    at = datetime(2026, 10, 21, 8, tzinfo=timezone(timedelta(hours=8)))
    values = tuple(
        replace(
            review(),
            candidate_id=f"candidate:{i}",
            reason="核" * 333,
            reviewed_at=at + timedelta(microseconds=i),
        )
        for i in range(count)
    )
    incoming = ReviewStateInput(scope, uuid4(), 9, values)
    facts = ReviewPlanFacts(
        hashlib.sha256(b"original source").digest(),
        incoming.input_digest,
        tuple(v.reviewed_at for v in values),
    )
    frozen = EvaluationStateFacts(incoming.intent, at.astimezone(UTC), 8, 9, facts.encode())
    state = pb.EvaluationRuntimeState(
        run_id=str(scope.run_id),
        organization_id="7",
        version=10,
        event_sequence=9,
        status="awaiting_review",
        release_fingerprint="sha256:" + "a" * 64,
    )
    signing = jwk.JWK.generate(kty="EC", crv="P-256", kid="ai.sign")
    recipient = jwk.JWK.generate(kty="EC", crv="P-256", kid="qs.encrypt")

    def seal(value=state):
        return prepare(
            pb.EVALUATION_STATE,
            str(uuid5(scope.run_id, f"state:{value.version}")),
            str(scope.run_id),
            pb.MessagingBody(evaluation_state=value),
            organization_id="7",
            signing_key=signing,
            recipient_key=recipient,
            original_occurred_at=frozen.at.isoformat(timespec="microseconds"),
        )

    first = seal()
    winner = freeze_evaluation_state_plan(
        frozen, str(incoming.operation_id), first, route_resolver=legacy_routes()
    )
    return locals()


def test_full_original_batch_binds_all_input_without_putting_it_in_protocol_metadata():
    f = fixture()
    incoming, winner = f["incoming"], f["winner"]
    assert sum(len(v.reason.encode()) for v in incoming.values) > 8192
    facts, original, archived = read_review_state_plan(
        incoming, decode_plan(encode_plan(winner)), route_resolver=legacy_routes()
    )
    assert facts == f["frozen"] and original == f["facts"]
    assert archived.wire == f["first"].wire
    assert len(archived.context) <= 16384
    assert len(original.encode()) <= 8192 and ("核" * 10).encode() not in original.encode()
    assert "核" not in repr(incoming) and "核" not in repr(original)


def test_losing_clock_vector_uses_durable_winner_and_preserves_original_offset_format():
    f = fixture(2)
    incoming = f["incoming"]
    losing = replace(
        incoming,
        values=tuple(
            replace(v, reviewed_at=v.reviewed_at + timedelta(seconds=1)) for v in incoming.values
        ),
    )
    assert (
        losing.input_digest == incoming.input_digest
        and losing.intent.request == incoming.intent.request
    )
    _, original, _ = read_review_state_plan(losing, f["winner"], route_resolver=legacy_routes())
    restored = original.values_for(losing)
    assert tuple(v.reviewed_at.isoformat() for v in restored) == tuple(
        v.reviewed_at.isoformat() for v in incoming.values
    )
    assert restored != losing.values


@pytest.mark.parametrize("damage", ["reason", "decision", "scope", "identity"])
def test_changed_original_authority_or_payload_cannot_execute_an_existing_plan(damage):
    f = fixture(2)
    incoming = f["incoming"]
    if damage == "scope":
        changed = replace(
            incoming,
            scope=replace(incoming.scope, operator_user_id=43),
            values=tuple(replace(v, reviewer="user:43") for v in incoming.values),
        )
    elif damage == "identity":
        changed = replace(incoming, operation_id=uuid4())
    else:
        changed = replace(
            incoming,
            values=(
                replace(
                    incoming.values[0], **{damage: "不同理由" if damage == "reason" else "reject"}
                ),
                incoming.values[1],
            ),
        )
    with pytest.raises(ValueError):
        read_review_state_plan(changed, f["winner"], route_resolver=legacy_routes())


@pytest.mark.parametrize(
    "damage", ["duplicate", "unknown", "naive", "empty", "version", "noncanonical"]
)
def test_review_context_rejects_ambiguous_identity_clock_or_codec(damage):
    f = fixture(2)
    facts = f["facts"]
    raw = facts.encode()
    if damage == "duplicate":
        raw = raw[:-1] + b',"input_digest":"' + b"a" * 64 + b'"}'
    elif damage == "unknown":
        raw = raw[:-1] + b',"unexpected":true}'
    elif damage == "naive":
        with pytest.raises(ValueError):
            replace(facts, reviewed_at=(datetime(2026, 10, 21),)).encode()
        return
    elif damage == "empty":
        with pytest.raises(ValueError):
            replace(facts, reviewed_at=()).encode()
        return
    elif damage == "version":
        raw = raw.replace(b"/v1", b"/v2", 1)
    else:
        raw = raw.replace(b'"reviewed_at":', b'"reviewed_at": ')
    with pytest.raises(ValueError):
        ReviewPlanFacts.decode(raw)


def test_clock_count_or_input_digest_never_substitutes_different_business_values():
    f = fixture(2)
    for invalid in (
        replace(f["facts"], reviewed_at=f["facts"].reviewed_at[:1]),
        replace(f["facts"], input_digest=b"x" * 32),
    ):
        with pytest.raises(ValueError):
            invalid.values_for(f["incoming"])


@pytest.mark.parametrize("version,status", [(11, "awaiting_review"), (10, "approved")])
def test_one_review_cas_cannot_be_replaced_by_another_frozen_state(version, status):
    f = fixture(1)
    state = pb.EvaluationRuntimeState.FromString(f["state"].SerializeToString())
    state.version, state.status = version, status
    changed = freeze_evaluation_state_plan(
        f["frozen"],
        str(f["incoming"].operation_id),
        f["seal"](state),
        route_resolver=legacy_routes(),
    )
    with pytest.raises(ValueError):
        read_review_state_plan(f["incoming"], changed, route_resolver=legacy_routes())
