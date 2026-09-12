from dataclasses import asdict
from datetime import UTC, datetime

import grpc
import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import EvaluationView
from qs_ai.contracts.workflow import workflow_pb2 as pb
from tests.test_evaluation_review import contradiction
from tests.test_grpc_commands import Aborted, Context
from tests.test_grpc_evaluation import command
from tests.test_grpc_evaluation import handler as handler


def request():
    return pb.EvaluationReviewCommand(
        scope=command().scope,
        expected_version=17,
        role="assessment_semantics",
        reviews=[
            pb.CandidateReviewItem(
                candidate_id="candidate:1",
                decision="approve",
                reason=" 核对事实 ",
                semantic_review=pb.SemanticContradictionReview(**asdict(contradiction())),
            ),
            pb.CandidateReviewItem(candidate_id="candidate:2", decision="reject", reason="不合格"),
        ],
    )


async def test_review_uses_one_server_time_and_trusted_actor_for_entire_batch(handler):
    service, store = handler
    value = request()
    store.review.return_value = EvaluationView(
        value.scope.run_id, 18, "awaiting_review", 0, "[]", '[{"reviewer":"user:42"}]'
    )
    before = datetime.now(UTC)
    response = await service.Review(value, Context())
    after = datetime.now(UTC)
    scope, version, values = store.review.await_args.args
    assert scope.actor == "user:42" and version == 17
    assert len(values) == 2 and all(v.reviewer == scope.actor for v in values)
    assert all(v.role == "assessment_semantics" for v in values)
    assert before <= values[0].reviewed_at == values[1].reviewed_at <= after
    assert values[0].reason == "核对事实" and values[0].semantic_review == contradiction()
    assert values[1].semantic_review is None and values[1].decision == "reject"
    assert response.version == 18 and response.status == "awaiting_review"
    assert response.reviews_json == '[{"reviewer":"user:42"}]'
    # Transport has no client-supplied reviewer or reviewed_at fields.
    assert {f.name for f in pb.CandidateReviewItem.DESCRIPTOR.fields} == {
        "candidate_id",
        "decision",
        "reason",
        "semantic_review",
    }


@pytest.mark.parametrize(
    "case",
    [
        "scope",
        "version",
        "role",
        "empty",
        "oversize",
        "duplicate",
        "candidate",
        "decision",
        "reason",
        "semantic",
        "workload",
    ],
)
async def test_invalid_review_is_rejected_before_persistence(handler, case):
    service, store = handler
    value, context = request(), Context()
    expected = grpc.StatusCode.INVALID_ARGUMENT
    if case == "scope":
        value.scope.organization_id = 0
    elif case == "version":
        value.expected_version = 0
    elif case == "role":
        value.role = "administrator"
    elif case == "empty":
        value.ClearField("reviews")
    elif case == "oversize":
        for _ in range(34):
            value.reviews.add(candidate_id="candidate:x", decision="approve", reason="核对")
    elif case == "duplicate":
        value.reviews[1].candidate_id = value.reviews[0].candidate_id
    elif case == "candidate":
        value.reviews[1].candidate_id = " "
    elif case == "decision":
        value.reviews[1].decision = "waive"
    elif case == "reason":
        value.reviews[1].reason = "汉" * 334
    elif case == "semantic":
        value.reviews[0].semantic_review.policy_version = ""
    else:
        context.auth_context = lambda: {}
        expected = grpc.StatusCode.PERMISSION_DENIED
    with pytest.raises(Aborted) as failure:
        await service.Review(value, context)
    assert failure.value.args[0] == expected
    store.review.assert_not_awaited()


@pytest.mark.parametrize(
    "error,code",
    [
        (CheckpointConflict("private-details"), grpc.StatusCode.ABORTED),
        (RuntimeError("private-details"), grpc.StatusCode.UNAVAILABLE),
    ],
)
async def test_review_failure_requires_readback_and_does_not_expose_details(handler, error, code):
    service, store = handler
    store.review.side_effect = error
    with pytest.raises(Aborted) as failure:
        await service.Review(request(), Context())
    assert failure.value.args[0] == code
    assert "private-details" not in str(failure.value)
