from datetime import UTC, datetime
from uuid import uuid4

import grpc
import pytest

from qs_ai.application.evaluation.management import EvaluationView
from qs_ai.contracts.workflow import workflow_pb2 as pb
from tests.test_grpc_commands import Aborted, Context
from tests.test_grpc_evaluation import command
from tests.test_grpc_evaluation import handler as handler


def request():
    return pb.EvaluationReviewCorrectionCommand(
        scope=command().scope,
        command_id=str(uuid4()),
        expected_version=17,
        role="safety_product",
        candidate_id="candidate:1",
        decision="approve",
        reason=" 核对后通过，记录措辞建议 ",
        confirm=True,
        previous_review_fingerprint="sha256:" + "a" * 64,
        candidate_output_fingerprint="sha256:" + "b" * 64,
    )


async def test_trusted_actor_and_server_time_only(handler):
    service, store = handler
    value = request()
    store.correct_review.return_value = EvaluationView(
        value.scope.run_id, 18, "awaiting_review", 0, "[]"
    )
    before = datetime.now(UTC)
    response = await service.CorrectReview(value, Context())
    args = store.correct_review.await_args.args
    assert args[0].actor == args[-1].reviewer == "user:42"
    assert args[1:5] == (
        17,
        value.command_id,
        value.previous_review_fingerprint,
        value.candidate_output_fingerprint,
    )
    assert before <= args[-1].reviewed_at <= datetime.now(UTC)
    assert args[-1].reason == value.reason.strip() and response.version == 18
    assert "reviewer" not in {f.name for f in value.DESCRIPTOR.fields}


async def test_untrusted_workload_cannot_enter_correction_store(handler):
    service, store = handler
    context = Context()
    context.auth_context = lambda: {}
    with pytest.raises(Aborted) as failure:
        await service.CorrectReview(request(), context)
    assert failure.value.args[0] == grpc.StatusCode.PERMISSION_DENIED
    store.correct_review.assert_not_awaited()


@pytest.mark.parametrize(
    "field,value",
    [
        ("confirm", False),
        ("expected_version", 0),
        ("command_id", "invalid"),
        ("candidate_id", " "),
        ("role", "admin"),
        ("previous_review_fingerprint", "bad"),
        ("candidate_output_fingerprint", "bad"),
        ("decision", "waive"),
        ("reason", "汉" * 334),
    ],
)
async def test_invalid_contract_never_calls_store(handler, field, value):
    service, store = handler
    message = request()
    setattr(message, field, value)
    with pytest.raises(Aborted) as failure:
        await service.CorrectReview(message, Context())
    assert failure.value.args[0] == grpc.StatusCode.INVALID_ARGUMENT
    store.correct_review.assert_not_awaited()
