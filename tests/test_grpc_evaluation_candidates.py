import grpc
import pytest

from qs_ai.application.evaluation.candidates import (
    CandidateEvidence,
    CandidateIndex,
    CandidateSummary,
)
from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.contracts.workflow import workflow_pb2 as pb
from tests.test_grpc_commands import Aborted, Context
from tests.test_grpc_evaluation import command
from tests.test_grpc_evaluation import handler as handler


async def test_index_and_candidate_preserve_scope_version_and_exact_bytes(handler):
    service, store = handler
    scope = command().scope
    store.list_candidates.return_value = CandidateIndex(
        scope.run_id, 12, (CandidateSummary("candidate:1", "case:1", 1),)
    )
    index = await service.ListCandidates(scope, Context())
    assert index.version == 12 and index.candidates[0].candidate_id == "candidate:1"
    store.get_candidate.return_value = CandidateEvidence(
        scope.run_id, 12, "candidate:1", '{"原文": 1}'.encode(), b'{"scores":{}}', "{}"
    )
    request = pb.EvaluationCandidateQuery(
        scope=scope, candidate_id="candidate:1", expected_version=12
    )
    result = await service.GetCandidate(request, Context())
    trusted, candidate, version = store.get_candidate.await_args.args
    assert trusted.organization_id == 7 and trusted.actor == "user:42"
    assert (candidate, version) == ("candidate:1", 12)
    assert result.normalized_output == '{"原文": 1}'.encode()
    assert pb.EvaluationCandidateEvidence.FromString(result.SerializeToString()) == result


@pytest.mark.parametrize("case", ["scope", "version", "candidate", "workload"])
async def test_bad_query_is_rejected_before_store(handler, case):
    service, store = handler
    request = pb.EvaluationCandidateQuery(
        scope=command().scope, candidate_id="candidate:1", expected_version=12
    )
    context = Context()
    expected = grpc.StatusCode.INVALID_ARGUMENT
    if case == "scope":
        request.scope.organization_id = 0
    elif case == "version":
        request.expected_version = 0
    elif case == "candidate":
        request.candidate_id = "bad id"
    else:
        context.auth_context = lambda: {}
        expected = grpc.StatusCode.PERMISSION_DENIED
    with pytest.raises(Aborted) as error:
        await service.GetCandidate(request, context)
    assert error.value.args[0] == expected
    store.get_candidate.assert_not_awaited()
    if case in ("scope", "workload"):
        with pytest.raises(Aborted):
            await service.ListCandidates(request.scope, context)
        store.list_candidates.assert_not_awaited()


@pytest.mark.parametrize(
    "error,code",
    [
        (CheckpointConflict("private"), grpc.StatusCode.ABORTED),
        (NotFound("private"), grpc.StatusCode.NOT_FOUND),
    ],
)
async def test_stale_and_foreign_candidate_errors_are_redacted(handler, error, code):
    service, store = handler
    store.get_candidate.side_effect = error
    request = pb.EvaluationCandidateQuery(
        scope=command().scope, candidate_id="candidate:1", expected_version=12
    )
    with pytest.raises(Aborted) as caught:
        await service.GetCandidate(request, Context())
    assert caught.value.args[0] == code and "private" not in str(caught.value)
