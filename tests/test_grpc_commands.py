from contextlib import asynccontextmanager
from uuid import uuid4

import grpc
import pytest

from qs_ai.application.interpretation.ports import AccessDenied, DependencyUnavailable
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.domain.interpretation.model import RuleViolation
from qs_ai.transport.grpc.commands import Commands


class Aborted(Exception):
    pass


class Context:
    def auth_context(self):
        return {"transport_security_type": [b"ssl"], "x509_common_name": [b"qs-apiserver.svc"]}

    async def abort(self, code, detail):
        raise Aborted(code, detail)


@pytest.mark.parametrize("method", ["Start", "Change"])
@pytest.mark.parametrize(
    "failure,code,detail",
    [
        (AccessDenied(), grpc.StatusCode.PERMISSION_DENIED, "Resource access denied"),
        (RuleViolation("version_conflict"), grpc.StatusCode.ABORTED, "version_conflict"),
        (RuleViolation("invalid_state"), grpc.StatusCode.ABORTED, "invalid_state"),
        (RuleViolation("invalid_answer"), grpc.StatusCode.INVALID_ARGUMENT, "invalid_answer"),
        (ValueError("private"), grpc.StatusCode.INVALID_ARGUMENT, "Invalid command"),
        (DependencyUnavailable(), grpc.StatusCode.UNAVAILABLE, "Business dependencies unavailable"),
        (
            RuntimeError("secret"),
            grpc.StatusCode.UNAVAILABLE,
            "Command outcome unknown; replay original ID",
        ),
    ],
)
async def test_error_mapping_and_redaction(method, failure, code, detail):
    class FailingService:
        async def fail(self, *args):
            raise failure

        start_external = change = answer = cancel = fail

    class Scope:
        async def get(self, dependency):
            return FailingService()

    @asynccontextmanager
    async def container():
        yield Scope()

    handler = Commands(container)
    actor = pb.Actor(org_id="1", subject_id="parent")
    request = (
        pb.StartCommand(actor=actor, request_id=str(uuid4()))
        if method == "Start"
        else pb.ChangeCommand(
            actor=actor,
            command_id=str(uuid4()),
            session_id=str(uuid4()),
            action="cancel",
            expected_version=1,
        )
    )
    with pytest.raises(Aborted) as error:
        await getattr(handler, method)(request, Context())
    assert error.value.args == (code, detail)
