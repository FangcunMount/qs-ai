from unittest.mock import MagicMock

import grpc
import pytest

from qs_ai.transport.grpc.mq_cutover import MQ_REQUIRED
from tests.test_grpc_mq_cutover import WRITES


class Aborted(Exception):
    pass


class Context:
    def auth_context(self):
        return {"transport_security_type": [b"ssl"], "x509_common_name": [b"qs-apiserver.svc"]}

    async def abort(self, code, detail):
        raise Aborted(code, detail)


@pytest.mark.parametrize("case", WRITES, ids=[f"{v[0].__name__}.{v[2]}" for v in WRITES])
@pytest.mark.parametrize("trusted", [False, True])
async def test_retired_execution_handlers_never_enter_business_scope(case, trusted):
    service, _, method, request, _ = case
    container = MagicMock(side_effect=AssertionError("No business scope permitted"))
    context = Context()
    if not trusted:
        context.auth_context = lambda: {}
    with pytest.raises(Aborted) as error:
        await getattr(service(container), method)(request(), context)
    assert error.value.args == (
        (grpc.StatusCode.FAILED_PRECONDITION, MQ_REQUIRED)
        if trusted
        else (grpc.StatusCode.PERMISSION_DENIED, "Untrusted workload")
    )
    container.assert_not_called()
