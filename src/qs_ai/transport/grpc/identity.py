"""Authenticated workload identity for QS-delegated management operations."""

from typing import Any

import grpc
from grpc import aio


async def require_qs_workload(context: aio.ServicerContext[Any, Any]) -> None:
    auth = context.auth_context()
    if auth.get("transport_security_type") != [b"ssl"] or auth.get("x509_common_name") != [
        b"qs-apiserver.svc"
    ]:
        await context.abort(grpc.StatusCode.PERMISSION_DENIED, "Untrusted workload")
