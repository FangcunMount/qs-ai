"""Shared protobuf mappings; no transport, storage, or business execution ownership."""

from uuid import UUID

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.application.interpretation.ports import Receipt
from qs_ai.contracts.workflow import workflow_pb2 as pb


def receipt_message(result: Receipt) -> pb.Receipt:
    return pb.Receipt(
        session_id=result.session_id,
        run_id=result.run_id or "",
        status=result.status,
        version=result.version,
    )


def scope_from(request: pb.EvaluationQuery) -> ManagementScope:
    run_id = UUID(request.run_id)
    if str(run_id) != request.run_id:
        raise ValueError("Canonical Run id required")
    return ManagementScope(run_id, request.organization_id, request.operator_user_id)
