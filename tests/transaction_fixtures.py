"""Real original admission, persistence and rollback; no real model or Broker calls."""

from dataclasses import asdict
from uuid import uuid4

from sqlalchemy import select

from qs_ai.application.evaluation.capacity import EvaluationCapacityPolicy
from qs_ai.application.evaluation.execution_mode import EvaluationRuntimeLimits
from qs_ai.application.execution.capacity import ParticipantCapacityPolicy
from qs_ai.application.governance.solution_models import DEFAULT_EDITABLE_MODELS
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.infrastructure.workflow_transport.command_admission import WorkflowCommandAdmission
from qs_ai.infrastructure.workflow_transport.messaging import prepare
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_start import requested as requested
from tests.integration.test_interpretation import kit as kit
from tests.messaging_fixtures import keys as keys
from tests.test_input_binding import bound_case


def admission(source):
    return WorkflowCommandAdmission(
        source,
        ParticipantCapacityPolicy(),
        EvaluationCapacityPolicy(),
        DEFAULT_EDITABLE_MODELS,
        None,
        EvaluationRuntimeLimits(),
    )


def command(keys, kind, body, aggregate, command_id=None, *, original_occurred_at=""):
    return prepare(
        kind,
        command_id or str(uuid4()),
        aggregate,
        body,
        organization_id="1",
        signing_key=keys["qs.sign"],
        recipient_key=keys["ai.encrypt"],
        original_occurred_at=original_occurred_at,
    )


def start_message(kit, keys, *, original_occurred_at=""):
    identity = str(uuid4())
    item = bound_case()[1].items[0]
    return command(
        keys,
        pb.START,
        pb.MessagingBody(
            start=workflow.StartCommand(
                request_id=identity,
                actor=workflow.Actor(**asdict(kit.actor)),
                testee_id="7",
                assessment_ids=["42"],
                goal="MQ事务验收",
                evidence=[workflow.EvidenceItem(**asdict(item))],
            )
        ),
        identity,
        identity,
        original_occurred_at=original_occurred_at,
    )


async def saved(tx, table):
    async with tx.open() as db:
        return list((await db.execute(select(table))).mappings())
