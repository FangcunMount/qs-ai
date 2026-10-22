"""One isolated actual interpretation worker; private config, synthetic HTTP provider."""

import argparse
import asyncio
import contextvars
import json
import logging
import os
from pathlib import Path

import httpx
from jwcrypto import jwk
from reliable_messaging.rocketmq_transaction import (
    TransactionalSender,
    TransactionConfig,
    TransactionRoute,
)
from reliable_messaging.transaction import Binding, Limits
from reliable_messaging.transaction_mysql import MySQLTransactionStore
from sqlalchemy.ext.asyncio import async_sessionmaker

from qs_ai.application.execution.configuration import PublishedReportWorkflow
from qs_ai.application.execution.generation import DurableGeneration
from qs_ai.application.execution.worker import ExecuteNext
from qs_ai.bootstrap.transaction_messaging import TransactionRuntime
from qs_ai.infrastructure.models.router import ModelGatewayRouter
from qs_ai.infrastructure.persistence.model_call_codec import JSONModelCallCodec
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.execution import MySQLExecutionStore
from qs_ai.infrastructure.persistence.mysql.execution_configurations import (
    MySQLExecutionConfigurations,
)
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.qs_server.responses import DeepSeekResponses
from qs_ai.infrastructure.workflow_transport.messaging import EVENTS
from qs_ai.infrastructure.workflow_transport.transaction_events import TransactionOnlyEvents
from qs_ai.infrastructure.workflow_transport.transactional_interpretation_execution import (
    TransactionalInterpretationExecutionStore,
)
from qs_ai.infrastructure.workflows.report import create_report_workflow
from tests.probes.p1_runtime import SyntheticEvidence


def progress(path, phase, **fields):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(dict(phase=phase, pid=os.getpid(), **fields)))
    temporary.chmod(0o600)
    temporary.replace(path)


async def run(config):
    from rocketmq.v5.log.log_config import logger

    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()
    log = Path(config["client_log"])
    logger.addHandler(logging.FileHandler(log))
    logger.setLevel(logging.ERROR)
    marker = Path(config["progress"])
    database = Database(config["database_url"])
    tx = Transactions(database)
    database.state_events = TransactionOnlyEvents()
    binding = Binding(**config["binding"])
    factory = async_sessionmaker(database.engine, expire_on_commit=False)
    store = MySQLTransactionStore(factory, binding)
    archives = TransactionArchive(factory, binding)
    credentials = config["broker"]["ai"]
    sender = TransactionalSender(
        TransactionConfig(
            config["broker"]["endpoint"],
            credentials["username"],
            credentials["password"],
            "ai.mysql",
            (TransactionRoute(EVENTS, config["physical_topic"], "m3-1"),),
            Limits(sends=4, check_workers=1, check_queue=16),
        ),
        store,
    )
    runtime = TransactionRuntime(sender, admitted=True)
    invocation = contextvars.ContextVar("probe_invocation", default="")

    async def correlate(request):
        request.headers["X-Test-Invocation"] = invocation.get()

    class ResponseStore(MySQLExecutionStore):
        async def record_model_response(self, claim, invocation_id, **kwargs):
            await super().record_model_response(claim, invocation_id, **kwargs)
            if config["pause_after_response"] and kwargs.get("response_json") is not None:
                progress(marker, "response_committed", invocation_id=invocation_id)
                await asyncio.Event().wait()  # Parent verifies SQL, then SIGKILLs this worker.

    try:
        async with httpx.AsyncClient(event_hooks={"request": [correlate]}) as client:
            transport = DeepSeekResponses(
                client,
                config["provider_endpoint"],
                "isolated-test-token",
            )

            class Gateway:
                async def generate(self, prepared, route, schema, invocation_id):
                    token = invocation.set(invocation_id)
                    try:
                        response = await transport.generate(prepared, route, schema, invocation_id)
                        return ModelGatewayRouter._with_identity(response, route)
                    finally:
                        invocation.reset(token)

            original = ResponseStore(tx)
            execution = TransactionalInterpretationExecutionStore(
                original,
                archives,
                runtime,
                signing_key=config["signing_key_object"],
                recipient_key=config["recipient_key_object"],
            )
            workflow = PublishedReportWorkflow(
                MySQLExecutionConfigurations(tx),
                DurableGeneration(original, Gateway(), JSONModelCallCodec()),
                create_report_workflow,
            )
            worker = ExecuteNext(execution, SyntheticEvidence(), workflow)
            await runtime.start()
            progress(marker, "ready")
            worked = await worker.once(60)
            progress(marker, "done", worked=worked)
    finally:
        try:
            await runtime.close(grace_seconds=10)
        finally:
            await database.close()
            for handler in list(logger.handlers):
                logger.removeHandler(handler)
                handler.close()


if __name__ == "__main__":
    os.umask(0o077)
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    cfg = json.loads(parser.parse_args().config.read_text())
    Path(cfg["client_log"]).touch(mode=0o600)
    for key in ("signing_key", "recipient_key"):
        cfg[key + "_object"] = jwk.JWK.from_json(Path(cfg[key]).read_text())
    try:
        asyncio.run(run(cfg))
    except Exception as error:
        progress(Path(cfg["progress"]), "error", error_class=type(error).__name__)
        raise SystemExit(1) from None
