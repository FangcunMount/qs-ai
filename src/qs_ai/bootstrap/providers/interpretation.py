import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

from dishka import Provider, Scope, provide

from qs_ai.application.execution.capacity import ParticipantCapacityPolicy
from qs_ai.application.execution.management import ParticipantCapacityReader
from qs_ai.application.execution.retry import ParticipantRetryStore, RetryParticipant
from qs_ai.application.execution.worker import ExecuteNext
from qs_ai.application.interpretation.ports import (
    EvidenceSource,
    ExecutionStore,
    UnitOfWorkFactory,
)
from qs_ai.application.interpretation.service import InterpretationService
from qs_ai.config import Settings
from qs_ai.infrastructure.interpretation.unconfigured import (
    UnconfiguredEvidenceSource,
)
from qs_ai.infrastructure.persistence.mysql.execution import MySQLExecutionStore
from qs_ai.infrastructure.persistence.mysql.interpretation import MySQLUnitOfWorkFactory
from qs_ai.infrastructure.persistence.mysql.participant_management import (
    MySQLParticipantCapacityReader,
)
from qs_ai.infrastructure.persistence.mysql.participant_retries import MySQLParticipantRetries
from qs_ai.infrastructure.qs_server.access import QSAccessSource
from qs_ai.infrastructure.qs_server.report_probe import mtls_channel


class InterpretationProvider(Provider):
    @provide(scope=Scope.APP)
    def participant_capacity(self, settings: Settings) -> ParticipantCapacityPolicy:
        return ParticipantCapacityPolicy(**settings.participant_capacity.model_dump())

    capacity_reader = provide(
        MySQLParticipantCapacityReader, provides=ParticipantCapacityReader, scope=Scope.REQUEST
    )
    retries = provide(MySQLParticipantRetries, provides=ParticipantRetryStore, scope=Scope.REQUEST)
    retry_participant = provide(RetryParticipant, scope=Scope.REQUEST)

    @provide(scope=Scope.APP)
    async def source(self, settings: Settings) -> AsyncIterator[EvidenceSource]:
        options = settings.grpc
        if not options.access_address:
            yield UnconfiguredEvidenceSource()
            return
        if not all((options.ca_file, options.cert_file, options.key_file)):
            raise ValueError("QS authorization requires CA, certificate and private key")
        ca, cert, key = await asyncio.gather(
            *(
                asyncio.to_thread(Path(path).read_bytes)
                for path in (options.ca_file, options.cert_file, options.key_file)
                if path
            )
        )
        async with mtls_channel(options.access_address, ca, key, cert) as channel:
            yield QSAccessSource(channel, options.request_timeout_seconds)

    uows = provide(MySQLUnitOfWorkFactory, provides=UnitOfWorkFactory, scope=Scope.REQUEST)
    store = provide(MySQLExecutionStore, provides=ExecutionStore, scope=Scope.REQUEST)

    @provide(scope=Scope.REQUEST)
    def service(
        self, settings: Settings, uows: UnitOfWorkFactory, source: EvidenceSource
    ) -> InterpretationService:
        return InterpretationService(
            uows, source, use_publications=settings.generation.use_publications
        )

    worker = provide(ExecuteNext, scope=Scope.REQUEST)
