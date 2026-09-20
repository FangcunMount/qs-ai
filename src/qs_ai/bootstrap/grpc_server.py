"""Explicit internal mTLS entry; no development identity or workflow fallback."""

import grpc
from dishka import AsyncContainer
from grpc import aio

from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.transport.grpc.asset_catalog import AssetCatalogService
from qs_ai.transport.grpc.commands import Commands
from qs_ai.transport.grpc.evaluation import EvaluationManagement
from qs_ai.transport.grpc.participant import ParticipantManagement
from qs_ai.transport.grpc.profile_registration import ProfileManagement
from qs_ai.transport.grpc.prompt_drafts import PromptDraftManagement
from qs_ai.transport.grpc.publication import PublicationManagement
from qs_ai.transport.grpc.solutions import SolutionManagement
from qs_ai.transport.grpc.suite_registration import SuiteManagement


def create_grpc_server(
    container: AsyncContainer, settings: Settings, ca: bytes, cert: bytes, key: bytes
) -> aio.Server:
    server = aio.server(
        options=(("grpc.max_receive_message_length", settings.grpc.max_receive_bytes),)
    )
    rpc.add_CommandsServicer_to_server(Commands(container), server)
    if settings.grpc.governance_enabled:
        rpc.add_SolutionManagementServicer_to_server(SolutionManagement(container), server)
        rpc.add_AssetCatalogServicer_to_server(AssetCatalogService(container), server)
        rpc.add_SuiteManagementServicer_to_server(SuiteManagement(container), server)
        rpc.add_ProfileManagementServicer_to_server(ProfileManagement(container), server)
        rpc.add_PromptDraftManagementServicer_to_server(PromptDraftManagement(container), server)
        rpc.add_PublicationManagementServicer_to_server(PublicationManagement(container), server)
        rpc.add_ParticipantManagementServicer_to_server(ParticipantManagement(container), server)
        rpc.add_EvaluationManagementServicer_to_server(EvaluationManagement(container), server)
    credentials = grpc.ssl_server_credentials(
        [(key, cert)], root_certificates=ca, require_client_auth=True
    )
    if server.add_secure_port(settings.grpc.bind_address, credentials) == 0:
        raise RuntimeError("Unable to bind internal service")
    return server
