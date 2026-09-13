import json
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import grpc
import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.governance.publication import (
    PublicationHistoryPage,
    PublicationHistoryQuery,
    PublicationReceipt,
    PublicationStore,
)
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.domain.governance.publication import (
    PublicationAudit,
    PublicationConflict,
    PublicationPointer,
    change_publication,
)
from qs_ai.infrastructure.persistence.mysql.publications import MySQLPublications
from qs_ai.transport.grpc.publication import PublicationManagement, selector_message
from tests.test_generation_manifest import assets as assets
from tests.test_generation_manifest import complete_release as complete_release
from tests.test_generation_manifest import evaluation_release as evaluation_release
from tests.test_grpc_commands import Aborted, Context
from tests.test_publication import evidence as evidence
from tests.test_publication import published


@pytest.fixture
async def handler(evidence):
    store = AsyncMock(spec=PublicationStore)
    approved_at = datetime.now(UTC) - timedelta(minutes=1)
    evidence = replace(
        evidence, final_review=replace(evidence.final_review, finalized_at=approved_at)
    )
    old = published(evidence, approved_at + timedelta(seconds=1))

    @asynccontextmanager
    async def container():
        class Operation:
            async def get(self, dependency):
                assert dependency is PublicationStore
                return store

        yield Operation()

    async def apply(scope, command, at):
        if command.action == "publish":
            target = published(evidence, at)
            previous = PublicationPointer(evidence.selector)
            audit = target.audit
        else:
            target = old if command.action == "rollback" else None
            active = published(evidence, old.audit.at)
            previous = PublicationPointer(evidence.selector, 1, active, active.audit.at)
            audit = PublicationAudit(scope.actor, command.reason, at)
        change = change_publication(
            previous,
            target,
            command.action,
            audit,
            expected_version=previous.version,
            expected_active_id=previous.active.publication_id if previous.active else None,
        )
        return PublicationReceipt(command.command_id, change)

    store.apply.side_effect = apply
    store.get.return_value = PublicationPointer(evidence.selector)
    return PublicationManagement(container), store, evidence, old


def request_for(method, evidence, old):
    scope = pb.PublicationScope(organization_id=1, operator_user_id=42)
    selector = selector_message(evidence.selector)
    if method == "ListHistory":
        return pb.PublicationHistoryQuery(scope=scope, selector=selector, limit=20)
    if method == "GetHistory":
        return pb.PublicationHistoryVersionQuery(scope=scope, selector=selector, version=1)
    if method == "Get":
        return pb.PublicationQuery(scope=scope, selector=selector)
    if method == "GetReceipt":
        return pb.PublicationReceiptQuery(scope=scope, command_id=str(uuid4()))
    common = dict(
        scope=scope,
        command_id=str(uuid4()),
        expected=pb.PublicationExpectation(selector=selector, version=0),
        reason="核对后操作",
        confirm=True,
    )
    if method == "Publish":
        return pb.PublicationPublishCommand(
            **common,
            run_id=str(evidence.run_id),
            run_version=evidence.run_version,
            release_fingerprint=evidence.release.fingerprint(),
        )
    common["expected"].version = 1
    common["expected"].active_publication_id = str(old.publication_id)
    if method == "Rollback":
        return pb.PublicationRollbackCommand(
            **common, target_publication_id=str(old.publication_id)
        )
    return pb.PublicationDisableCommand(**common)


@pytest.mark.parametrize(
    "method", ["Publish", "Rollback", "Disable", "Get", "GetReceipt", "ListHistory", "GetHistory"]
)
@pytest.mark.parametrize(
    "auth",
    [
        {},
        {"transport_security_type": [b"ssl"], "x509_common_name": [b"qs-ai.svc"]},
        {"transport_security_type": [b"insecure"], "x509_common_name": [b"qs-apiserver.svc"]},
    ],
)
async def test_all_operations_require_qs_workload_before_store_access(handler, method, auth):
    service, store, evidence, old = handler

    class Untrusted(Context):
        def auth_context(self):
            return auth

    with pytest.raises(Aborted) as error:
        await getattr(service, method)(request_for(method, evidence, old), Untrusted())
    assert error.value.args[0] == grpc.StatusCode.PERMISSION_DENIED
    assert store.mock_calls == []


@pytest.mark.parametrize("method", ["Publish", "Rollback", "Disable"])
@pytest.mark.parametrize(
    "invalid", ["scope", "confirmation", "expected", "uuid", "version", "reason", "selector"]
)
async def test_invalid_mutations_are_rejected_before_store(handler, method, invalid):
    service, store, evidence, old = handler
    request = request_for(method, evidence, old)
    if invalid == "scope":
        request.scope.operator_user_id = 0
    elif invalid == "confirmation":
        request.confirm = False
    elif invalid == "expected":
        request.ClearField("expected")
    elif invalid == "uuid":
        request.command_id = request.command_id.replace("-", "")
    elif invalid == "version":
        request.expected.version = -1
    elif invalid == "reason":
        request.reason = " "
    else:
        request.expected.selector.model_code = ""
    with pytest.raises(Aborted) as error:
        await getattr(service, method)(request, Context())
    assert error.value.args[0] == grpc.StatusCode.INVALID_ARGUMENT
    store.apply.assert_not_awaited()


@pytest.mark.parametrize("method", ["Publish", "Rollback", "Disable"])
async def test_commands_bind_confirmation_trusted_scope_and_server_time(handler, method):
    service, store, evidence, old = handler
    request = request_for(method, evidence, old)
    before = datetime.now(UTC)
    reply = await getattr(service, method)(request, Context())
    scope, command, at = store.apply.await_args.args
    assert (scope.organization_id, scope.actor) == (1, "user:42")
    assert before <= at <= datetime.now(UTC)
    assert command.confirm is True and command.reason == request.reason
    assert str(command.command_id) == request.command_id == reply.command_id
    assert command.expected_version == request.expected.version
    assert reply.action == method.lower()
    assert reply.current.version == reply.previous.version + 1
    if method == "Rollback":
        assert str(command.target_id) == request.target_publication_id
        assert reply.current.publication_json and reply.current.active_publication_id == str(
            old.publication_id
        )
    elif method == "Disable":
        assert command.target_id is None and reply.current.publication_json == ""


@pytest.mark.parametrize(
    "failure,code",
    [
        (PublicationConflict("private"), grpc.StatusCode.ABORTED),
        (CheckpointConflict("private"), grpc.StatusCode.ABORTED),
        (NotFound("private"), grpc.StatusCode.NOT_FOUND),
        (ValueError("private"), grpc.StatusCode.INVALID_ARGUMENT),
        (RuntimeError("private"), grpc.StatusCode.UNAVAILABLE),
    ],
)
async def test_failures_are_redacted_and_never_retried(handler, failure, code):
    service, store, evidence, old = handler
    store.apply.side_effect = failure
    with pytest.raises(Aborted) as error:
        await service.Publish(request_for("Publish", evidence, old), Context())
    assert error.value.args[0] == code and "private" not in str(error.value)
    assert store.apply.await_count == 1


async def test_queries_never_mutate_and_keep_optional_selector_semantics(handler):
    service, store, evidence, old = handler
    reply = await service.Get(request_for("Get", evidence, old), Context())
    assert reply.version == 0 and not reply.selector.HasField("model_code")
    request = request_for("Publish", evidence, old)
    accepted = await service.Publish(request, Context())
    receipt = store.apply.await_args.args
    # Obtain the same immutable result for the query test.
    store.get_receipt.return_value = await store.apply.side_effect(*receipt)
    store.apply.reset_mock()
    reply = await service.GetReceipt(
        pb.PublicationReceiptQuery(scope=request.scope, command_id=accepted.command_id), Context()
    )
    store.apply.assert_not_awaited()
    scope, command_id = store.get_receipt.await_args.args
    assert scope.actor == "user:42" and str(command_id) == reply.command_id


async def test_dependency_graph_resolves_publication_store_without_activating_governance():
    settings = Settings(environment="production", database_url=None)
    assert settings.grpc.governance_enabled is False
    container = create_container(settings)
    try:
        async with container() as operation:
            assert isinstance(await operation.get(PublicationStore), MySQLPublications)
    finally:
        await container.close()


@pytest.mark.parametrize("method", ["ListHistory", "GetHistory"])
@pytest.mark.parametrize("invalid", ["scope", "selector", "version"])
async def test_history_rejects_invalid_queries_before_store(handler, method, invalid):
    service, store, evidence, old = handler
    request = request_for(method, evidence, old)
    if invalid == "scope":
        request.scope.organization_id = 0
    elif invalid == "selector":
        request.selector.model_version = "v1"
    elif method == "ListHistory":
        request.before_version = -1
    else:
        request.version = 0
    with pytest.raises(Aborted) as error:
        await getattr(service, method)(request, Context())
    assert error.value.args[0] == grpc.StatusCode.INVALID_ARGUMENT
    assert store.mock_calls == []


@pytest.mark.parametrize("limit", [0, -1, 21])
async def test_history_page_limit_is_bounded(handler, limit):
    service, store, evidence, old = handler
    request = request_for("ListHistory", evidence, old)
    request.limit = limit
    with pytest.raises(Aborted):
        await service.ListHistory(request, Context())
    assert store.mock_calls == []


async def test_history_queries_preserve_original_actor_without_mutation(handler):
    service, store, evidence, old = handler
    request = request_for("Publish", evidence, old)
    await service.Publish(request, Context())
    receipt = await store.apply.side_effect(*store.apply.await_args.args)
    store.apply.reset_mock()
    store.get_history.return_value = receipt
    query = request_for("GetHistory", evidence, old)
    query.scope.operator_user_id = 99
    result = await service.GetHistory(query, Context())
    assert result.actor == "user:42"
    store.get_history.assert_awaited_once_with(evidence.selector, 1)
    store.get_receipt.assert_not_awaited()
    store.list_history.return_value = PublicationHistoryPage(evidence.selector, ())
    reply = await service.ListHistory(request_for("ListHistory", evidence, old), Context())
    body = json.loads(reply.payload_json)
    assert body["schema_version"] == reply.schema_version == "qs-ai-publication-history/v1"
    assert body["entries"] == [] and body["next_before_version"] == 0
    store.list_history.assert_awaited_once_with(PublicationHistoryQuery(evidence.selector))
    store.apply.assert_not_awaited()


@pytest.mark.parametrize(
    "kwargs", [{"limit": True}, {"before_version": True}, {"before_version": 2**63}, {"limit": 1.5}]
)
def test_history_query_rejects_unsafe_domain_values(kwargs):
    from qs_ai.domain.governance.publication import ReleaseSelector

    with pytest.raises(ValueError):
        PublicationHistoryQuery(ReleaseSelector("participant", "scale", "score_range"), **kwargs)
