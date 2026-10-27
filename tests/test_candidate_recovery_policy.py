"""Only the configured host recovery policy authorizes one exact old-Aborted recovery."""

from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from reliable_messaging.durable import MessageConflict
from reliable_messaging.transaction import Execution, Reservation, State

from qs_ai.infrastructure.workflow_transport.candidate_recovery_runtime import (
    CandidateRecoveryRuntime,
)
from tests.route_fixtures import legacy_routes
from tests.test_candidate_completion_state import frozen as frozen
from tests.test_candidate_expiry_sender import fixture as expiry_fixture
from tests.test_evaluation_serial_response_receipts import original as original


@pytest.mark.parametrize("status", [State.PREPARED, State.COMMITTED, State.ABORTED])
@pytest.mark.parametrize("advanced", [False, True])
async def test_expiry_policy_only_recovers_exact_abort_after_bounded_fresh_observation(
    original, monkeypatch, status, advanced
):
    f = await expiry_fixture(original, monkeypatch)
    f.store.lookup.return_value = Reservation(f.execution, False, status)
    runtime = CandidateRecoveryRuntime(
        f.tx, f.archives, f.runtime, signing_key=None, recipient_key=None, recover_aborted=True
    )
    expiry = AsyncMock(return_value=True)
    expiry.retry, expiry.replan = AsyncMock(return_value=True), AsyncMock(return_value=True)
    runtime._expiry = expiry
    version = f.facts.source_version + int(advanced)
    assert await runtime._finish_expiry(f.p.request, version, f.facts.at)
    f.store.lookup.assert_awaited_once_with(f.p.request.intent.request)
    if status != State.ABORTED:
        expiry.assert_awaited_once_with(f.p.request, f.facts.at)
        expiry.retry.assert_not_called()
        expiry.replan.assert_not_called()
    else:
        expiry.assert_not_called()
        if advanced:
            expiry.replan.assert_awaited_once_with(f.p.request, f.execution, f.facts.at)
            expiry.retry.assert_not_called()
        else:
            expiry.retry.assert_awaited_once_with(f.p.request, f.execution)
            expiry.replan.assert_not_called()


async def test_expiry_policy_can_replan_renewed_expiry_but_cannot_recover_regressed_coordinator(
    original, monkeypatch
):
    f = await expiry_fixture(original, monkeypatch)
    f.store.lookup.return_value = Reservation(f.execution, False, State.ABORTED)
    runtime = CandidateRecoveryRuntime(
        f.tx, f.archives, f.runtime, signing_key=None, recipient_key=None, recover_aborted=True
    )
    runtime._expiry = AsyncMock()
    renewed = replace(
        f.p.request,
        claim=replace(
            f.p.claim,
            checkpoint=replace(
                f.p.cp, lease_expires_at=f.p.cp.lease_expires_at + timedelta(seconds=30)
            ),
        ),
    )
    at = renewed.claim.checkpoint.lease_expires_at
    await runtime._finish_expiry(renewed, f.facts.source_version, at)
    runtime._expiry.replan.assert_awaited_once_with(renewed, f.execution, at)
    runtime._expiry.retry.assert_not_called()
    runtime._expiry.reset_mock()
    with pytest.raises(MessageConflict):
        await runtime._finish_expiry(f.p.request, f.facts.source_version - 1, at)
    runtime._expiry.replan.assert_not_called()
    runtime._expiry.retry.assert_not_called()


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("advanced", [False, True])
async def test_first_response_policy_preserves_reference_and_chooses_retry_or_explicit_new_source(
    frozen, enabled, advanced
):
    f = frozen
    execution = Execution(f.winner, 1, "m3-1", b"x" * 32, f.facts.at + timedelta(seconds=60))
    lookup = AsyncMock(return_value=Reservation(execution, False, State.ABORTED))
    owner = SimpleNamespace(routing=legacy_routes(), store=SimpleNamespace(lookup=lookup))
    runtime = CandidateRecoveryRuntime(
        Mock(), Mock(), owner, signing_key=None, recipient_key=None, recover_aborted=enabled
    )
    runtime._completion_planner.intent = AsyncMock(return_value=f.intent)
    runtime._completion = AsyncMock()
    reference = f.operation.reference
    await runtime._finish_response(reference, f.facts.source_version + int(advanced))
    if not enabled:
        lookup.assert_not_called()
        runtime._completion.assert_awaited_once_with(reference)
        runtime._completion.retry.assert_not_called()
        runtime._completion.replan.assert_not_called()
    else:
        lookup.assert_awaited_once_with(f.intent.request)
        runtime._completion.assert_not_called()
        selected = runtime._completion.replan if advanced else runtime._completion.retry
        selected.assert_awaited_once_with(reference, execution)


@pytest.mark.parametrize("damage", ["identity", "coordinator"])
async def test_corrupt_aborted_response_identity_is_not_automatic_replan_authority(frozen, damage):
    f = frozen
    execution = Execution(f.winner, 1, "m3-1", b"x" * 32, f.facts.at + timedelta(seconds=60))
    owner = SimpleNamespace(
        routing=legacy_routes(),
        store=SimpleNamespace(
            lookup=AsyncMock(return_value=Reservation(execution, False, State.ABORTED))
        ),
    )
    runtime = CandidateRecoveryRuntime(
        Mock(), Mock(), owner, signing_key=None, recipient_key=None, recover_aborted=True
    )
    runtime._completion_planner.intent = AsyncMock(return_value=f.intent)
    runtime._completion = AsyncMock()
    reference = f.operation.reference
    version = f.facts.source_version
    if damage == "identity":
        reference = replace(reference, organization_id=2)
    else:
        version -= 1
    with pytest.raises(MessageConflict):
        await runtime._finish_response(reference, version)
    runtime._completion.assert_not_called()
    runtime._completion.retry.assert_not_called()
    runtime._completion.replan.assert_not_called()
