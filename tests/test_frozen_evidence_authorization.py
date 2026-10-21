"""Authorization snapshots are target-bound; the root has no external client."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from qs_ai.application.interpretation.ports import AccessDenied
from qs_ai.domain.interpretation.model import Actor
from qs_ai.infrastructure.persistence.mysql.execution_configurations import (
    ConfigurationPlanConflict,
)
from qs_ai.infrastructure.workflow_transport.frozen_authorization import (
    FrozenEvidenceSource,
    RecordingEvidenceSource,
)


@pytest.mark.parametrize("allowed", [False, True])
async def test_current_external_authorization_is_recorded_before_frozen_root(allowed):
    source = AsyncMock()
    if not allowed:
        source.authorize.side_effect = AccessDenied()
    at = datetime(2026, 10, 21, tzinfo=UTC)
    recording = RecordingEvidenceSource(source, clock=lambda: at)
    target = (Actor("7", "9"), "19", ("42",))
    if allowed:
        await recording.authorize(*target)
    else:
        with pytest.raises(AccessDenied):
            await recording.authorize(*target)
    source.authorize.assert_awaited_once_with(*target)
    assert recording.proof is not None
    assert recording.proof.allowed is allowed and recording.proof.observed_at == at
    root = FrozenEvidenceSource(recording.proof)
    if allowed:
        await root.authorize(*target)
    else:
        with pytest.raises(AccessDenied):
            await root.authorize(*target)
    assert source.authorize.await_count == 1


async def test_external_unknown_cannot_be_recorded_as_allow_or_deny():
    source = AsyncMock()
    source.authorize.side_effect = ConnectionError("authorization unavailable")
    recording = RecordingEvidenceSource(source)
    with pytest.raises(ConnectionError):
        await recording.authorize(Actor("7", "9"), "19", ("42",))
    assert recording.proof is None


async def test_frozen_authorization_cannot_expand_to_another_subject_or_target():
    recording = RecordingEvidenceSource(AsyncMock())
    await recording.authorize(Actor("7", "9"), "19", ("42",))
    root = FrozenEvidenceSource(recording.proof)
    for target in (
        (Actor("7", "10"), "19", ("42",)),
        (Actor("8", "9"), "19", ("42",)),
        (Actor("7", "9"), "20", ("42",)),
        (Actor("7", "9"), "19", ("43",)),
    ):
        with pytest.raises(ConfigurationPlanConflict):
            await root.authorize(*target)


async def test_missing_authorization_is_not_permission_to_call_an_external_service_in_root():
    with pytest.raises(ConfigurationPlanConflict):
        await FrozenEvidenceSource(None).authorize(Actor("7", "9"), "19", ("42",))
