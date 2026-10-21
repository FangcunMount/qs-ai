"""A caller Run UUID binds explicit creation input, not a fresh clock or host mode."""

from dataclasses import replace
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.infrastructure.workflow_transport.creation_state_plan import CreationStateInput
from tests.test_generation_manifest import assets as assets
from tests.test_generation_manifest import complete_release as complete_release
from tests.test_generation_manifest import evaluation_release as evaluation_release


@pytest.mark.parametrize("reason", ["原创建意图", " 原创建意图 "])
async def test_creation_request_is_stable_for_same_public_input(complete_release, reason):
    scope = ManagementScope(uuid4(), 1, 42)
    incoming = CreationStateInput(scope, complete_release, reason)
    same = CreationStateInput(scope, complete_release, "原创建意图")
    assert incoming.intent == same.intent and incoming.reason == "原创建意图"
    assert incoming.intent.expected_version == 0
    assert incoming.intent.request.owner == "ai.mysql"
    assert incoming.intent.run_id == str(scope.run_id)
    assert "原创建意图" not in repr(incoming)
    # Neither the current host execution mode nor a newly sampled server time
    # is client intent. The winner's frozen plan carries those separate values.
    assert set(incoming.__dataclass_fields__) == {"scope", "release", "reason"}


@pytest.mark.parametrize("damage", ["actor", "org", "run", "release", "reason"])
async def test_different_authority_or_release_never_reuses_original_input_hash(
    complete_release, damage
):
    incoming = CreationStateInput(ManagementScope(uuid4(), 1, 42), complete_release, "原创建意图")
    if damage == "actor":
        changed = replace(incoming, scope=replace(incoming.scope, operator_user_id=43))
    elif damage == "org":
        changed = replace(incoming, scope=replace(incoming.scope, organization_id=2))
    elif damage == "run":
        changed = replace(incoming, scope=replace(incoming.scope, run_id=uuid4()))
    elif damage == "release":
        changed = replace(
            incoming,
            release=replace(
                complete_release,
                prompt=replace(complete_release.prompt, fingerprint="sha256:" + "f" * 64),
            ),
        )
    else:
        changed = replace(incoming, reason="不同创建意图")
    assert changed.intent.request.input_hash != incoming.intent.request.input_hash


@pytest.mark.parametrize("reason", ["", " ", "x" * 1001, "<private>"])
async def test_invalid_creation_intent_is_rejected_before_planning(complete_release, reason):
    with pytest.raises(ValueError):
        CreationStateInput(ManagementScope(uuid4(), 1, 42), complete_release, reason)
