"""Original preparation IDs and edited routes freeze before the business callback."""

from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4, uuid5

import pytest

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.solution_models import selection
from qs_ai.application.interpretation.route_assets import executable_route
from qs_ai.infrastructure.persistence.mysql import solution_assets as preparation
from tests.test_generation_manifest import assets as assets
from tests.test_generation_manifest import complete_release as complete_release
from tests.test_generation_manifest import evaluation_release as evaluation_release


def test_original_preparation_identities_are_frozen_as_one_immutable_operation():
    solution_id, command_id = uuid4(), uuid4()
    first = preparation.PreparationIDs.freeze(solution_id, command_id)
    assert first == preparation.PreparationIDs.freeze(solution_id, command_id)
    assert first.prompt_command_id == uuid5(command_id, "freeze")
    assert first.profile_command_id == uuid5(command_id, "profile")
    assert first.suite_command_id == uuid5(command_id, "suite")
    assert first.run_id == uuid5(solution_id, "evaluation")
    with pytest.raises(ValueError):
        replace(first, run_id=uuid4())
    with pytest.raises(ValueError):
        replace(first, command_id=uuid4())


@pytest.mark.parametrize("purpose", ["generation", "semantic"])
async def test_original_route_bytes_and_provenance_stay_in_the_same_caller_root(
    assets, complete_release, monkeypatch, purpose
):
    source = assets[0][2].get.return_value
    original = executable_route(source)
    solution_id, scope = uuid4(), DraftScope(1, 42)
    values = asdict(replace(selection(original), max_output_tokens=4000))
    candidate = preparation.freeze_route(source, solution_id, purpose, values, (original.model,))
    expected = replace(
        original, revision=f"solution-{solution_id}-{purpose}", max_output_tokens=4000
    )
    assert candidate.definition_json == expected.definition_json()
    assert candidate.fingerprint == expected.fingerprint()
    source_read = AsyncMock(return_value=source)
    monkeypatch.setattr(preparation, "route_for", source_read)
    db = SimpleNamespace(execute=AsyncMock(), commit=AsyncMock(), rollback=AsyncMock())
    result = await preparation.register_route(
        db, scope, solution_id, complete_release, purpose, values, (original.model,)
    )
    assert result == preparation.route_reference(candidate)
    source_read.assert_awaited_once_with(db, getattr(complete_release, purpose + "_route"))
    fields = db.execute.await_args.args[0].compile().params
    assert fields["body_bytes"] == candidate.definition_json.encode()
    assert fields["source_ref"] == f"qs-ai:solution:{solution_id}"
    assert fields["imported_by"] == "qs:org/1/user/42"
    db.commit.assert_not_called()
    db.rollback.assert_not_called()


async def test_invalid_route_selection_never_writes_asset(assets):
    source = assets[0][2].get.return_value
    values = asdict(replace(selection(executable_route(source)), model="not-allowed"))
    with pytest.raises(ValueError):
        preparation.freeze_route(source, uuid4(), "generation", values, ("deepseek-v4-pro",))
