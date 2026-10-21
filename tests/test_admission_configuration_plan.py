"""Read planning shares original validation; root binding rechecks the same evidence."""

from dataclasses import FrozenInstanceError, asdict, dataclass, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from sqlalchemy.dialects import mysql

from qs_ai.infrastructure.persistence.mysql import execution_configurations as config
from tests.test_input_binding import bound_case


@dataclass(frozen=True)
class Query:
    organization_id: int = 1

    def key(self):
        return "selector"

    def admission_candidates(self):
        return (self,)


def configured(monkeypatch):
    session, evidence, _ = bound_case()
    query = Query()
    publication = SimpleNamespace(publication_id=uuid4())
    pointer = SimpleNamespace(active=publication, version=3)
    mapping = Mock()
    mapping.mappings.return_value.all.return_value = [{"pointer": "original"}]
    db = SimpleNamespace(execute=AsyncMock(return_value=mapping))
    compiled = SimpleNamespace(
        route=object(), release=object(), package=object(), validate_input=Mock()
    )
    monkeypatch.setattr(config, "report_selector", Mock(return_value=query))
    monkeypatch.setattr(config, "load_pointer", AsyncMock(return_value=pointer))
    monkeypatch.setattr(config, "resolve_publication", Mock(return_value=publication))
    monkeypatch.setattr(config, "compile_configuration", AsyncMock(return_value=compiled))
    monkeypatch.setattr(config, "validate_v2_admission", Mock())
    monkeypatch.setattr(
        config,
        "prepare_explanation",
        Mock(
            return_value=SimpleNamespace(
                assembled_input=SimpleNamespace(canonical_json='{"frozen":true}')
            )
        ),
    )
    monkeypatch.setattr(config, "publication_hash", Mock(return_value="a" * 64))
    return SimpleNamespace(**locals())


async def test_read_only_configuration_plan_freezes_binding_after_original_validation(monkeypatch):
    f = configured(monkeypatch)
    planned = await config.plan_configuration(f.db, f.session, f.evidence)
    assert asdict(planned) == {
        "session_id": f.session.id,
        "evidence_set_id": f.evidence.id,
        "evidence_fingerprint": f.evidence.fingerprint,
        "publication_id": str(f.publication.publication_id),
        "publication_sha256": "a" * 64,
        "pointer_version": 3,
        "selector_query": config.canonical(asdict(f.query)),
    }
    assert f.db.execute.await_count == 1
    statement = f.db.execute.await_args.args[0]
    assert statement.is_select and statement._for_update_arg is None
    config.compile_configuration.assert_awaited_once_with(f.db, f.publication)
    f.compiled.validate_input.assert_called_once_with('{"frozen":true}')
    with pytest.raises(FrozenInstanceError):
        planned.pointer_version = 4


async def test_original_bind_keeps_same_values_and_defers_insert_until_validation(monkeypatch):
    f = configured(monkeypatch)
    await config.bind_configuration(f.db, f.session, f.evidence)
    assert f.db.execute.await_count == 2
    first, second = [call.args[0] for call in f.db.execute.await_args_list]
    assert first.is_select and first._for_update_arg is None
    assert second.is_insert
    assert second.compile(dialect=mysql.dialect()).params["publication_id"] == str(
        f.publication.publication_id
    )
    f.compiled.validate_input.assert_called_once()


@pytest.mark.parametrize(
    "changed", ["publication_sha256", "pointer_version", "evidence_fingerprint"]
)
async def test_frozen_binding_conflict_never_writes_or_becomes_business_refusal(
    monkeypatch, changed
):
    f = configured(monkeypatch)
    first = await config.plan_configuration(f.db, f.session, f.evidence)
    value = 4 if changed == "pointer_version" else "changed"
    expected = replace(first, **{changed: value})
    f.db.execute.reset_mock()
    with pytest.raises(config.ConfigurationPlanConflict) as error:
        await config.bind_configuration(f.db, f.session, f.evidence, expected=expected)
    assert not isinstance(error.value, ValueError), "stale plan is not an invalid-command decision"
    assert f.db.execute.await_count == 1
    statement = f.db.execute.await_args.args[0]
    assert statement.is_select and statement._for_update_arg is not None


async def test_matching_frozen_binding_rechecks_locked_pointer_then_writes_original_values(
    monkeypatch,
):
    f = configured(monkeypatch)
    planned = await config.plan_configuration(f.db, f.session, f.evidence)
    f.db.execute.reset_mock()
    await config.bind_configuration(f.db, f.session, f.evidence, expected=planned)
    first, second = [call.args[0] for call in f.db.execute.await_args_list]
    assert first.is_select and first._for_update_arg is not None
    assert second.compile(dialect=mysql.dialect()).params == asdict(planned)


async def test_failed_original_input_validation_cannot_create_binding(monkeypatch):
    f = configured(monkeypatch)
    f.compiled.validate_input.side_effect = ValueError("invalid input")
    with pytest.raises(ValueError):
        await config.bind_configuration(f.db, f.session, f.evidence)
    assert f.db.execute.await_count == 1 and f.db.execute.await_args.args[0].is_select
