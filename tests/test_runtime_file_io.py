"""Fixed database suite verification may not block the shared event loop or read files."""

import threading
from unittest.mock import AsyncMock, Mock

from qs_ai.infrastructure.persistence.mysql import evaluation_suites
from qs_ai.infrastructure.qs_server.evaluation_suite import V6_PUBLISHED, load_suite


async def test_database_suite_verification_runs_off_event_loop(monkeypatch):
    raw = load_suite(V6_PUBLISHED).definition_json
    owner = threading.get_ident()
    original = evaluation_suites.load_suite
    calls = []
    row = dict(
        suite_id=V6_PUBLISHED.id,
        suite_version=V6_PUBLISHED.version,
        fingerprint=V6_PUBLISHED.fingerprint,
        definition_json=raw,
        organization_id=0,
        command_id=None,
        operator_user_id=None,
        receipt_json=None,
        receipt_sha256=None,
        source_ref="original",
        imported_by="test",
    )
    result = Mock()
    result.mappings.return_value.one_or_none.return_value = row
    db = Mock(execute=AsyncMock(return_value=result))

    def verified(reference, **kwargs):
        assert threading.get_ident() != owner
        assert kwargs["definition_json"] == raw
        calls.append(reference)
        return original(reference, **kwargs)

    def forbidden(*args, **kwargs):
        raise AssertionError("Runtime suite loading may not read a file")

    monkeypatch.setattr(evaluation_suites, "load_suite", verified)
    monkeypatch.setattr("pathlib.Path.read_bytes", forbidden)
    suite = await evaluation_suites.load_registered_suite(db, V6_PUBLISHED, organization_id=1)
    assert suite.reference == V6_PUBLISHED
    assert calls == [V6_PUBLISHED]
