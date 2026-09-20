"""Asset verification remains exact but may not block the shared event loop."""

import threading

from qs_ai.infrastructure.persistence.mysql import evaluation_suites
from qs_ai.infrastructure.qs_server.evaluation_suite import V6_PUBLISHED


async def test_builtin_suite_verification_runs_off_event_loop(monkeypatch):
    owner = threading.get_ident()
    original = evaluation_suites.load_suite
    calls = []

    def verified(reference):
        assert threading.get_ident() != owner
        calls.append(reference)
        return original(reference)

    monkeypatch.setattr(evaluation_suites, "load_suite", verified)
    suite = await evaluation_suites.load_registered_suite(None, V6_PUBLISHED)
    assert suite.reference == V6_PUBLISHED
    assert calls == [V6_PUBLISHED]
