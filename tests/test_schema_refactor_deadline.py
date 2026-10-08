"""Budget expiration interrupts streamed verification without accepting partial facts."""

from unittest.mock import Mock

import pytest

from qs_ai.maintenance.schema_refactor import conversion
from qs_ai.maintenance.schema_refactor.layouts import OLD_HEAD


def test_expired_digest_does_not_issue_a_query(monkeypatch):
    connection = Mock(info={"schema_refactor_deadline": 1.0})
    monkeypatch.setattr(conversion.time, "monotonic", lambda: 2.0)
    with pytest.raises(TimeoutError, match="verification deadline"):
        conversion.digest(connection, "isolated", "external_requests", OLD_HEAD)
    connection.execute.assert_not_called()


def test_digest_closes_stream_and_rejects_partial_hash_when_fetch_exhausts_budget(monkeypatch):
    clock = [0.0]
    connection = Mock(info={"schema_refactor_deadline": 1.0})
    result = connection.execute.return_value

    def fetched(_size):
        clock[0] = 2.0
        return [("36:request_hash", "36:session_hash")]

    result.fetchmany.side_effect = fetched
    monkeypatch.setattr(conversion.time, "monotonic", lambda: clock[0])
    with pytest.raises(TimeoutError, match="verification deadline"):
        conversion.digest(connection, "isolated", "external_requests", OLD_HEAD)
    result.fetchmany.assert_called_once_with(500)
    result.close.assert_called_once_with()
