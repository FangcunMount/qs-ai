"""Original SQL writes borrow a planned job identity without a new allocation."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from sqlalchemy.dialects import mysql

from qs_ai.domain.interpretation.model import Actor, Session
from qs_ai.infrastructure.persistence.mysql import interpretation


@pytest.mark.parametrize("frozen", [False, True])
async def test_original_enqueue_borrows_job_id_and_keeps_run_and_payload(monkeypatch, frozen):
    identity = str(uuid4())
    allocate = Mock(return_value=identity)
    if frozen:
        allocate.side_effect = AssertionError("original root must not allocate a replacement job")
    monkeypatch.setattr(interpretation, "uuid4", allocate)
    db = SimpleNamespace(execute=AsyncMock())
    session = Session(str(uuid4()), Actor("1", "2"), "7", ("42",), "goal")
    session.queue(str(uuid4()))
    kwargs = {"job_id": identity} if frozen else {}
    await interpretation.MySQLUnitOfWork(db, **kwargs).enqueue(session, "answer", False, "q")
    writes = [
        call.args[0].compile(dialect=mysql.dialect()).params for call in db.execute.await_args_list
    ]
    assert len(writes) == 2
    assert writes[0]["id"] == session.active_run_id
    assert writes[1]["id"] == identity
    assert writes[1]["run_id"] == session.active_run_id
    assert writes[1]["session_id"] == session.id
    assert writes[1]["answer"] == "answer" and writes[1]["question_id"] == "q"
    if frozen:
        allocate.assert_not_called()
    else:
        allocate.assert_called_once_with()


@pytest.mark.parametrize("identity", ["", "invalid", str(uuid4()).upper()])
def test_invalid_frozen_job_id_is_rejected_before_sql(identity):
    db = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(ValueError):
        interpretation.MySQLUnitOfWork(db, job_id=identity)
    db.execute.assert_not_called()
