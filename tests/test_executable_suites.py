from dataclasses import replace
from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from qs_ai.infrastructure.persistence.mysql.evaluation_runs import create_run
from qs_ai.infrastructure.persistence.mysql.evaluation_suites import load_registered_suite
from qs_ai.infrastructure.qs_server.evaluation_input import validate_suite_inputs
from qs_ai.infrastructure.qs_server.evaluation_suite import V6, load_suite
from tests.test_evaluation_case import release


async def test_old_suite_cannot_be_resolved_for_execution():
    db = AsyncMock()
    with pytest.raises(ValueError, match="baseline"):
        await load_registered_suite(db, V6)
    db.execute.assert_not_awaited()


async def test_old_suite_cannot_create_a_run_or_any_checkpoint():
    db = AsyncMock()
    with pytest.raises(ValueError, match="baseline"):
        await create_run(
            db, uuid4(), replace(release(), suite=V6), 1, "user:42", "test", datetime.now(UTC)
        )
    db.execute.assert_not_awaited()


def test_baseline_suite_cannot_bypass_input_validation():
    with pytest.raises(ValueError, match="input contract"):
        validate_suite_inputs(load_suite(V6), release().input_schema)
