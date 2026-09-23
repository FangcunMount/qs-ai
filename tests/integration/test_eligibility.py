"""Real MySQL publication checks with SQL write guards and synthetic report facts."""

from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import event, select, update

from qs_ai.application.governance.publication import MovePublication
from qs_ai.application.governance.solution_models import DEFAULT_EDITABLE_MODELS
from qs_ai.application.interpretation.eligibility import Eligibility
from qs_ai.infrastructure.persistence.mysql.eligibility import MySQLEligibilityReader
from qs_ai.infrastructure.persistence.mysql.publications import MySQLPublications
from qs_ai.infrastructure.persistence.mysql.schema import configuration_publications
from qs_ai.infrastructure.qs_server.evaluation_suite import V6_PUBLISHED
from tests.integration.test_execution_configurations import dispatched as dispatched
from tests.integration.test_execution_configurations import freeze_creation as freeze_creation
from tests.integration.test_execution_configurations import judge as judge
from tests.integration.test_execution_configurations import passing_reviewable as passing_reviewable
from tests.integration.test_execution_configurations import passing_semantics as passing_semantics
from tests.integration.test_execution_configurations import persisted_assets as persisted_assets
from tests.integration.test_execution_configurations import ready as ready
from tests.integration.test_execution_configurations import reviewable as reviewable
from tests.integration.test_execution_configurations import setup_run as setup_run
from tests.test_input_binding import bound_case
from tests.test_mbti_runtime import mbti_case

pytestmark = [
    pytest.mark.integration,
    pytest.mark.parametrize("freeze_creation", [V6_PUBLISHED], indirect=True),
]


async def readonly_check(tx, actor, items):
    statements = []

    def guard(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)
        assert statement.lstrip().upper().startswith("SELECT"), statement
        assert "FOR UPDATE" not in statement.upper()

    engine = tx.database.engine.sync_engine
    event.listen(engine, "before_cursor_execute", guard)
    try:
        result = await MySQLEligibilityReader(tx, DEFAULT_EDITABLE_MODELS).check(
            actor, "7", ("42",), items
        )
        assert statements
        return result
    finally:
        event.remove(engine, "before_cursor_execute", guard)


async def test_available_paused_and_corrupt_publication_are_distinct_readonly(ready):
    tx, scope, command, at = ready
    session, evidence, _ = bound_case()
    publications = MySQLPublications(tx)
    published = await publications.apply(scope, command, at)
    assert await readonly_check(tx, session.actor, evidence.items) == Eligibility("available")
    # The MBTI selector cannot adopt the scale publication, including a generic one.
    claim, mbti_evidence, _ = mbti_case()
    assert await readonly_check(tx, claim.session.actor, mbti_evidence.items) == Eligibility(
        "unavailable", "publication_missing"
    )
    active_id = published.change.current.active.publication_id
    async with tx.open() as db:
        original = await db.scalar(
            select(configuration_publications.c.content_sha256).where(
                configuration_publications.c.publication_id == str(active_id)
            )
        )
        await db.execute(
            update(configuration_publications)
            .where(configuration_publications.c.publication_id == str(active_id))
            .values(content_sha256="0" * 64)
        )
        await db.commit()
    try:
        assert await readonly_check(tx, session.actor, evidence.items) == Eligibility(
            "unavailable", "asset_invalid"
        )
    finally:
        async with tx.open() as db:
            await db.execute(
                update(configuration_publications)
                .where(configuration_publications.c.publication_id == str(active_id))
                .values(content_sha256=original)
            )
            await db.commit()
    await publications.apply(
        scope,
        MovePublication(uuid4(), command.selector, 1, active_id, "测试暂停", True, None),
        at + timedelta(seconds=1),
    )
    assert await readonly_check(tx, session.actor, evidence.items) == Eligibility(
        "unavailable", "publication_paused"
    )
