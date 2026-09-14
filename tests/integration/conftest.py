"""Published configuration fixtures for current participant admission tests.

These traverse the real asset, evaluation, human-review and publication transactions.
They are opt-in, so lower-level storage tests can still exercise invalid records.
"""

from types import SimpleNamespace

import pytest

from qs_ai.infrastructure.persistence.mysql.publications import MySQLPublications
from qs_ai.infrastructure.qs_server.evaluation_suite import V6_PUBLISHED
from tests.integration import test_publications
from tests.integration.test_publications import dispatched as dispatched
from tests.integration.test_publications import judge as judge
from tests.integration.test_publications import passing_reviewable as passing_reviewable
from tests.integration.test_publications import passing_semantics as passing_semantics
from tests.integration.test_publications import persisted_assets as persisted_assets
from tests.integration.test_publications import ready as ready
from tests.integration.test_publications import reviewable as reviewable
from tests.integration.test_publications import setup_run as setup_run


@pytest.fixture
async def freeze_creation(setup_run, persisted_assets, monkeypatch):
    async for _ in test_publications.freeze_creation.__wrapped__(
        setup_run, persisted_assets, monkeypatch, SimpleNamespace(param=V6_PUBLISHED)
    ):
        yield


@pytest.fixture
async def published_configuration(freeze_creation, ready):
    tx, scope, command, at = ready
    return await MySQLPublications(tx).apply(scope, command, at)
