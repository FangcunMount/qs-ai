import pytest

from qs_ai.application.execution.configuration import ConfigurationUnavailable
from qs_ai.infrastructure.persistence.mysql.execution_configurations import compile_configuration
from qs_ai.infrastructure.persistence.mysql.publications import MySQLPublications
from tests.integration.test_publications import dispatched as dispatched
from tests.integration.test_publications import freeze_creation as freeze_creation
from tests.integration.test_publications import judge as judge
from tests.integration.test_publications import passing_reviewable as passing_reviewable
from tests.integration.test_publications import passing_semantics as passing_semantics
from tests.integration.test_publications import persisted_assets as persisted_assets
from tests.integration.test_publications import ready as ready
from tests.integration.test_publications import reviewable as reviewable
from tests.integration.test_publications import setup_run as setup_run

pytestmark = pytest.mark.integration


async def test_approved_legacy_suite_is_readable_but_not_new_input_execution_authority(ready):
    tx, scope, command, at = ready
    store = MySQLPublications(tx)
    receipt = await store.apply(scope, command, at)
    publication = receipt.change.current.active
    assert publication is not None
    async with tx.open() as db:
        with pytest.raises(ConfigurationUnavailable, match="input construction"):
            await compile_configuration(db, publication)
    # Blocking generation does not destroy the original publication or audit.
    assert await store.get_receipt(scope, command.command_id) == receipt
