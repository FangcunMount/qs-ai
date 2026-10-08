"""Finish module-qualified physical names; 0039 is not a runnable image contract."""

from alembic import op

from qs_ai.maintenance.schema_refactor.migrations import module_names

revision = "0040_module_table_names"
down_revision = "0039_data_consolidation"
branch_labels = None
depends_on = None


def upgrade() -> None:
    module_names(op.get_bind())


def downgrade() -> None:
    raise RuntimeError("Use full current-data reverse conversion in schema_refactor")
