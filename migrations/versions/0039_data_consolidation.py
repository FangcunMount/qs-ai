"""Consolidate five groups in isolated schemas using frozen conversion contracts."""

from alembic import op

from qs_ai.maintenance.schema_refactor.migrations import consolidate

revision = "0039_data_consolidation"
down_revision = "0038_messaging_observations"
branch_labels = None
depends_on = None


def upgrade() -> None:
    consolidate(op.get_bind())


def downgrade() -> None:
    raise RuntimeError("Use full current-data reverse conversion in schema_refactor")
