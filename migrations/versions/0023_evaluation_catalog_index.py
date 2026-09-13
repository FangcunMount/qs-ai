"""Scope evaluation catalog scans before extracting chronological summary keys."""

from alembic import op

revision = "0023_evaluation_catalog_index"
down_revision = "0022_evaluation_suites"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("ix_evaluation_runs_organization", "evaluation_runs", ["organization_id"])


def downgrade() -> None:
    op.drop_index("ix_evaluation_runs_organization", table_name="evaluation_runs")
