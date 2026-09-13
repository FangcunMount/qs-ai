"""Durable daily evaluation reservations and cross-process admission lock."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision = "0024_evaluation_capacity"
down_revision = "0023_evaluation_catalog_index"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "evaluation_admission_locks",
        sa.Column("organization_id", mysql.BIGINT(unsigned=True), primary_key=True),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )
    op.create_table(
        "evaluation_capacity_reservations",
        sa.Column("run_id", sa.String(36, collation="utf8mb4_bin"), primary_key=True),
        sa.Column("organization_id", mysql.BIGINT(unsigned=True), nullable=False),
        sa.Column("budget_day", sa.Date, nullable=False),
        sa.Column("provider_calls", sa.Integer, nullable=False),
        sa.Column("daily_limit", sa.Integer, nullable=False),
        sa.Column("requested_by", sa.String(128), nullable=False),
        sa.Column("reserved_at", mysql.DATETIME(fsp=6), nullable=False),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )
    op.create_index(
        "ix_evaluation_capacity_day",
        "evaluation_capacity_reservations",
        ["organization_id", "budget_day"],
    )


def downgrade() -> None:
    op.drop_table("evaluation_capacity_reservations")
    op.drop_table("evaluation_admission_locks")
