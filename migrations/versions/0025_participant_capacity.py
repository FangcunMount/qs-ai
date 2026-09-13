"""Participant daily reservations and durable active provider slots."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision = "0025_participant_capacity"
down_revision = "0024_evaluation_capacity"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "participant_admission_locks",
        sa.Column("organization_id", mysql.BIGINT(unsigned=True), primary_key=True),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )
    op.create_table(
        "participant_capacity_reservations",
        sa.Column("run_id", sa.String(36, collation="utf8mb4_bin"), primary_key=True),
        sa.Column("session_id", sa.String(36, collation="utf8mb4_bin"), nullable=False),
        sa.Column("organization_id", mysql.BIGINT(unsigned=True), nullable=False),
        sa.Column("subject_id", sa.String(128, collation="utf8mb4_bin"), nullable=False),
        sa.Column("assessment_ids", sa.JSON, nullable=False),
        sa.Column("budget_day", sa.Date, nullable=False),
        sa.Column("reserved_at", mysql.DATETIME(fsp=6), nullable=False),
        sa.Column("active", sa.Boolean, nullable=False),
        sa.Column("acquired_at", mysql.DATETIME(fsp=6)),
        sa.Column("released_at", mysql.DATETIME(fsp=6)),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )
    op.create_index(
        "ix_participant_capacity_day",
        "participant_capacity_reservations",
        ["organization_id", "budget_day"],
    )
    op.create_index(
        "ix_participant_capacity_active",
        "participant_capacity_reservations",
        ["organization_id", "active"],
    )


def downgrade() -> None:
    op.drop_table("participant_capacity_reservations")
    op.drop_table("participant_admission_locks")
