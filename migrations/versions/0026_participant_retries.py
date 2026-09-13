"""Preserve explicit participant retry lineage and the original model request."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision = "0026_participant_retries"
down_revision = "0025_participant_capacity"
branch_labels = None
depends_on = None


def upgrade() -> None:
    id_type = sa.String(36, collation="utf8mb4_bin")
    op.create_table(
        "participant_retries",
        sa.Column("organization_id", mysql.BIGINT(unsigned=True), primary_key=True),
        sa.Column("command_id", id_type, primary_key=True),
        sa.Column("session_id", id_type, nullable=False),
        sa.Column("request_id", id_type, nullable=False),
        sa.Column("source_run_id", id_type, nullable=False, unique=True),
        sa.Column("run_id", id_type, nullable=False, unique=True),
        sa.Column("operator_user_id", mysql.BIGINT(unsigned=True), nullable=False),
        sa.Column("expected_version", sa.Integer, nullable=False),
        sa.Column("reason", sa.Text, nullable=False),
        sa.Column("accepted_unknown_risk", sa.Boolean, nullable=False),
        sa.Column("source_failure_code", sa.String(64)),
        sa.Column("frozen_request_json", mysql.LONGTEXT),
        sa.Column("receipt", sa.JSON, nullable=False),
        sa.Column(
            "created_at", mysql.DATETIME(fsp=6), server_default=sa.text("CURRENT_TIMESTAMP(6)")
        ),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )
    op.create_index("ix_participant_retry_session", "participant_retries", ["session_id"])


def downgrade() -> None:
    op.drop_table("participant_retries")
