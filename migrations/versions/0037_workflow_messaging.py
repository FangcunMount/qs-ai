"""Add host MQ storage and an explicit handoff flag; preserve all existing evidence."""

import sqlalchemy as sa
from alembic import op

revision = "0037_workflow_messaging"
down_revision = "0036_evaluation_slot_claims"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for statement in DDL:
        op.execute(statement)
    op.add_column(
        "result_outbox",
        sa.Column("mq_owned", sa.Boolean(), nullable=False, server_default=sa.text("0")),
    )


def downgrade() -> None:
    raise RuntimeError("Retain MQ evidence and ownership; rollback requires a compatible image")


DDL = (
    """CREATE TABLE ai_messaging_outbox (
	producer VARCHAR(64) COLLATE ascii_bin NOT NULL,
	destination VARCHAR(64) COLLATE ascii_bin NOT NULL,
	message_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
	body_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
	body MEDIUMBLOB NOT NULL,
	wire MEDIUMBLOB NOT NULL,
	wire_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
	topic VARCHAR(64) COLLATE ascii_bin NOT NULL,
	kind INTEGER NOT NULL,
	organization_id BIGINT UNSIGNED NOT NULL,
	aggregate_key VARCHAR(192) COLLATE utf8mb4_bin NOT NULL,
	aggregate_sequence BIGINT UNSIGNED NOT NULL,
	ordered BOOL NOT NULL,
	requires_receipt BOOL NOT NULL,
	stage VARCHAR(32) COLLATE ascii_bin NOT NULL,
	attempts BIGINT UNSIGNED NOT NULL DEFAULT '0',
	available_at DATETIME(6) NOT NULL,
	created_at DATETIME(6) NOT NULL,
	published_at DATETIME(6),
	confirmed_at DATETIME(6),
	error_code VARCHAR(128) COLLATE ascii_bin NOT NULL DEFAULT '',
	PRIMARY KEY (producer, destination, message_id)
)ENGINE=InnoDB CHARSET=utf8mb4""",
    """CREATE INDEX ix_ai_messaging_order ON ai_messaging_outbox
        (producer, destination, aggregate_key, ordered, aggregate_sequence, stage)""",
    """CREATE INDEX ix_ai_messaging_pending ON ai_messaging_outbox
        (stage, available_at, message_id)""",
    """CREATE TABLE ai_messaging_inbox (
	producer VARCHAR(64) COLLATE ascii_bin NOT NULL,
	message_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
	destination VARCHAR(64) COLLATE ascii_bin NOT NULL,
	body_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
	body MEDIUMBLOB NOT NULL,
	wire_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
	kind INTEGER NOT NULL,
	aggregate_key VARCHAR(192) COLLATE utf8mb4_bin NOT NULL,
	reservation_token VARCHAR(36) NOT NULL,
	decision VARCHAR(32) NOT NULL,
	receipt_id VARCHAR(128) COLLATE utf8mb4_bin,
	received_at DATETIME(6) NOT NULL,
	PRIMARY KEY (producer, message_id)
)ENGINE=InnoDB CHARSET=utf8mb4""",
    """CREATE TABLE ai_messaging_quarantine (
	wire_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
	wire MEDIUMBLOB NOT NULL,
	code VARCHAR(128) COLLATE ascii_bin NOT NULL,
	logical_producer VARCHAR(64) COLLATE ascii_bin,
	logical_message_id VARCHAR(128) COLLATE utf8mb4_bin,
	logical_body_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin,
	attempts BIGINT UNSIGNED NOT NULL,
	first_seen_at DATETIME(6) NOT NULL,
	last_seen_at DATETIME(6) NOT NULL,
	PRIMARY KEY (wire_sha256),
	CONSTRAINT uq_ai_messaging_failure_identity UNIQUE (logical_producer, logical_message_id)
)ENGINE=InnoDB CHARSET=utf8mb4""",
    """CREATE TABLE ai_messaging_evaluation_sequences (
	run_id CHAR(36) NOT NULL,
	sequence BIGINT NOT NULL,
	version BIGINT NOT NULL,
	PRIMARY KEY (run_id)
)ENGINE=InnoDB CHARSET=utf8mb4""",
)
