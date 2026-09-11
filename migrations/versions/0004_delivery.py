"""External request correlation and transactional result delivery."""

from alembic import op

revision = "0004_delivery"
down_revision = "0003_interpretation"
branch_labels = None
depends_on = None
DDL = (
    """CREATE TABLE external_requests (
	request_id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL,
	session_id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL,
	PRIMARY KEY (request_id),
	UNIQUE (session_id),
	FOREIGN KEY(session_id) REFERENCES interpretation_sessions (id)
)ENGINE=InnoDB CHARSET=utf8mb4
""",
    """CREATE TABLE result_outbox (
	event_id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL,
	session_id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL,
	version INTEGER NOT NULL,
	payload JSON NOT NULL,
	delivered BOOL NOT NULL DEFAULT 0,
	attempts INTEGER NOT NULL DEFAULT 0,
	available_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
	PRIMARY KEY (event_id),
	CONSTRAINT uq_result_version UNIQUE (session_id, version),
	FOREIGN KEY(session_id) REFERENCES interpretation_sessions (id)
)ENGINE=InnoDB CHARSET=utf8mb4
""",
    """CREATE INDEX ix_results_due ON result_outbox (delivered, available_at, event_id)
""",
)


def upgrade() -> None:
    for statement in DDL:
        op.execute(statement)


def downgrade() -> None:
    op.drop_table("result_outbox")
    op.drop_table("external_requests")
