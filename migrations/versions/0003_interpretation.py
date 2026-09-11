"""P1 interpretation persistence. Frozen MySQL DDL, no runtime model imports."""

from alembic import op

revision = "0003_interpretation"
down_revision = "0002_checkpoint_leases"
branch_labels = None
depends_on = None

DDL = (
    """
CREATE TABLE idempotency_requests (
	scope_hash VARCHAR(64) COLLATE utf8mb4_bin NOT NULL,
	`key` VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
	request_hash VARCHAR(64) NOT NULL,
	response JSON,
	created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
	PRIMARY KEY (scope_hash, `key`)
)ENGINE=InnoDB CHARSET=utf8mb4
""",
    """
CREATE TABLE interpretation_sessions (
	id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL,
	org_id BIGINT UNSIGNED NOT NULL,
	owner_subject_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
	testee_id BIGINT UNSIGNED NOT NULL,
	assessment_ids JSON NOT NULL,
	goal TEXT NOT NULL,
	status VARCHAR(32) NOT NULL,
	version INTEGER NOT NULL,
	active_run_id VARCHAR(36) COLLATE utf8mb4_bin,
	current_question_id VARCHAR(36) COLLATE utf8mb4_bin,
	evidence_set_id VARCHAR(36) COLLATE utf8mb4_bin,
	workflow_version VARCHAR(64) NOT NULL,
	failure_code VARCHAR(64),
	created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
	updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
	PRIMARY KEY (id)
)ENGINE=InnoDB CHARSET=utf8mb4
""",
    """CREATE INDEX ix_session_owner ON interpretation_sessions
    (org_id, owner_subject_id, updated_at, id)
""",
    """
CREATE TABLE clarifications (
	id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL,
	session_id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL,
	question_seq INTEGER NOT NULL,
	text TEXT NOT NULL,
	can_skip BOOL NOT NULL,
	answer TEXT,
	skipped BOOL NOT NULL,
	answered_by VARCHAR(128),
	answered_at DATETIME(6),
	PRIMARY KEY (id),
	CONSTRAINT uq_question_seq UNIQUE (session_id, question_seq),
	FOREIGN KEY(session_id) REFERENCES interpretation_sessions (id)
)ENGINE=InnoDB CHARSET=utf8mb4
""",
    """
CREATE TABLE evidence_sets (
	id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL,
	session_id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL,
	fingerprint VARCHAR(64) NOT NULL,
	schema_version VARCHAR(32) NOT NULL,
	items JSON NOT NULL,
	frozen_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
	PRIMARY KEY (id),
	UNIQUE (session_id),
	FOREIGN KEY(session_id) REFERENCES interpretation_sessions (id)
)ENGINE=InnoDB CHARSET=utf8mb4
""",
    """
CREATE TABLE interpretation_runs (
	id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL,
	session_id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL,
	session_version INTEGER NOT NULL,
	status VARCHAR(32) NOT NULL,
	checkpoint_ref VARCHAR(128),
	PRIMARY KEY (id),
	FOREIGN KEY(session_id) REFERENCES interpretation_sessions (id)
)ENGINE=InnoDB CHARSET=utf8mb4
""",
    """
CREATE TABLE execution_jobs (
	id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL,
	run_id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL,
	session_id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL,
	status VARCHAR(16) NOT NULL,
	available_at DATETIME(6) NOT NULL,
	lease_until DATETIME(6),
	fence_token BIGINT UNSIGNED NOT NULL,
	attempt INTEGER NOT NULL,
	answer TEXT,
	skipped BOOL NOT NULL,
	question_id VARCHAR(36) COLLATE utf8mb4_bin,
	PRIMARY KEY (id),
	UNIQUE (run_id),
	FOREIGN KEY(run_id) REFERENCES interpretation_runs (id),
	FOREIGN KEY(session_id) REFERENCES interpretation_sessions (id)
)ENGINE=InnoDB CHARSET=utf8mb4
""",
    """CREATE INDEX ix_job_claim ON execution_jobs (status, available_at, id)
""",
)


def upgrade() -> None:
    for statement in DDL:
        op.execute(statement)


def downgrade() -> None:
    op.drop_table("execution_jobs")
    op.drop_table("interpretation_runs")
    op.drop_table("evidence_sets")
    op.drop_table("clarifications")
    op.drop_table("interpretation_sessions")
    op.drop_table("idempotency_requests")
