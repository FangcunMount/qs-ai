"""Terminal generation bytes and candidate evidence accepted with Run progress."""

from alembic import op

revision = "0015_generation_completions"
down_revision = "0014_evaluation_progress"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""CREATE TABLE evaluation_generation_completions (
        run_id CHAR(36) NOT NULL,
        execution_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
        invocation_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
        case_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
        slot_ordinal INT NOT NULL,
        execution_ordinal INT NOT NULL,
        candidate_id VARCHAR(128) COLLATE utf8mb4_bin NULL,
        candidate_json JSON NULL,
        evidence_json JSON NOT NULL,
        raw_output MEDIUMBLOB NOT NULL,
        normalized_output MEDIUMBLOB NOT NULL,
        PRIMARY KEY (run_id, execution_id),
        CONSTRAINT uq_evaluation_completion_invocation UNIQUE (run_id, invocation_id),
        CONSTRAINT uq_evaluation_completion_candidate UNIQUE (run_id, candidate_id),
        CONSTRAINT uq_evaluation_completion_ordinal
            UNIQUE (run_id, case_id, slot_ordinal, execution_ordinal)
    ) ENGINE=InnoDB CHARSET=utf8mb4""")


def downgrade() -> None:
    op.drop_table("evaluation_generation_completions")
