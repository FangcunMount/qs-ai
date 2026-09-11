"""Persist judge evidence independently of immutable generated output."""

from alembic import op

revision = "0016_semantic_completions"
down_revision = "0015_generation_completions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""CREATE TABLE evaluation_semantic_completions (
        run_id CHAR(36) NOT NULL,
        execution_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
        invocation_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
        candidate_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
        execution_ordinal INT NOT NULL,
        evidence_json JSON NOT NULL,
        result_json JSON NULL,
        raw_output MEDIUMBLOB NOT NULL,
        normalized_output MEDIUMBLOB NOT NULL,
        PRIMARY KEY (run_id, execution_id),
        CONSTRAINT uq_semantic_invocation UNIQUE (run_id, invocation_id),
        CONSTRAINT uq_semantic_ordinal UNIQUE (run_id, candidate_id, execution_ordinal)
    ) ENGINE=InnoDB CHARSET=utf8mb4""")


def downgrade() -> None:
    op.drop_table("evaluation_semantic_completions")
