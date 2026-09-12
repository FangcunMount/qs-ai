"""Bind accepted QS snapshot tasks to immutable configuration publications."""

from alembic import op

revision = "0018_execution_configurations"
down_revision = "0017_configuration_publications"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""CREATE TABLE execution_configurations (
        session_id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
        evidence_set_id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL,
        evidence_fingerprint CHAR(64) NOT NULL,
        publication_id CHAR(36) NOT NULL,
        publication_sha256 CHAR(64) NOT NULL,
        pointer_version BIGINT NOT NULL,
        selector_query TEXT NOT NULL,
        FOREIGN KEY (session_id) REFERENCES interpretation_sessions(id),
        FOREIGN KEY (evidence_set_id) REFERENCES evidence_sets(id),
        FOREIGN KEY (publication_id) REFERENCES configuration_publications(publication_id)
    ) ENGINE=InnoDB CHARSET=utf8mb4""")


def downgrade() -> None:
    op.drop_table("execution_configurations")
