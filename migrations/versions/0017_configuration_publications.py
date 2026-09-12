"""Retain publications and atomically version global selector pointers and commands."""

from alembic import op

revision = "0017_configuration_publications"
down_revision = "0016_semantic_completions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""CREATE TABLE configuration_publications (
        publication_id CHAR(36) NOT NULL PRIMARY KEY,
        selector_key CHAR(64) NOT NULL,
        run_id CHAR(36) NOT NULL,
        run_version BIGINT NOT NULL,
        organization_id BIGINT NOT NULL,
        content_json LONGTEXT NOT NULL,
        content_sha256 CHAR(64) NOT NULL,
        FOREIGN KEY (run_id) REFERENCES evaluation_runs(run_id)
    ) ENGINE=InnoDB CHARSET=utf8mb4""")
    op.execute("""CREATE TABLE configuration_publication_pointers (
        selector_key CHAR(64) NOT NULL PRIMARY KEY,
        selector_json TEXT NOT NULL,
        version BIGINT NOT NULL,
        active_publication_id CHAR(36) NULL,
        changed_at VARCHAR(64) NULL,
        FOREIGN KEY (active_publication_id) REFERENCES configuration_publications(publication_id)
    ) ENGINE=InnoDB CHARSET=utf8mb4""")
    op.execute("""CREATE TABLE configuration_publication_changes (
        command_id CHAR(36) NOT NULL PRIMARY KEY,
        selector_key CHAR(64) NOT NULL,
        version BIGINT NOT NULL,
        organization_id BIGINT NOT NULL,
        operator_user_id BIGINT NOT NULL,
        request_json TEXT NOT NULL,
        receipt_json TEXT NOT NULL,
        CONSTRAINT uq_publication_change_version UNIQUE (selector_key, version),
        FOREIGN KEY (selector_key) REFERENCES configuration_publication_pointers(selector_key)
    ) ENGINE=InnoDB CHARSET=utf8mb4""")


def downgrade() -> None:
    op.drop_table("configuration_publication_changes")
    op.drop_table("configuration_publication_pointers")
    op.drop_table("configuration_publications")
