"""Immutable native evaluation suite bindings and original registration receipts."""

from alembic import op

revision = "0022_evaluation_suites"
down_revision = "0021_profile_registrations"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""CREATE TABLE evaluation_suites (
        suite_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
        suite_version VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
        fingerprint VARCHAR(71) NOT NULL,
        definition_json LONGTEXT NOT NULL,
        command_id CHAR(36) NOT NULL UNIQUE,
        organization_id BIGINT NOT NULL,
        operator_user_id BIGINT NOT NULL,
        receipt_json LONGTEXT NOT NULL,
        receipt_sha256 CHAR(64) NOT NULL,
        PRIMARY KEY (suite_id, suite_version)
    ) ENGINE=InnoDB CHARSET=utf8mb4""")


def downgrade() -> None:
    op.drop_table("evaluation_suites")
