"""Retain scoped Profile registration commands beside immutable configuration assets."""

from alembic import op

revision = "0021_profile_registrations"
down_revision = "0020_prompt_freezes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""CREATE TABLE profile_registrations (
        command_id CHAR(36) NOT NULL PRIMARY KEY,
        organization_id BIGINT NOT NULL,
        operator_user_id BIGINT NOT NULL,
        profile_id VARCHAR(255) COLLATE utf8mb4_0900_bin NOT NULL,
        profile_version VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
        receipt_json LONGTEXT NOT NULL,
        receipt_sha256 CHAR(64) NOT NULL,
        CONSTRAINT uq_profile_registration UNIQUE (profile_id, profile_version),
        FOREIGN KEY (profile_id, profile_version) REFERENCES profile_assets(profile_id, version)
    ) ENGINE=InnoDB CHARSET=utf8mb4""")


def downgrade() -> None:
    op.drop_table("profile_registrations")
