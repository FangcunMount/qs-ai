"""Versioned organization quota configuration; existing reservations remain historical."""

from alembic import op

revision = "0030_organization_quotas"
down_revision = "0029_solution_revisions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""CREATE TABLE organization_quota_versions (
        organization_id BIGINT UNSIGNED NOT NULL,
        revision BIGINT NOT NULL,
        definition_json LONGTEXT NOT NULL,
        definition_sha256 VARCHAR(64) NOT NULL,
        operator_user_id BIGINT UNSIGNED NOT NULL,
        reason TEXT NOT NULL,
        created_at DATETIME(6) NOT NULL,
        PRIMARY KEY (organization_id, revision)
    ) ENGINE=InnoDB CHARSET=utf8mb4""")
    op.execute("""CREATE TABLE organization_quota_pointers (
        organization_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
        revision BIGINT NOT NULL,
        FOREIGN KEY (organization_id, revision)
          REFERENCES organization_quota_versions(organization_id, revision)
    ) ENGINE=InnoDB CHARSET=utf8mb4""")
    op.execute("""CREATE TABLE organization_quota_commands (
        organization_id BIGINT UNSIGNED NOT NULL,
        command_id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL,
        operator_user_id BIGINT UNSIGNED NOT NULL,
        request_json LONGTEXT NOT NULL,
        receipt_json LONGTEXT NOT NULL,
        receipt_sha256 VARCHAR(64) NOT NULL,
        PRIMARY KEY (organization_id, command_id)
    ) ENGINE=InnoDB CHARSET=utf8mb4""")
    for table in ("participant_capacity_reservations", "evaluation_capacity_reservations"):
        op.execute(f"ALTER TABLE {table} ADD quota_snapshot JSON NULL")


def downgrade() -> None:
    # Downgrade would destroy audit evidence and is intentionally not automatic.
    raise RuntimeError("Quota history requires an explicit preservation and recovery procedure")
