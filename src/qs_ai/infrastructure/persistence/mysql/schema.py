import sqlalchemy as sa
from sqlalchemy.dialects import mysql

from qs_ai.infrastructure.persistence.mysql.database import Base

metadata = Base.metadata
ID = sa.String(36, collation="utf8mb4_bin")
EXTERNAL_ID = mysql.BIGINT(unsigned=True)

sessions = sa.Table(
    "interpretation_sessions",
    metadata,
    sa.Column("id", ID, primary_key=True),
    sa.Column("request_id", ID, nullable=True),
    sa.Column("org_id", EXTERNAL_ID, nullable=False),
    sa.Column("owner_subject_id", sa.String(128, collation="utf8mb4_bin"), nullable=False),
    sa.Column("testee_id", EXTERNAL_ID, nullable=False),
    sa.Column("assessment_ids", sa.JSON, nullable=False),
    sa.Column("goal", sa.Text, nullable=False),
    sa.Column("status", sa.String(32), nullable=False),
    sa.Column("version", sa.Integer, nullable=False),
    sa.Column("active_run_id", ID),
    sa.Column("current_question_id", ID),
    sa.Column("evidence_set_id", ID),
    sa.Column("workflow_version", sa.String(64), nullable=False),
    sa.Column("failure_code", sa.String(64)),
    sa.Column("created_at", mysql.DATETIME(fsp=6), server_default=sa.text("CURRENT_TIMESTAMP(6)")),
    sa.Column("updated_at", mysql.DATETIME(fsp=6), server_default=sa.text("CURRENT_TIMESTAMP(6)")),
    sa.Index("ix_session_owner", "org_id", "owner_subject_id", "updated_at", "id"),
    sa.Column("created_at_utc", mysql.DATETIME(fsp=6)),
    sa.Column("updated_at_utc", mysql.DATETIME(fsp=6)),
    sa.UniqueConstraint("request_id"),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
questions = sa.Table(
    "interpretation_clarifications",
    metadata,
    sa.Column("id", ID, primary_key=True),
    sa.Column("session_id", ID, sa.ForeignKey(sessions.c.id), nullable=False),
    sa.Column("question_seq", sa.Integer, nullable=False),
    sa.Column("text", sa.Text, nullable=False),
    sa.Column("can_skip", sa.Boolean, nullable=False),
    sa.Column("answer", sa.Text),
    sa.Column("skipped", sa.Boolean, nullable=False),
    sa.Column("answered_by", sa.String(128)),
    sa.Column("answered_at", mysql.DATETIME(fsp=6)),
    sa.UniqueConstraint("session_id", "question_seq", name="uq_question_seq"),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
evidence_sets = sa.Table(
    "interpretation_evidence_sets",
    metadata,
    sa.Column("id", ID, primary_key=True),
    sa.Column("session_id", ID, sa.ForeignKey(sessions.c.id), nullable=False),
    sa.Column("fingerprint", sa.String(64), nullable=False),
    sa.Column("schema_version", sa.String(32), nullable=False),
    sa.Column("items", sa.JSON, nullable=False),
    sa.Column("frozen_at", mysql.DATETIME(fsp=6), server_default=sa.text("CURRENT_TIMESTAMP(6)")),
    sa.UniqueConstraint("session_id"),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
runs = sa.Table(
    "interpretation_runs",
    metadata,
    sa.Column("id", ID, primary_key=True),
    sa.Column("session_id", ID, sa.ForeignKey(sessions.c.id), nullable=False),
    sa.Column("session_version", sa.Integer, nullable=False),
    sa.Column("status", sa.String(32), nullable=False),
    sa.Column("checkpoint_ref", sa.String(128)),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
jobs = sa.Table(
    "execution_jobs",
    metadata,
    sa.Column("id", ID, primary_key=True),
    sa.Column("run_id", ID, sa.ForeignKey(runs.c.id), nullable=False),
    sa.Column("session_id", ID, sa.ForeignKey(sessions.c.id), nullable=False),
    sa.Column("status", sa.String(16), nullable=False),
    sa.Column("available_at", mysql.DATETIME(fsp=6), nullable=False),
    sa.Column("lease_until", mysql.DATETIME(fsp=6)),
    sa.Column("fence_token", mysql.BIGINT(unsigned=True), nullable=False),
    sa.Column("attempt", sa.Integer, nullable=False),
    sa.Column("answer", sa.Text),
    sa.Column("skipped", sa.Boolean, nullable=False),
    sa.Column("question_id", ID),
    sa.Index("ix_job_claim", "status", "available_at", "id"),
    sa.UniqueConstraint("run_id"),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
model_calls = sa.Table(
    "execution_model_calls",
    metadata,
    sa.Column("run_id", ID, sa.ForeignKey(runs.c.id), primary_key=True),
    sa.Column("invocation_id", ID, nullable=False),
    sa.Column("fence_token", mysql.BIGINT(unsigned=True), nullable=False),
    sa.Column("status", sa.String(32), nullable=False),
    sa.Column("request_json", mysql.LONGTEXT, nullable=False),
    sa.Column("response_json", mysql.LONGTEXT),
    sa.Column("failure_code", sa.String(64)),
    sa.Column("created_at", mysql.DATETIME(fsp=6), server_default=sa.text("CURRENT_TIMESTAMP(6)")),
    sa.Column("created_at_utc", mysql.DATETIME(fsp=6)),
    sa.UniqueConstraint("invocation_id"),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
artifacts = sa.Table(
    "interpretation_artifacts",
    metadata,
    sa.Column("id", ID, primary_key=True),
    sa.Column("session_id", ID, sa.ForeignKey(sessions.c.id), nullable=False),
    sa.Column("run_id", ID, sa.ForeignKey(runs.c.id), nullable=False),
    sa.Column("payload", sa.JSON, nullable=False),
    sa.Column("created_at", mysql.DATETIME(fsp=6), server_default=sa.text("CURRENT_TIMESTAMP(6)")),
    sa.UniqueConstraint("session_id"),
    sa.UniqueConstraint("run_id"),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
idempotency = sa.Table(
    "interpretation_idempotency_requests",
    metadata,
    sa.Column("scope_hash", sa.String(64, collation="utf8mb4_bin"), primary_key=True),
    sa.Column("key", sa.String(128, collation="utf8mb4_bin"), primary_key=True),
    sa.Column("request_hash", sa.String(64), nullable=False),
    sa.Column("response", sa.JSON),
    sa.Column("created_at", mysql.DATETIME(fsp=6), server_default=sa.text("CURRENT_TIMESTAMP(6)")),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
execution_leases = sa.Table(
    "execution_leases",
    metadata,
    sa.Column("thread_id", sa.String(191, collation="utf8mb4_bin"), primary_key=True),
    sa.Column("fence", mysql.BIGINT(unsigned=True), nullable=False),
    sa.Column("expires_at", mysql.DATETIME(fsp=6), nullable=False),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)

result_outbox = sa.Table(
    "interpretation_result_outbox",
    metadata,
    sa.Column("event_id", ID, primary_key=True),
    sa.Column("session_id", ID, sa.ForeignKey(sessions.c.id), nullable=False),
    sa.Column("version", sa.Integer, nullable=False),
    sa.Column("payload", sa.JSON, nullable=False),
    sa.Column("delivered", sa.Boolean, nullable=False, server_default=sa.text("0")),
    sa.Column("mq_owned", sa.Boolean, nullable=False, server_default=sa.text("0")),
    sa.Column("attempts", sa.Integer, nullable=False, server_default=sa.text("0")),
    sa.Column("created_at", mysql.DATETIME(fsp=6)),
    sa.Column("delivered_at", mysql.DATETIME(fsp=6)),
    sa.Column(
        "available_at",
        mysql.DATETIME(fsp=6),
        nullable=False,
        server_default=sa.text("CURRENT_TIMESTAMP(6)"),
    ),
    sa.UniqueConstraint("session_id", "version", name="uq_result_version"),
    sa.Index("ix_results_due", "delivered", "available_at", "event_id"),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)

asset_versions = sa.Table(
    "governance_asset_versions",
    metadata,
    sa.Column("asset_row_id", mysql.BIGINT(unsigned=True), primary_key=True, autoincrement=True),
    sa.Column("asset_kind", sa.String(32, collation="ascii_bin"), nullable=False),
    sa.Column("owner_organization_id", EXTERNAL_ID, nullable=False, server_default="0"),
    sa.Column("asset_id", sa.String(255, collation="utf8mb4_0900_bin"), nullable=False),
    sa.Column("version", sa.String(128, collation="utf8mb4_bin"), nullable=False),
    sa.Column("fingerprint", sa.String(71, collation="utf8mb4_bin"), nullable=False),
    sa.Column("body_format", sa.String(32, collation="ascii_bin"), nullable=False),
    sa.Column("body_bytes", mysql.LONGBLOB, nullable=False),
    sa.Column("package_sha256", sa.String(64, collation="utf8mb4_bin")),
    sa.Column("source_ref", sa.String(255, collation="utf8mb4_bin"), nullable=False),
    sa.Column("imported_by", sa.String(255, collation="utf8mb4_bin"), nullable=False),
    sa.Column("created_at", mysql.DATETIME(fsp=6), server_default=sa.text("CURRENT_TIMESTAMP(6)")),
    sa.Column(
        "native_id_key",
        sa.String(255, collation="utf8mb4_0900_bin"),
        sa.Computed(
            "CASE WHEN asset_kind IN ('profile','prompt','route','schema') THEN "
            "asset_id ELSE NULL END",
            persisted=True,
        ),
    ),
    sa.Column(
        "scoped_id_key",
        sa.String(128, collation="utf8mb4_bin"),
        sa.Computed(
            "CASE WHEN asset_kind IN "
            "('execution_policy','gate_policy','semantic_prompt') THEN asset_id "
            "ELSE NULL END",
            persisted=True,
        ),
    ),
    sa.Column(
        "profile_id_key",
        sa.String(255, collation="utf8mb4_0900_bin"),
        sa.Computed("CASE WHEN asset_kind='profile' THEN asset_id ELSE NULL END", persisted=True),
    ),
    sa.Column(
        "catalog_version_key",
        sa.String(128, collation="utf8mb4_0900_bin"),
        sa.Computed("version", persisted=True),
    ),
    sa.UniqueConstraint("asset_kind", "owner_organization_id", "native_id_key", "version"),
    sa.UniqueConstraint("asset_kind", "owner_organization_id", "scoped_id_key", "version"),
    sa.UniqueConstraint("profile_id_key", "version"),
    sa.CheckConstraint(
        "asset_kind IN ('profile','prompt','route','schema','execution_policy',"
        "'gate_policy','semantic_prompt')",
        name="ck_governance_asset_versions_kind",
    ),
    sa.CheckConstraint(
        "asset_kind='semantic_prompt' OR owner_organization_id=0",
        name="ck_governance_asset_versions_scope",
    ),
    sa.CheckConstraint(
        "asset_kind NOT IN ('execution_policy','gate_policy','semantic_prompt') "
        "OR CHAR_LENGTH(asset_id)<=128",
        name="ck_governance_asset_versions_identity",
    ),
    sa.CheckConstraint(
        "(asset_kind='prompt' AND body_format='prompt_package_json' AND "
        "package_sha256 IS NOT NULL) OR (asset_kind='semantic_prompt' AND "
        "body_format='semantic_markdown' AND package_sha256 IS NULL) OR "
        "(asset_kind IN ('profile','route','schema','execution_policy','gate_po"
        "licy') AND body_format='definition_json' AND package_sha256 IS NULL)",
        name="ck_governance_asset_versions_shape",
    ),
    sa.CheckConstraint(
        "asset_kind NOT IN ('execution_policy','gate_policy','semantic_prompt') "
        "OR created_at IS NOT NULL",
        name="ck_governance_asset_versions_time",
    ),
    sa.Index(
        "ix_governance_asset_versions_catalog",
        "asset_kind",
        "owner_organization_id",
        "asset_id",
        "catalog_version_key",
    ),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)


evaluation_checkpoints = sa.Table(
    "evaluation_checkpoints",
    metadata,
    sa.Column("run_id", sa.CHAR(36), primary_key=True),
    sa.Column("version", sa.BigInteger, nullable=False),
    sa.Column("checkpoint_json", mysql.JSON),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)

evaluation_dispatches = sa.Table(
    "evaluation_dispatches",
    metadata,
    sa.Column("run_id", sa.CHAR(36), primary_key=True),
    sa.Column("invocation_id", sa.String(128, collation="utf8mb4_bin"), primary_key=True),
    sa.Column("execution_id", sa.String(128, collation="utf8mb4_bin"), nullable=False),
    sa.UniqueConstraint("run_id", "execution_id", name="uq_evaluation_dispatch_execution"),
    sa.Column("kind", sa.String(16), nullable=False),
    sa.Column("case_id", sa.String(128, collation="utf8mb4_bin"), nullable=False),
    sa.Column("slot_ordinal", sa.Integer, nullable=False),
    sa.Column("candidate_id", sa.String(128, collation="utf8mb4_bin"), nullable=False),
    sa.Column("checkpoint_json", mysql.JSON, nullable=False),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)

evaluation_runs = sa.Table(
    "evaluation_runs",
    metadata,
    sa.Column("run_id", sa.CHAR(36), primary_key=True),
    sa.Column("execution_mode", sa.String(32), nullable=False, server_default="serial_v1"),
    sa.Column("organization_id", sa.BigInteger, nullable=False),
    sa.Column("requested_by", sa.String(128, collation="utf8mb4_bin"), nullable=False),
    sa.Column("definition_json", mysql.LONGTEXT, nullable=False),
    sa.Column("progress_json", mysql.JSON, nullable=True),
    sa.Column("frozen_execution_policy_fingerprint", sa.String(71), nullable=True),
    sa.Column("frozen_execution_policy_json", mysql.LONGTEXT, nullable=True),
    sa.CheckConstraint(
        "(frozen_execution_policy_fingerprint IS NULL AND "
        "frozen_execution_policy_json IS NULL) OR "
        "(frozen_execution_policy_fingerprint IS NOT NULL AND "
        "frozen_execution_policy_json IS NOT NULL)",
        name="ck_evaluation_runs_frozen_policy",
    ),
    sa.Index("ix_evaluation_runs_organization", "organization_id"),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)

evaluation_completions = sa.Table(
    "evaluation_completions",
    metadata,
    sa.Column("kind", sa.String(16, collation="ascii_bin"), primary_key=True),
    sa.Column("run_id", sa.CHAR(36), primary_key=True),
    sa.Column("execution_id", sa.String(128, collation="utf8mb4_bin"), primary_key=True),
    sa.Column("invocation_id", sa.String(128, collation="utf8mb4_bin"), nullable=False),
    sa.Column("case_id", sa.String(128, collation="utf8mb4_bin")),
    sa.Column("slot_ordinal", sa.Integer),
    sa.Column("execution_ordinal", sa.Integer, nullable=False),
    sa.Column("candidate_id", sa.String(128, collation="utf8mb4_bin")),
    sa.Column("candidate_json", mysql.JSON),
    sa.Column("evidence_json", mysql.JSON, nullable=False),
    sa.Column("result_json", mysql.JSON),
    sa.Column("raw_output", mysql.MEDIUMBLOB, nullable=False),
    sa.Column("normalized_output", mysql.MEDIUMBLOB, nullable=False),
    sa.Column(
        "gen_candidate_key",
        sa.String(128, collation="utf8mb4_bin"),
        sa.Computed("CASE WHEN kind='generation' THEN candidate_id ELSE NULL END", persisted=False),
    ),
    sa.Column(
        "gen_case_key",
        sa.String(128, collation="utf8mb4_bin"),
        sa.Computed("CASE WHEN kind='generation' THEN case_id ELSE NULL END", persisted=False),
    ),
    sa.Column(
        "gen_slot_key",
        sa.Integer,
        sa.Computed("CASE WHEN kind='generation' THEN slot_ordinal ELSE NULL END", persisted=False),
    ),
    sa.Column(
        "sem_candidate_key",
        sa.String(128, collation="utf8mb4_bin"),
        sa.Computed("CASE WHEN kind='semantic' THEN candidate_id ELSE NULL END", persisted=False),
    ),
    sa.Index(
        "ix_evaluation_completions_candidate", "kind", "run_id", "candidate_id", "execution_ordinal"
    ),
    sa.Index(
        "ix_evaluation_completions_slot",
        "kind",
        "run_id",
        "case_id",
        "slot_ordinal",
        "execution_ordinal",
    ),
    sa.UniqueConstraint("kind", "run_id", "invocation_id"),
    sa.UniqueConstraint("run_id", "gen_candidate_key"),
    sa.UniqueConstraint("run_id", "gen_case_key", "gen_slot_key", "execution_ordinal"),
    sa.UniqueConstraint("run_id", "sem_candidate_key", "execution_ordinal"),
    sa.CheckConstraint("kind IN ('generation','semantic')", name="ck_evaluation_completions_kind"),
    sa.CheckConstraint(
        "(kind='generation' AND case_id IS NOT NULL AND slot_ordinal IS NOT "
        "NULL AND result_json IS NULL) OR (kind='semantic' AND candidate_id IS "
        "NOT NULL AND case_id IS NULL AND slot_ordinal IS NULL AND "
        "candidate_json IS NULL)",
        name="ck_evaluation_completions_shape",
    ),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)


configuration_publications = sa.Table(
    "publication_records",
    metadata,
    sa.Column("publication_id", sa.CHAR(36), primary_key=True),
    sa.Column("selector_key", sa.CHAR(64), nullable=False),
    sa.Column("run_id", sa.CHAR(36), sa.ForeignKey("evaluation_runs.run_id"), nullable=False),
    sa.Column("run_version", sa.BigInteger, nullable=False),
    sa.Column("organization_id", sa.BigInteger, nullable=False),
    sa.Column("content_json", mysql.LONGTEXT, nullable=False),
    sa.Column("content_sha256", sa.CHAR(64), nullable=False),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)

configuration_publication_pointers = sa.Table(
    "publication_pointers",
    metadata,
    sa.Column("selector_key", sa.CHAR(64), primary_key=True),
    sa.Column("selector_json", mysql.TEXT, nullable=False),
    sa.Column("version", sa.BigInteger, nullable=False),
    sa.Column(
        "active_publication_id",
        sa.CHAR(36),
        sa.ForeignKey("publication_records.publication_id"),
    ),
    sa.Column("changed_at", sa.String(64)),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)

configuration_publication_changes = sa.Table(
    "publication_changes",
    metadata,
    sa.Column("command_id", sa.CHAR(36), primary_key=True),
    sa.Column(
        "selector_key",
        sa.CHAR(64),
        sa.ForeignKey("publication_pointers.selector_key"),
        nullable=False,
    ),
    sa.Column("version", sa.BigInteger, nullable=False),
    sa.Column("organization_id", sa.BigInteger, nullable=False),
    sa.Column("operator_user_id", sa.BigInteger, nullable=False),
    sa.Column("request_json", mysql.TEXT, nullable=False),
    sa.Column("receipt_json", mysql.TEXT, nullable=False),
    sa.UniqueConstraint("selector_key", "version", name="uq_publication_change_version"),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)

execution_configurations = sa.Table(
    "execution_configurations",
    metadata,
    sa.Column("session_id", ID, sa.ForeignKey(sessions.c.id), primary_key=True),
    sa.Column("evidence_set_id", ID, sa.ForeignKey(evidence_sets.c.id), nullable=False),
    sa.Column("evidence_fingerprint", sa.CHAR(64), nullable=False),
    sa.Column(
        "publication_id",
        sa.CHAR(36),
        sa.ForeignKey(configuration_publications.c.publication_id),
        nullable=False,
    ),
    sa.Column("publication_sha256", sa.CHAR(64), nullable=False),
    sa.Column("pointer_version", sa.BigInteger, nullable=False),
    sa.Column("selector_query", mysql.TEXT, nullable=False),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)

# Prompt UUID/command keys retain the legacy default collation. The maintenance
# preflight verifies that effective legacy collations match the rendered target DDL.
LEGACY_PROMPT_COLLATION = "utf8mb4_0900_ai_ci"
draft_heads = sa.Table(
    "governance_draft_heads",
    metadata,
    sa.Column("draft_row_id", mysql.BIGINT(unsigned=True), primary_key=True, autoincrement=True),
    sa.Column("draft_kind", sa.String(16, collation="ascii_bin"), nullable=False),
    sa.Column("organization_id", EXTERNAL_ID, nullable=False),
    sa.Column("draft_id", sa.CHAR(36, collation="utf8mb4_bin"), nullable=False),
    sa.Column("revision", sa.BigInteger, nullable=False),
    sa.Column(
        "prompt_draft_id_key",
        sa.CHAR(36, collation=LEGACY_PROMPT_COLLATION),
        sa.Computed("CASE WHEN draft_kind='prompt' THEN draft_id ELSE NULL END", persisted=True),
    ),
    sa.Column(
        "semantic_draft_id_key",
        sa.CHAR(36, collation="utf8mb4_bin"),
        sa.Computed("CASE WHEN draft_kind='semantic' THEN draft_id ELSE NULL END", persisted=True),
    ),
    sa.UniqueConstraint("prompt_draft_id_key"),
    sa.UniqueConstraint("organization_id", "semantic_draft_id_key"),
    sa.UniqueConstraint("draft_row_id", "draft_kind", "organization_id"),
    sa.CheckConstraint(
        "draft_kind IN ('prompt','semantic')", name="ck_governance_draft_heads_kind"
    ),
    sa.CheckConstraint("revision>=0", name="ck_governance_draft_heads_revision"),
    sa.CheckConstraint(
        "draft_kind <> 'prompt' OR organization_id <= 9223372036854775807",
        name="ck_governance_draft_heads_prompt_signed_scope",
    ),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
draft_versions = sa.Table(
    "governance_draft_versions",
    metadata,
    sa.Column("draft_row_id", mysql.BIGINT(unsigned=True), primary_key=True),
    sa.Column("revision", sa.BigInteger, primary_key=True),
    sa.Column("draft_kind", sa.String(16, collation="ascii_bin"), nullable=False),
    sa.Column("organization_id", EXTERNAL_ID, nullable=False),
    sa.Column("snapshot_bytes", mysql.LONGBLOB, nullable=False),
    sa.Column("snapshot_sha256", sa.CHAR(64), nullable=False),
    sa.Column("command_id", sa.CHAR(36, collation=LEGACY_PROMPT_COLLATION)),
    sa.Column("operator_user_id", EXTERNAL_ID),
    sa.Column("request_bytes", mysql.LONGBLOB),
    sa.Column(
        "prompt_command_id_key",
        sa.CHAR(36, collation=LEGACY_PROMPT_COLLATION),
        sa.Computed("CASE WHEN draft_kind='prompt' THEN command_id ELSE NULL END", persisted=True),
    ),
    sa.UniqueConstraint("prompt_command_id_key"),
    sa.ForeignKeyConstraint(
        ["draft_row_id", "draft_kind", "organization_id"],
        [
            "governance_draft_heads.draft_row_id",
            "governance_draft_heads.draft_kind",
            "governance_draft_heads.organization_id",
        ],
    ),
    sa.CheckConstraint(
        "draft_kind IN ('prompt','semantic')", name="ck_governance_draft_versions_kind"
    ),
    sa.CheckConstraint("revision>0", name="ck_governance_draft_versions_revision"),
    sa.CheckConstraint(
        "draft_kind <> 'prompt' OR (organization_id <= 9223372036854775807 "
        "AND operator_user_id <= 9223372036854775807)",
        name="ck_governance_draft_versions_prompt_signed_audit",
    ),
    sa.CheckConstraint(
        "(draft_kind='prompt' AND command_id IS NOT NULL AND operator_user_id "
        "IS NOT NULL AND request_bytes IS NOT NULL) OR (draft_kind='semantic' "
        "AND command_id IS NULL AND operator_user_id IS NULL AND request_bytes "
        "IS NULL)",
        name="ck_governance_draft_versions_audit",
    ),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)


prompt_draft_freezes = sa.Table(
    "governance_prompt_draft_freezes",
    metadata,
    sa.Column("command_id", sa.CHAR(36), primary_key=True),
    sa.Column(
        "draft_id",
        sa.CHAR(36, collation=LEGACY_PROMPT_COLLATION),
        sa.ForeignKey(draft_heads.c.prompt_draft_id_key),
        nullable=False,
    ),
    sa.Column("organization_id", sa.BigInteger, nullable=False),
    sa.Column("operator_user_id", sa.BigInteger, nullable=False),
    sa.Column("receipt_json", mysql.LONGTEXT, nullable=False),
    sa.Column("receipt_sha256", sa.CHAR(64), nullable=False),
    sa.UniqueConstraint("draft_id", name="uq_prompt_draft_freeze"),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)

profile_registrations = sa.Table(
    "governance_profile_registrations",
    metadata,
    sa.Column("command_id", sa.CHAR(36), primary_key=True),
    sa.Column("organization_id", sa.BigInteger, nullable=False),
    sa.Column("operator_user_id", sa.BigInteger, nullable=False),
    sa.Column("profile_id", sa.String(255, collation="utf8mb4_0900_bin"), nullable=False),
    sa.Column("profile_version", sa.String(128, collation="utf8mb4_bin"), nullable=False),
    sa.Column("receipt_json", mysql.LONGTEXT, nullable=False),
    sa.Column("receipt_sha256", sa.CHAR(64), nullable=False),
    sa.UniqueConstraint("profile_id", "profile_version", name="uq_profile_registration"),
    sa.ForeignKeyConstraint(
        ["profile_id", "profile_version"],
        [asset_versions.c.profile_id_key, asset_versions.c.version],
    ),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)


evaluation_suites = sa.Table(
    "evaluation_suites",
    metadata,
    sa.Column("suite_id", sa.String(128, collation="utf8mb4_bin"), primary_key=True),
    sa.Column("suite_version", sa.String(128, collation="utf8mb4_bin"), primary_key=True),
    sa.Column("fingerprint", sa.String(71), nullable=False),
    sa.Column("definition_json", mysql.LONGTEXT, nullable=False),
    sa.Column("command_id", sa.CHAR(36), nullable=True),
    sa.Column("organization_id", sa.BigInteger, nullable=False),
    sa.Column("operator_user_id", sa.BigInteger, nullable=True),
    sa.Column("receipt_json", mysql.LONGTEXT, nullable=True),
    sa.Column("receipt_sha256", sa.CHAR(64), nullable=True),
    sa.Column("source_ref", sa.String(255)),
    sa.Column("imported_by", sa.String(128)),
    sa.Column("contracts_json", mysql.LONGTEXT),
    sa.Column("contracts_sha256", sa.CHAR(64)),
    sa.UniqueConstraint("command_id"),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)

# Organization locks serialize admission across processes and UTC-day boundaries.
evaluation_admission_locks = sa.Table(
    "quota_evaluation_admission_locks",
    metadata,
    sa.Column("organization_id", EXTERNAL_ID, primary_key=True),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
evaluation_capacity_reservations = sa.Table(
    "quota_evaluation_capacity_reservations",
    metadata,
    sa.Column("quota_snapshot", sa.JSON),
    sa.Column("run_id", ID, primary_key=True),
    sa.Column("organization_id", EXTERNAL_ID, nullable=False),
    sa.Column("budget_day", sa.Date, nullable=False),
    sa.Column("provider_calls", sa.Integer, nullable=False),
    sa.Column("daily_limit", sa.Integer, nullable=False),
    sa.Column("requested_by", sa.String(128), nullable=False),
    sa.Column("reserved_at", mysql.DATETIME(fsp=6), nullable=False),
    sa.Index("ix_evaluation_capacity_day", "organization_id", "budget_day"),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)

participant_admission_locks = sa.Table(
    "quota_participant_admission_locks",
    metadata,
    sa.Column("organization_id", EXTERNAL_ID, primary_key=True),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
participant_capacity_reservations = sa.Table(
    "quota_participant_capacity_reservations",
    metadata,
    sa.Column("quota_snapshot", sa.JSON),
    sa.Column("run_id", ID, primary_key=True),
    sa.Column("session_id", ID, nullable=False),
    sa.Column("organization_id", EXTERNAL_ID, nullable=False),
    sa.Column("subject_id", sa.String(128, collation="utf8mb4_bin"), nullable=False),
    sa.Column("assessment_ids", sa.JSON, nullable=False),
    sa.Column("budget_day", sa.Date, nullable=False),
    sa.Column("reserved_at", mysql.DATETIME(fsp=6), nullable=False),
    sa.Column("active", sa.Boolean, nullable=False),
    sa.Column("acquired_at", mysql.DATETIME(fsp=6)),
    sa.Column("released_at", mysql.DATETIME(fsp=6)),
    sa.Index("ix_participant_capacity_day", "organization_id", "budget_day"),
    sa.Index("ix_participant_capacity_active", "organization_id", "active"),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)

participant_retries = sa.Table(
    "execution_participant_retries",
    metadata,
    sa.Column("organization_id", EXTERNAL_ID, primary_key=True),
    sa.Column("command_id", ID, primary_key=True),
    sa.Column("session_id", ID, nullable=False),
    sa.Column("request_id", ID, nullable=False),
    sa.Column("source_run_id", ID, nullable=False),
    sa.Column("run_id", ID, nullable=False),
    sa.Column("operator_user_id", EXTERNAL_ID, nullable=False),
    sa.Column("expected_version", sa.Integer, nullable=False),
    sa.Column("reason", sa.Text, nullable=False),
    sa.Column("accepted_unknown_risk", sa.Boolean, nullable=False),
    sa.Column("source_failure_code", sa.String(64)),
    sa.Column("frozen_request_json", mysql.LONGTEXT),
    sa.Column("receipt", sa.JSON, nullable=False),
    sa.Column("created_at", mysql.DATETIME(fsp=6), server_default=sa.text("CURRENT_TIMESTAMP(6)")),
    sa.Index("ix_participant_retry_session", "session_id"),
    sa.UniqueConstraint("source_run_id"),
    sa.UniqueConstraint("run_id"),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)

# Editing state references existing immutable assets; it never duplicates Run approval state.
solution_revisions = sa.Table(
    "governance_solutions",
    metadata,
    sa.Column("solution_id", ID, primary_key=True),
    sa.Column("organization_id", EXTERNAL_ID, nullable=False),
    sa.Column("revision", sa.BigInteger, nullable=False),
    sa.Column(
        "draft_id",
        sa.CHAR(36, collation=LEGACY_PROMPT_COLLATION),
        sa.ForeignKey("governance_draft_heads.prompt_draft_id_key"),
        nullable=False,
    ),
    sa.Column("state_json", mysql.LONGTEXT, nullable=False),
    sa.Column("state_sha256", sa.String(64), nullable=False),
    sa.Index("ix_solution_org", "organization_id", "solution_id"),
    sa.UniqueConstraint("draft_id"),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
solution_commands = sa.Table(
    "governance_solution_commands",
    metadata,
    sa.Column("command_id", ID, primary_key=True),
    sa.Column("solution_id", ID, sa.ForeignKey("governance_solutions.solution_id"), nullable=False),
    sa.Column("organization_id", EXTERNAL_ID, nullable=False),
    sa.Column("operator_user_id", EXTERNAL_ID, nullable=False),
    sa.Column("request_json", mysql.LONGTEXT, nullable=False),
    sa.Column("receipt_json", mysql.LONGTEXT, nullable=False),
    sa.Column("receipt_sha256", sa.String(64), nullable=False),
    sa.Index("ix_solution_commands", "solution_id"),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)

organization_quota_versions = sa.Table(
    "quota_organization_versions",
    metadata,
    sa.Column("organization_id", EXTERNAL_ID, primary_key=True),
    sa.Column("revision", sa.BigInteger, primary_key=True),
    sa.Column("definition_json", mysql.LONGTEXT, nullable=False),
    sa.Column("definition_sha256", sa.String(64), nullable=False),
    sa.Column("operator_user_id", EXTERNAL_ID, nullable=False),
    sa.Column("reason", sa.Text, nullable=False),
    sa.Column("created_at", mysql.DATETIME(fsp=6), nullable=False),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
organization_quota_pointers = sa.Table(
    "quota_organization_pointers",
    metadata,
    sa.Column("organization_id", EXTERNAL_ID, primary_key=True),
    sa.Column("revision", sa.BigInteger, nullable=False),
    sa.ForeignKeyConstraint(
        ["organization_id", "revision"],
        ["quota_organization_versions.organization_id", "quota_organization_versions.revision"],
    ),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
organization_quota_commands = sa.Table(
    "quota_organization_commands",
    metadata,
    sa.Column("organization_id", EXTERNAL_ID, primary_key=True),
    sa.Column("command_id", sa.String(36, collation="utf8mb4_bin"), primary_key=True),
    sa.Column("operator_user_id", EXTERNAL_ID, nullable=False),
    sa.Column("request_json", mysql.LONGTEXT, nullable=False),
    sa.Column("receipt_json", mysql.LONGTEXT, nullable=False),
    sa.Column("receipt_sha256", sa.String(64), nullable=False),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)


runtime_milestones = sa.Table(
    "operations_runtime_milestones",
    metadata,
    sa.Column("session_id", ID, sa.ForeignKey(sessions.c.id, ondelete="CASCADE"), primary_key=True),
    sa.Column("dedupe_key", sa.String(128, collation="utf8mb4_bin"), primary_key=True),
    sa.Column("run_id", ID, nullable=False),
    sa.Column("kind", sa.String(32), nullable=False),
    sa.Column("invocation_id", ID),
    sa.Column("attempt", sa.Integer),
    sa.Column("occurred_at", mysql.DATETIME(fsp=6), nullable=False),
    sa.Column("expires_at", mysql.DATETIME(fsp=6), nullable=False),
    sa.Index("ix_runtime_milestones_expiry", "expires_at"),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)

semantic_draft_commands = sa.Table(
    "governance_semantic_draft_commands",
    metadata,
    sa.Column("organization_id", EXTERNAL_ID, primary_key=True),
    sa.Column("command_id", sa.CHAR(36, collation="utf8mb4_bin"), primary_key=True),
    sa.Column("operator_user_id", EXTERNAL_ID, nullable=False),
    sa.Column("draft_id", sa.CHAR(36, collation="utf8mb4_bin"), nullable=False),
    sa.Column("request_json", mysql.LONGTEXT, nullable=False),
    sa.Column("receipt_json", mysql.LONGTEXT, nullable=False),
    sa.Column("receipt_sha256", sa.CHAR(64), nullable=False),
    sa.ForeignKeyConstraint(
        ["organization_id", "draft_id"],
        ["governance_draft_heads.organization_id", "governance_draft_heads.semantic_draft_id_key"],
    ),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)


# Run coordination remains in evaluation_checkpoints; ownership is per frozen slot.
evaluation_slot_claims = sa.Table(
    "evaluation_slot_claims",
    metadata,
    sa.Column("run_id", sa.CHAR(36), sa.ForeignKey("evaluation_runs.run_id"), primary_key=True),
    sa.Column("case_id", sa.String(128, collation="utf8mb4_bin"), primary_key=True),
    sa.Column("slot_ordinal", sa.Integer, primary_key=True),
    sa.Column("version", sa.BigInteger, nullable=False),
    sa.Column("checkpoint_json", mysql.JSON, nullable=False),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)

evaluation_response_receipts = sa.Table(
    "evaluation_response_receipts",
    metadata,
    sa.Column("run_id", sa.CHAR(36), sa.ForeignKey("evaluation_runs.run_id"), primary_key=True),
    sa.Column("invocation_id", sa.String(128, collation="utf8mb4_bin"), primary_key=True),
    sa.Column("execution_id", sa.String(128, collation="utf8mb4_bin"), nullable=False),
    sa.Column("claim_version", sa.BigInteger, nullable=False),
    sa.Column("definition_json", mysql.LONGTEXT, nullable=False),
    sa.Column("sha256", sa.CHAR(64), nullable=False),
    sa.UniqueConstraint("run_id", "execution_id", name="uq_evaluation_response_execution"),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)


# Every runtime-owned constraint has a stable physical-table-qualified name.
# MySQL names are limited to 64 characters; shorten deterministically, not by dialect.
def _constraint_name(prefix: str, table_name: str, columns: list[str]) -> str:
    import hashlib

    name = "_".join([prefix, table_name, *columns])
    return (
        name if len(name) <= 64 else name[:55] + "_" + hashlib.sha256(name.encode()).hexdigest()[:8]
    )


for _table in metadata.tables.values():
    # Explicit names replace InnoDB's implicit FK support indexes without adding
    # another access path. PK, unique or ordinary left prefixes already suffice.
    _covered = [list(index.columns.keys()) for index in _table.indexes]
    _covered += [
        list(constraint.columns.keys())
        for constraint in _table.constraints
        if isinstance(constraint, (sa.PrimaryKeyConstraint, sa.UniqueConstraint))
    ]
    for _foreign_key in sorted(
        _table.foreign_key_constraints,
        key=lambda constraint: tuple(constraint.columns.keys()),
    ):
        _columns = list(_foreign_key.columns.keys())
        if not any(existing[: len(_columns)] == _columns for existing in _covered):
            sa.Index(_constraint_name("idx", _table.name, _columns), *_foreign_key.columns)
            _covered.append(_columns)
    for _index in _table.indexes:
        _index.name = sa.sql.elements.quoted_name(
            _constraint_name("idx", _table.name, [column.name for column in _index.columns]),
            None,
        )
    for _constraint in _table.constraints:
        if isinstance(_constraint, sa.PrimaryKeyConstraint):
            _constraint.name = _constraint_name("pk", _table.name, [])
        elif isinstance(_constraint, sa.UniqueConstraint):
            _constraint.name = _constraint_name(
                "uk", _table.name, [column.name for column in _constraint.columns]
            )
        elif isinstance(_constraint, sa.ForeignKeyConstraint):
            _constraint.name = _constraint_name(
                "fk", _table.name, [column.name for column in _constraint.columns]
            )
