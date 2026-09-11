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
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
questions = sa.Table(
    "clarifications",
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
    "evidence_sets",
    metadata,
    sa.Column("id", ID, primary_key=True),
    sa.Column("session_id", ID, sa.ForeignKey(sessions.c.id), nullable=False, unique=True),
    sa.Column("fingerprint", sa.String(64), nullable=False),
    sa.Column("schema_version", sa.String(32), nullable=False),
    sa.Column("items", sa.JSON, nullable=False),
    sa.Column("frozen_at", mysql.DATETIME(fsp=6), server_default=sa.text("CURRENT_TIMESTAMP(6)")),
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
    sa.Column("run_id", ID, sa.ForeignKey(runs.c.id), nullable=False, unique=True),
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
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
model_calls = sa.Table(
    "model_calls",
    metadata,
    sa.Column("run_id", ID, sa.ForeignKey(runs.c.id), primary_key=True),
    sa.Column("invocation_id", ID, nullable=False, unique=True),
    sa.Column("fence_token", mysql.BIGINT(unsigned=True), nullable=False),
    sa.Column("status", sa.String(32), nullable=False),
    sa.Column("request_json", mysql.LONGTEXT, nullable=False),
    sa.Column("response_json", mysql.LONGTEXT),
    sa.Column("failure_code", sa.String(64)),
    sa.Column("created_at", mysql.DATETIME(fsp=6), server_default=sa.text("CURRENT_TIMESTAMP(6)")),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
artifacts = sa.Table(
    "interpretation_artifacts",
    metadata,
    sa.Column("id", ID, primary_key=True),
    sa.Column("session_id", ID, sa.ForeignKey(sessions.c.id), nullable=False, unique=True),
    sa.Column("run_id", ID, sa.ForeignKey(runs.c.id), nullable=False, unique=True),
    sa.Column("payload", sa.JSON, nullable=False),
    sa.Column("created_at", mysql.DATETIME(fsp=6), server_default=sa.text("CURRENT_TIMESTAMP(6)")),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
idempotency = sa.Table(
    "idempotency_requests",
    metadata,
    sa.Column("scope_hash", sa.String(64, collation="utf8mb4_bin"), primary_key=True),
    sa.Column("key", sa.String(128, collation="utf8mb4_bin"), primary_key=True),
    sa.Column("request_hash", sa.String(64), nullable=False),
    sa.Column("response", sa.JSON),
    sa.Column("created_at", mysql.DATETIME(fsp=6), server_default=sa.text("CURRENT_TIMESTAMP(6)")),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
leases = sa.Table(
    "checkpoint_leases",
    metadata,
    sa.Column("thread_id", sa.String(191, collation="utf8mb4_bin"), primary_key=True),
    sa.Column("fence", mysql.BIGINT(unsigned=True), nullable=False),
    sa.Column("expires_at", mysql.DATETIME(fsp=6), nullable=False),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)

external_requests = sa.Table(
    "external_requests",
    metadata,
    sa.Column("request_id", ID, primary_key=True),
    sa.Column("session_id", ID, sa.ForeignKey(sessions.c.id), nullable=False, unique=True),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
result_outbox = sa.Table(
    "result_outbox",
    metadata,
    sa.Column("event_id", ID, primary_key=True),
    sa.Column("session_id", ID, sa.ForeignKey(sessions.c.id), nullable=False),
    sa.Column("version", sa.Integer, nullable=False),
    sa.Column("payload", sa.JSON, nullable=False),
    sa.Column("delivered", sa.Boolean, nullable=False, server_default=sa.text("0")),
    sa.Column("attempts", sa.Integer, nullable=False, server_default=sa.text("0")),
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

profile_assets = sa.Table(
    "profile_assets",
    metadata,
    sa.Column("profile_id", sa.String(255, collation="utf8mb4_0900_bin"), primary_key=True),
    sa.Column("version", sa.String(128, collation="utf8mb4_bin"), primary_key=True),
    sa.Column("fingerprint", sa.String(71, collation="utf8mb4_bin"), nullable=False),
    sa.Column("definition_json", mysql.LONGTEXT(collation="utf8mb4_bin"), nullable=False),
    sa.Column("source_ref", sa.String(255, collation="utf8mb4_bin"), nullable=False),
    sa.Column("imported_by", sa.String(255, collation="utf8mb4_bin"), nullable=False),
    sa.Column("created_at", mysql.DATETIME(fsp=6), server_default=sa.text("CURRENT_TIMESTAMP(6)")),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)

prompt_assets = sa.Table(
    "prompt_assets",
    metadata,
    sa.Column("template_id", sa.String(255, collation="utf8mb4_0900_bin"), primary_key=True),
    sa.Column("version", sa.String(128, collation="utf8mb4_bin"), primary_key=True),
    sa.Column("fingerprint", sa.String(71, collation="utf8mb4_bin"), nullable=False),
    sa.Column("package_sha256", sa.String(64, collation="utf8mb4_bin"), nullable=False),
    sa.Column("package_json", mysql.LONGTEXT(collation="utf8mb4_bin"), nullable=False),
    sa.Column("source_ref", sa.String(255, collation="utf8mb4_bin"), nullable=False),
    sa.Column("imported_by", sa.String(255, collation="utf8mb4_bin"), nullable=False),
    sa.Column("created_at", mysql.DATETIME(fsp=6), server_default=sa.text("CURRENT_TIMESTAMP(6)")),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)

route_assets = sa.Table(
    "route_assets",
    metadata,
    sa.Column("route", sa.String(255, collation="utf8mb4_0900_bin"), primary_key=True),
    sa.Column("revision", sa.String(128, collation="utf8mb4_bin"), primary_key=True),
    sa.Column("fingerprint", sa.String(71, collation="utf8mb4_bin"), nullable=False),
    sa.Column("definition_json", mysql.LONGTEXT(collation="utf8mb4_bin"), nullable=False),
    sa.Column("source_ref", sa.String(255, collation="utf8mb4_bin"), nullable=False),
    sa.Column("imported_by", sa.String(255, collation="utf8mb4_bin"), nullable=False),
    sa.Column("created_at", mysql.DATETIME(fsp=6), server_default=sa.text("CURRENT_TIMESTAMP(6)")),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)

schema_assets = sa.Table(
    "schema_assets",
    metadata,
    sa.Column("schema_id", sa.String(255, collation="utf8mb4_0900_bin"), primary_key=True),
    sa.Column("version", sa.String(128, collation="utf8mb4_bin"), primary_key=True),
    sa.Column("fingerprint", sa.String(71, collation="utf8mb4_bin"), nullable=False),
    sa.Column("definition_json", mysql.LONGTEXT(collation="utf8mb4_bin"), nullable=False),
    sa.Column("source_ref", sa.String(255, collation="utf8mb4_bin"), nullable=False),
    sa.Column("imported_by", sa.String(255, collation="utf8mb4_bin"), nullable=False),
    sa.Column("created_at", mysql.DATETIME(fsp=6), server_default=sa.text("CURRENT_TIMESTAMP(6)")),
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

evaluation_run_policies = sa.Table(
    "evaluation_run_policies",
    metadata,
    sa.Column("run_id", sa.CHAR(36), primary_key=True),
    sa.Column("fingerprint", sa.String(71), nullable=False),
    sa.Column("definition_json", mysql.LONGTEXT, nullable=False),
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
    sa.Column("organization_id", sa.BigInteger, nullable=False),
    sa.Column("requested_by", sa.String(128, collation="utf8mb4_bin"), nullable=False),
    sa.Column("definition_json", mysql.LONGTEXT, nullable=False),
    sa.Column("progress_json", mysql.JSON, nullable=True),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)

evaluation_generation_completions = sa.Table(
    "evaluation_generation_completions",
    metadata,
    sa.Column("run_id", sa.CHAR(36), primary_key=True),
    sa.Column("execution_id", sa.String(128, collation="utf8mb4_bin"), primary_key=True),
    sa.Column("invocation_id", sa.String(128, collation="utf8mb4_bin"), nullable=False),
    sa.Column("case_id", sa.String(128, collation="utf8mb4_bin"), nullable=False),
    sa.Column("slot_ordinal", sa.Integer, nullable=False),
    sa.Column("execution_ordinal", sa.Integer, nullable=False),
    sa.Column("candidate_id", sa.String(128, collation="utf8mb4_bin")),
    sa.Column("candidate_json", mysql.JSON),
    sa.Column("evidence_json", mysql.JSON, nullable=False),
    sa.Column("raw_output", mysql.MEDIUMBLOB, nullable=False),
    sa.Column("normalized_output", mysql.MEDIUMBLOB, nullable=False),
    sa.UniqueConstraint("run_id", "invocation_id", name="uq_evaluation_completion_invocation"),
    sa.UniqueConstraint("run_id", "candidate_id", name="uq_evaluation_completion_candidate"),
    sa.UniqueConstraint(
        "run_id",
        "case_id",
        "slot_ordinal",
        "execution_ordinal",
        name="uq_evaluation_completion_ordinal",
    ),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
