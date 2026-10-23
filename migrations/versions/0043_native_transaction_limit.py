"""Install host-owned native-limit protection; never a message/witness table."""

from alembic import op

revision = "0043_native_transaction_limit"
down_revision = "0042_transaction_event_receipts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(DDL)


def downgrade() -> None:
    raise RuntimeError("Preserve native protection/review evidence; use an approved forward repair")


DDL = """-- Host-owned deny-only governance. Not an Outbox or a Broker confirmation.
CREATE TABLE messaging_native_limit_notices (
 notice_key BINARY(32) NOT NULL PRIMARY KEY,
 owner VARBINARY(128) NOT NULL,
 store_id VARBINARY(128) NOT NULL,
 writer_epoch VARBINARY(128) NOT NULL,
 route_revision VARBINARY(128) NOT NULL,
 native_index_sha256 BINARY(32) NOT NULL,
 evidence_sha256 BINARY(32) NOT NULL,
 status VARBINARY(32) NOT NULL,
 schema_version INT NOT NULL DEFAULT 1,
 recorded_at DATETIME(6) NOT NULL,
 resolved_at DATETIME(6) NULL,
 resolution_receipt_sha256 BINARY(32) NULL,
 KEY messaging_native_limit_owner (owner,status,notice_key),
 CONSTRAINT chk_native_limit_version CHECK (schema_version=1),
 CONSTRAINT chk_native_limit_state CHECK (status IN ('blocked','resolved')),
 CONSTRAINT chk_native_limit_resolution CHECK (
  (status='blocked' AND resolved_at IS NULL AND resolution_receipt_sha256 IS NULL)
  OR (status='resolved' AND resolved_at IS NOT NULL AND resolution_receipt_sha256 IS NOT NULL
      AND resolution_receipt_sha256<>UNHEX(REPEAT('00',32)))
 )
) ENGINE=InnoDB"""
