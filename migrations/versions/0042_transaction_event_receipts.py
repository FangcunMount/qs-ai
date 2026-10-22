"""Keep authenticated business ACKs independently of retired publish Outbox rows."""

from alembic import op

revision = "0042_transaction_event_receipts"
down_revision = "0041_transaction_messaging"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(DDL)


def downgrade() -> None:
    raise RuntimeError("Preserve business receipts; use a compatible forward repair")


DDL = """CREATE TABLE messaging_event_receipts (
 producer VARBINARY(128) NOT NULL,
 ack_id VARBINARY(128) NOT NULL,
 event_id VARBINARY(128) NOT NULL,
 organization_id BIGINT UNSIGNED NOT NULL,
 header MEDIUMBLOB NOT NULL,
 body MEDIUMBLOB NOT NULL,
 outcome INT NOT NULL,
 recorded_at DATETIME(6) NOT NULL,
 PRIMARY KEY(producer,ack_id),
 INDEX messaging_event_receipts_event(event_id,outcome)
) ENGINE=InnoDB"""
