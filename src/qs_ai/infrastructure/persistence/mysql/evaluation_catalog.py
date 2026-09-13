"""Bounded summary projection; raw model output and frozen suite bodies are never selected."""

import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import ColumnElement, and_, case, cast, func, or_, select
from sqlalchemy.dialects.mysql import DATETIME

from qs_ai.application.evaluation.catalog import (
    STATUSES,
    EvaluationCatalogQuery,
    EvaluationPage,
    EvaluationSummary,
)
from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_creation_receipt import creation_receipt
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints, evaluation_runs
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_generation_completions as generations,
)


def creation_projection() -> ColumnElement[Any]:
    document = evaluation_runs.c.definition_json
    return func.json_object(
        *[
            part
            for key in ("schema_version", "run_id", "release", "release_fingerprint", "audit")
            for part in (key, func.json_extract(document, "$." + key))
        ]
    )


def creation_time() -> ColumnElement[datetime]:
    raw = func.json_unquote(
        func.json_extract(evaluation_runs.c.definition_json, "$.audit.created_at")
    )
    zulu = func.right(raw, 1) == "Z"
    # Python's persisted ISO timestamps include a numeric offset. Convert fixed
    # offsets explicitly, independent of the MySQL session time zone or tz tables.
    local = func.left(raw, func.length(raw) - case((zulu, 1), else_=6))
    zone = case((zulu, "+00:00"), else_=func.right(raw, 6))
    return func.convert_tz(cast(local, DATETIME(fsp=6)), zone, "+00:00")


class MySQLEvaluationCatalog:
    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    async def list(self, query: EvaluationCatalogQuery) -> EvaluationPage:
        created = creation_time()
        progress = evaluation_runs.c.progress_json
        status = progress["status"].as_string()
        candidate_count = (
            select(func.count())
            .where(
                generations.c.run_id == evaluation_runs.c.run_id,
                generations.c.candidate_id.is_not(None),
            )
            .correlate(evaluation_runs)
        )
        ready_count = candidate_count.where(
            generations.c.candidate_json["review_ready"].as_boolean().is_(True)
        )
        statement = (
            select(
                evaluation_runs.c.run_id,
                evaluation_runs.c.organization_id,
                evaluation_runs.c.requested_by,
                evaluation_checkpoints.c.version,
                creation_projection().label("definition_json"),
                created.label("created_at_utc"),
                status.label("status"),
                func.json_length(
                    func.json_extract(evaluation_runs.c.definition_json, "$.slots")
                ).label("required_candidates"),
                candidate_count.scalar_subquery().label("accepted_candidates"),
                ready_count.scalar_subquery().label("review_ready_candidates"),
                func.json_unquote(
                    func.json_extract(progress, "$.transitions[last].cause_code")
                ).label("last_cause"),
                func.json_unquote(func.json_extract(progress, "$.transitions[last].reason")).label(
                    "last_reason"
                ),
                func.coalesce(progress["unresolved_result_unknown_count"].as_integer(), 0).label(
                    "unknown_count"
                ),
                func.coalesce(
                    func.json_length(func.json_extract(progress, "$.human_reviews")), 0
                ).label("review_count"),
            )
            .outerjoin(
                evaluation_checkpoints, evaluation_runs.c.run_id == evaluation_checkpoints.c.run_id
            )
            .where(evaluation_runs.c.organization_id == query.organization_id)
        )
        if query.status:
            statement = statement.where(status == query.status)
        after = query.after()
        if after is not None:
            at, run_id = after
            statement = statement.where(
                or_(
                    created < at,
                    and_(created == at, evaluation_runs.c.run_id < run_id),
                    created.is_(None),
                )
            )
        statement = statement.order_by(created.desc(), evaluation_runs.c.run_id.desc()).limit(
            query.limit + 1
        )
        async with self.transactions.open() as db:
            rows = (await db.execute(statement)).mappings().all()
        items = []
        for row in rows[: query.limit]:
            if (
                row["organization_id"] != query.organization_id
                or str(UUID(row["run_id"])) != row["run_id"]
                or UUID(row["run_id"]).int == 0
                or row["version"] is None
                or row["version"] < 1
                or row["status"] not in STATUSES
                or (query.status and row["status"] != query.status)
                or row["unknown_count"] < 0
                or row["review_count"] < 0
                or row["required_candidates"] is None
                or not 0
                <= row["review_ready_candidates"]
                <= row["accepted_candidates"]
                <= row["required_candidates"]
            ):
                raise CheckpointConflict("Evaluation catalog requires reconciliation")
            receipt = json.loads(creation_receipt(dict(row)))
            created_at = datetime.fromisoformat(receipt["created_at"]).astimezone(UTC)
            if created_at.replace(tzinfo=None) != row["created_at_utc"]:
                raise CheckpointConflict("Evaluation creation time requires reconciliation")
            release = receipt["release"]
            items.append(
                EvaluationSummary(
                    row["run_id"],
                    row["organization_id"],
                    row["version"],
                    row["status"],
                    created_at.isoformat(),
                    row["requested_by"],
                    release["profile"]["id"],
                    release["profile"]["version"],
                    release["prompt"]["id"],
                    release["prompt"]["version"],
                    receipt["release_fingerprint"],
                    row["unknown_count"],
                    row["review_count"],
                    row["required_candidates"],
                    row["accepted_candidates"],
                    row["review_ready_candidates"],
                    row["last_cause"] or "",
                    row["last_reason"] or "",
                )
            )
        next_cursor = (
            query.next_cursor(items[-1].created_at, items[-1].run_id)
            if len(rows) > query.limit
            else ""
        )
        return EvaluationPage(tuple(items), next_cursor)
