"""Use original dispatch/completion evidence; diagnostic reads never retry a model."""

import hashlib
import json
from dataclasses import replace
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.diagnostics import (
    ExecutionOutput,
    ExecutionPage,
    ExecutionQuery,
    ExecutionSummary,
)
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import decode
from qs_ai.infrastructure.persistence.mysql.evaluation_creation_receipt import creation_receipt
from qs_ai.infrastructure.persistence.mysql.evaluation_projection import (
    decode_completion,
    decode_semantic_completion,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_snapshot import header
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_dispatches as dispatches
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_generation_completions as gen
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_semantic_completions as sem


async def _header(db: AsyncSession, query: ExecutionQuery) -> Any:
    run = await header(db, query.scope)
    if run["version"] != query.expected_version:
        raise CheckpointConflict("Execution view version changed")
    creation_receipt(dict(run))
    return run


async def _completions(db: AsyncSession, run_id: str, ids: list[str]) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for kind, table in (("generation", gen), ("semantic", sem)):
        # Selecting lengths keeps all raw/normalized bodies out of the list query.
        statement = select(
            table.c.execution_id,
            table.c.evidence_json,
            func.length(table.c.raw_output).label("raw_size"),
            func.length(table.c.normalized_output).label("normalized_size"),
        ).where(table.c.run_id == run_id, table.c.execution_id.in_(ids))
        for row in (await db.execute(statement)).mappings():
            if row["execution_id"] in values:
                raise CheckpointConflict("Execution has conflicting completion kinds")
            values[row["execution_id"]] = {**row, "kind": kind}
    return values


def _summary(dispatch: Any, completion: Any, run: Any) -> ExecutionSummary:
    cp = decode(dispatch["checkpoint_json"])
    if cp is None or any(
        dispatch[key] != getattr(cp, key)
        for key in (
            "execution_id",
            "invocation_id",
            "kind",
            "case_id",
            "slot_ordinal",
            "candidate_id",
        )
    ):
        raise CheckpointConflict("Execution index differs from original dispatch")
    creation = json.loads(run["definition_json"])
    if not any(
        s["case_id"] == cp.case_id and s["ordinal"] == cp.slot_ordinal for s in creation["slots"]
    ):
        raise CheckpointConflict("Execution outside frozen slots")
    if completion is None:
        current = decode(run["checkpoint_json"])
        if (
            current is None
            or current.execution_id != cp.execution_id
            or current.invocation_id != cp.invocation_id
        ):
            raise CheckpointConflict("Missing terminal execution evidence")
        evidence = {
            "status": current.phase,
            "dispatch_started_at": current.dispatch_started_at.isoformat()
            if current.dispatch_started_at
            else None,
        }
        status, raw_size, normalized_size = current.phase, 0, 0
    else:
        evidence = completion["evidence_json"]
        if (
            completion["kind"] != cp.kind
            or any(
                evidence[key] != getattr(cp, key)
                for key in ("execution_id", "invocation_id", "execution_ordinal")
            )
            or evidence["status"] not in ("succeeded", "failed", "result_unknown")
        ):
            raise CheckpointConflict("Completion differs from dispatch")
        status, raw_size, normalized_size = (
            evidence["status"],
            completion["raw_size"],
            completion["normalized_size"],
        )
    raw = json.dumps(evidence, ensure_ascii=False, separators=(",", ":"))
    if (
        len(raw.encode()) > 16384
        or not 0 <= raw_size <= 256 * 1024
        or not 0 <= normalized_size <= 256 * 1024
    ):
        raise CheckpointConflict("Execution evidence exceeds read bound")
    return ExecutionSummary(
        cp.execution_id,
        cp.invocation_id,
        cp.kind,
        cp.case_id,
        cp.slot_ordinal,
        cp.execution_ordinal,
        status,
        raw_size,
        normalized_size,
        raw,
    )


class MySQLEvaluationDiagnostics:
    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    async def list(self, query: ExecutionQuery) -> ExecutionPage:
        run_id = str(query.scope.run_id)
        async with self.transactions.open() as db:
            run = await _header(db, query)
            records = list(
                (
                    await db.execute(
                        select(dispatches)
                        .where(
                            dispatches.c.run_id == run_id,
                            dispatches.c.execution_id > query.cursor,
                        )
                        .order_by(dispatches.c.execution_id)
                        .limit(query.limit + 1)
                    )
                ).mappings()
            )
            page = records[: query.limit]
            completions = (
                await _completions(db, run_id, [r["execution_id"] for r in page]) if page else {}
            )
            items = tuple(_summary(r, completions.get(r["execution_id"]), run) for r in page)
            return ExecutionPage(
                run_id,
                query.expected_version,
                items,
                items[-1].execution_id if len(records) > query.limit else "",
            )

    async def get(self, query: ExecutionQuery, execution_id: str) -> ExecutionOutput:
        if not execution_id:
            raise ValueError("Execution identity required")
        replace(query, cursor=execution_id)
        run_id = str(query.scope.run_id)
        async with self.transactions.open() as db:
            run = await _header(db, query)
            dispatch = (
                (
                    await db.execute(
                        select(dispatches).where(
                            dispatches.c.run_id == run_id,
                            dispatches.c.execution_id == execution_id,
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            if dispatch is None:
                raise NotFound("Execution unavailable in evaluation")
            completions = await _completions(db, run_id, [execution_id])
            summary = _summary(dispatch, completions.get(execution_id), run)
            raw, normalized = b"", b""
            if execution_id in completions:
                table = gen if summary.kind == "generation" else sem
                row = (
                    (
                        await db.execute(
                            select(table).where(
                                table.c.run_id == run_id,
                                table.c.execution_id == execution_id,
                            )
                        )
                    )
                    .mappings()
                    .one()
                )
                cp = decode(dispatch["checkpoint_json"])
                if cp is None:
                    raise CheckpointConflict("Original dispatch is missing")
                if summary.kind == "generation":
                    generated = decode_completion(row)
                    matches = generated.matches_checkpoint(cp, cp.owner)
                    raw, normalized = generated.raw_output, generated.normalized_output
                else:
                    judged = decode_semantic_completion(row)
                    candidate = (
                        (
                            await db.execute(
                                select(gen).where(
                                    gen.c.run_id == run_id,
                                    gen.c.candidate_id == cp.candidate_id,
                                )
                            )
                        )
                        .mappings()
                        .one_or_none()
                    )
                    if candidate is None:
                        raise CheckpointConflict("Semantic source candidate is missing")
                    generated = decode_completion(candidate)
                    matches = judged.matches_checkpoint(
                        cp, cp.owner, generated.normalized_fingerprint
                    )
                    raw, normalized = judged.raw_output, judged.normalized_output
                if not matches:
                    raise CheckpointConflict("Output does not match original dispatch")
            return ExecutionOutput(
                run_id,
                query.expected_version,
                summary,
                raw,
                normalized,
                hashlib.sha256(raw).hexdigest(),
                hashlib.sha256(normalized).hexdigest(),
            )
