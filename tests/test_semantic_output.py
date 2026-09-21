import hashlib
import json
from dataclasses import replace

import pytest

from qs_ai.domain.evaluation.preflight import AssertionReceipt
from qs_ai.infrastructure.qs_server.semantic_assets import load_semantic_assets
from qs_ai.infrastructure.qs_server.semantic_output import parse_semantic_output
from tests.test_generation_completion_assets import assets


def context():
    release, value, routes, _ = assets()
    semantic = load_semantic_assets()
    release = replace(
        release,
        semantic_route=release.generation_route,
        semantic_prompt=semantic.prompt,
        semantic_output_schema=semantic.output_schema,
    )
    obligation = AssertionReceipt(
        "faithful", "case", 1, True, "semantic", "pending_semantic", "pending"
    )
    output = {
        "schema_version": "ai-explanation-semantic-evaluation-output/v1",
        "scores": {
            name: 4
            for name in (
                "faithfulness",
                "cross_dimension_quality",
                "suggestion_actionability",
                "audience_clarity",
                "concision",
            )
        },
        "rationale": "结合所给事实评估。",
        "decisions": [
            {
                "type": "faithful",
                "scope": "case",
                "ordinal": 1,
                "status": "failed",
                "detail": "存在未支持的判断。",
            }
        ],
    }
    return release, value, routes, (obligation,), output


async def parse(context, raw=None):
    release, value, routes, obligations, output = context
    return await parse_semantic_output(
        json.dumps(output, ensure_ascii=False).encode() if raw is None else raw,
        release,
        routes,
        value.receipt,
        value.invocation_id,
        obligations,
        assets=load_semantic_assets(),
    )


@pytest.mark.asyncio
async def test_semantic_failure_decision_is_valid_result_not_execution_failure():
    values = context()
    raw = json.dumps(values[-1], ensure_ascii=False, indent=2).encode()
    result = await parse(values, raw)
    assert result.output_fingerprint == "sha256:" + hashlib.sha256(raw).hexdigest()
    assert result.evaluator_version == "v2"
    assert result.decisions[0].status == "failed"
    assert result.decisions[0].hard is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", ["missing", "unknown", "duplicate", "scope", "ordinal", "score", "blank", "html"]
)
async def test_schema_and_exact_obligation_inventory_are_enforced(damage):
    values = context()
    output = values[-1]
    if damage == "missing":
        output["decisions"] = []
    elif damage == "duplicate":
        output["decisions"] *= 2
    elif damage == "unknown":
        output["decisions"][0]["type"] = "other"
    elif damage == "scope":
        output["decisions"][0]["scope"] = "default"
    elif damage == "ordinal":
        output["decisions"][0]["ordinal"] = 2
    elif damage == "score":
        output["scores"]["concision"] = 6
    else:
        output["rationale"] = "  " if damage == "blank" else "<private>"
    with pytest.raises(ValueError) as exc:
        await parse(values)
    assert "<private>" not in str(exc.value)


@pytest.mark.asyncio
async def test_receipt_and_release_cannot_be_substituted():
    release, value, routes, obligations, output = context()
    for field in ("invocation_id", "provider", "model"):
        other = (
            replace(value, receipt=replace(value.receipt, **{field: "other"}))
            if field != "invocation_id"
            else replace(
                value, invocation_id="other", receipt=replace(value.receipt, invocation_id="other")
            )
        )
        # Keep the expected invocation original; provider and route must also match.
        with pytest.raises(ValueError):
            await parse_semantic_output(
                json.dumps(output).encode(),
                release,
                routes,
                other.receipt,
                value.invocation_id,
                obligations,
                assets=load_semantic_assets(),
            )
    with pytest.raises(ValueError):
        await parse(
            (
                replace(release, semantic_prompt=replace(release.semantic_prompt, version="v1")),
                value,
                routes,
                obligations,
                output,
            )
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", [b"NaN", b"\xff", b"", b"x" * (256 * 1024 + 1)])
async def test_invalid_or_oversized_bytes_are_rejected(raw):
    with pytest.raises(ValueError):
        await parse(context(), raw)


@pytest.mark.asyncio
async def test_same_count_cannot_hide_duplicate_decisions_or_obligations():
    release, value, routes, obligations, output = context()
    second = replace(obligations[0], type="clarity", hard=False)
    output["decisions"] *= 2
    with pytest.raises(ValueError, match="duplicate semantic decision"):
        await parse((release, value, routes, (*obligations, second), output))
    with pytest.raises(ValueError, match="Duplicate semantic obligation"):
        await parse((release, value, routes, obligations * 2, output))


@pytest.mark.asyncio
@pytest.mark.parametrize("field,limit", [("rationale", 4000), ("detail", 2000)])
async def test_chinese_text_obeys_original_go_byte_limit(field, limit):
    values = context()
    target = values[-1] if field == "rationale" else values[-1]["decisions"][0]
    target[field] = "中" * (limit // 3 + 1)
    with pytest.raises(ValueError, match="rationale"):
        await parse(values)
