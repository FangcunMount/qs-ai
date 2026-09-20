"""Versioned presentation of the existing snapshot executor, not a new workflow DSL."""

from dataclasses import asdict
from typing import Any

from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity

SCHEMA = "qs-ai-flow/v1"
DEFINITION = "qs-published-snapshot-v1"


def describe(
    release: EvidenceReleaseIdentity,
    *,
    content: dict[str, Any],
    models: dict[str, Any],
    editable: bool,
    semantic_prompt: str | None,
    plan: dict[str, Any] | None,
) -> dict[str, Any]:
    """Refs are verified by the reader; layout/text never contribute to release hashes."""
    refs = {key: asdict(value) for key, value in vars(release).items()}
    nodes: list[dict[str, Any]] = []

    def node(
        identity: str,
        lane: str,
        title: str,
        purpose: str,
        inputs: list[str],
        outputs: list[str],
        assets: list[str],
        *,
        edit: str | None = None,
        details: Any = None,
        kind: str = "step",
    ) -> None:
        nodes.append(
            dict(
                id=identity,
                lane=lane,
                title=title,
                purpose=purpose,
                inputs=inputs,
                outputs=outputs,
                assets=[refs[key] for key in assets],
                edit_target=edit if editable else None,
                editable=bool(edit and editable),
                kind=kind,
                details=details,
            )
        )

    node(
        "facts",
        "business",
        "读取标准报告",
        "核对授权与报告指纹，冻结本次事实。",
        ["QS 授权", "标准报告"],
        ["结构化事实"],
        ["input_schema"],
    )
    node(
        "prompt",
        "business",
        "组成生成 Prompt",
        "系统说明、任务模板和事实前言共同组成一次请求；不是三次模型调用。",
        ["结构化事实"],
        ["模型消息"],
        ["prompt"],
        edit="prompt",
        details=content,
        kind="composition",
    )
    node(
        "generation",
        "business",
        "生成补充解读",
        "使用本次固定模型路线；未知调用不会自动重发。",
        ["模型消息"],
        ["模型回执"],
        ["generation_route"],
        edit="generation",
        details=models["generation"],
        kind="model_call",
    )
    node(
        "validation",
        "business",
        "校验与保存成果",
        "解析结构、核验事实与安全约束；保留有效成果和调用证据。",
        ["模型回执", "结构化事实"],
        ["解读成果或明确失败"],
        ["profile", "output_schema"],
    )
    node(
        "delivery",
        "business",
        "可靠回传 QS",
        "重投已持久成果，不重新调用模型；QS 接收不代表用户已阅读。",
        ["解读成果"],
        ["QS 接收确认"],
        [],
    )
    node(
        "cases",
        "evaluation",
        "预检与完整案例",
        "按冻结策略执行预检和完整案例义务，不因图形展示减少案例。",
        ["冻结配置", "评测套件"],
        ["候选与预检证据"],
        ["suite", "execution_policy"],
        details=plan,
    )
    node(
        "candidates",
        "evaluation",
        "生成评测候选",
        "每个候选使用上方生成 Prompt 和路线；数量由执行策略约束。",
        ["案例事实"],
        ["候选成果"],
        ["prompt", "generation_route"],
        kind="model_call",
    )
    node(
        "semantic",
        "evaluation",
        "语义评测",
        "仅在配置评测中调用；用户业务生成不会额外执行此模型调用。语义 Prompt 本轮只读。",
        ["候选成果", "案例要求"],
        ["语义评测回执"],
        ["semantic_prompt", "semantic_route", "semantic_output_schema"],
        edit="semantic",
        details={"prompt": semantic_prompt, "model": models["semantic"], "prompt_editable": False},
        kind="model_call",
    )
    node(
        "review",
        "evaluation",
        "人工审核与最终门槛",
        "由有权限的人员实际审核；评测结果不自动代签。",
        ["候选成果", "评测证据"],
        ["审核与最终判断"],
        ["gate_policy"],
    )
    node(
        "publication",
        "evaluation",
        "发布已批准配置",
        "发布、暂停或回退沿用现有版本校验；已接单任务仍用原固定配置。",
        ["审核通过的冻结配置"],
        ["正式发布版本"],
        ["profile"],
    )
    edges: list[dict[str, str]] = []
    for lane in ("business", "evaluation"):
        ids = [n["id"] for n in nodes if n["lane"] == lane]
        edges.extend(
            {"source": a, "target": b, "relation": "sequence"}
            for a, b in zip(ids, ids[1:], strict=False)
        )
    edges.append({"source": "prompt", "target": "candidates", "relation": "reuses_configuration"})
    return {
        "definition_version": DEFINITION,
        "release_fingerprint": release.fingerprint(),
        "nodes": nodes,
        "edges": edges,
    }
