"""QS deterministic pre-publication baseline; not a semantic quality evaluator."""

import json
from dataclasses import dataclass

from qs_ai.application.interpretation.output import DeterministicOutput, InvalidOutput

VERSION = "ai-explanation-safety-deterministic-zh-en/v2"
RULES = (
    ("diagnosis", ("诊断为", "确诊", "证明患有", "diagnosed with", "you have a disorder")),
    ("causality", ("由此导致", "这导致了", "is caused by", "this causes")),
    ("medication", ("建议服用", "应当服药", "开始用药", "take medication", "start medication")),
    (
        "treatment_plan",
        ("治疗方案是", "需要接受心理治疗", "treatment plan is", "must undergo therapy"),
    ),
    (
        "risk_reclassification",
        ("重新分类为高风险", "属于危机状态", "reclassified as high risk", "in a crisis state"),
    ),
    (
        "identity_inference",
        ("说明你就是", "你本质上是", "这证明你是", "this means you are", "you are inherently"),
    ),
    (
        "deterministic_future_prediction",
        ("你一定会", "未来必然", "肯定会发生", "you will definitely", "will inevitably"),
    ),
)
NEGATIONS = (
    "不构成",
    "不能作为",
    "不可作为",
    "不属于",
    "不代表",
    "不等同于",
    "无法作为",
    "not a",
    "not an",
    "not diagnostic",
    "cannot be used as",
    "does not constitute",
)


def _normalize(value: str) -> str:
    # Go unicode.ToLower uses simple one-codepoint mappings; Python's full
    # mapping expands U+0130. Go IsSpace also excludes Python's C0 separators.
    return "".join(
        character.lower()[0]
        for character in value
        if not (character.isspace() and character not in "\x1c\x1d\x1e\x1f")
    )


def _contains(value: str, concepts: tuple[str, ...]) -> bool:
    return any(_normalize(concept) in value for concept in concepts)


def _negated(value: str, concepts: tuple[str, ...]) -> bool:
    for negation in NEGATIONS:
        token = _normalize(negation)
        if token in value and _contains(token, concepts):
            return True
        start = 0
        while (index := value.find(token, start)) >= 0:
            start = index + len(token)
            tail = value[start : start + 64]
            boundary = min(
                (
                    tail.find(marker)
                    for marker in ("但是", "但", "然而", "不过", "but", "however")
                    if marker in tail
                ),
                default=len(tail),
            )
            if _contains(tail[:boundary], concepts):
                return True
    return False


@dataclass(frozen=True)
class SafetyCheckedOutput:
    content_json: str
    deterministic_validator_version: str
    safety_validator_version: str = VERSION


def check_safety(output: DeterministicOutput) -> SafetyCheckedOutput:
    content = json.loads(output.content_json)
    normalized = _normalize(json.dumps(content, ensure_ascii=False, separators=(",", ":")))
    for claim, phrases in RULES:
        if _contains(normalized, phrases):
            raise InvalidOutput("forbidden_" + claim)
    limitations = _normalize(" ".join(content["limitations"]))
    if (
        not _contains(
            limitations, ("本次测评", "本次结果", "current assessment", "this assessment")
        )
        or not _negated(limitations, ("诊断", "diagnosis", "diagnostic"))
        or not _negated(
            limitations,
            (
                "确定性判断",
                "确定判断",
                "确定性结论",
                "确定结论",
                "确定性预测",
                "确定预测",
                "definitive conclusion",
                "deterministic judgment",
                "definitive prediction",
            ),
        )
    ):
        raise InvalidOutput("limitations_incomplete")
    return SafetyCheckedOutput(output.content_json, output.validator_version)
