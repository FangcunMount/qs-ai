"""Finite, versioned scene identities; not a mutable model catalog."""

from dataclasses import dataclass

MBTI_MODEL = "MBTI_OEJTS"
MBTI_VERSION = "v64-report-202608-v1"
MBTI_CONTRACT = "mbti-single-assessment/v1"
MBTI_THEMATIC_CONTRACT = "mbti-single-assessment/v2"
MBTI_AXES = (("EI", "I", "E"), ("SN", "S", "N"), ("TF", "F", "T"), ("JP", "J", "P"))


MBTI_EXPLORATION_MODEL = "MBTI_FC_93"
MBTI_EXPLORATION_VERSION = "v55-report-202608-v1"


@dataclass(frozen=True)
class MBTIModelContract:
    axes: tuple[tuple[str, str, str], ...]
    bounds: tuple[tuple[int, int, float], ...]
    thematic_input_version: str


def mbti_model_contract(code: str | None, version: str | None) -> MBTIModelContract | None:
    if (code, version) == (MBTI_MODEL, MBTI_VERSION):
        return MBTIModelContract(MBTI_AXES, ((8, 40, 24),) * 4, "ai-explanation-input/v3")
    if (code, version) == (MBTI_EXPLORATION_MODEL, MBTI_EXPLORATION_VERSION):
        return MBTIModelContract(
            (("EI", "E", "I"), ("SN", "S", "N"), ("TF", "T", "F"), ("JP", "J", "P")),
            ((0, 23, 11.5), (0, 23, 11.5), (0, 23, 11.5), (0, 24, 11.5)),
            "ai-explanation-input/v4",
        )
    return None


def is_mbti_selector(
    audience: str, kind: str, decision: str, code: str | None, version: str | None
) -> bool:
    return (audience, kind, decision) == (
        "participant",
        "typology",
        "pole_composition",
    ) and mbti_model_contract(code, version) is not None
