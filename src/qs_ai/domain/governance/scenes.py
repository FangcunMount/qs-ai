"""Finite, versioned scene identities; not a mutable model catalog."""

MBTI_MODEL = "MBTI_OEJTS"
MBTI_VERSION = "v64-report-202608-v1"
MBTI_CONTRACT = "mbti-single-assessment/v1"
MBTI_AXES = (("EI", "I", "E"), ("SN", "S", "N"), ("TF", "F", "T"), ("JP", "J", "P"))


def is_mbti_selector(
    audience: str, kind: str, decision: str, code: str | None, version: str | None
) -> bool:
    return (audience, kind, decision, code, version) == (
        "participant",
        "typology",
        "pole_composition",
        MBTI_MODEL,
        MBTI_VERSION,
    )
