import json

import pytest

from qs_ai.application.interpretation.output import DeterministicOutput, InvalidOutput
from qs_ai.application.interpretation.safety import RULES, check_safety


def output(
    summary: str = "分别观察本次结果。",
    limitation: str = "仅基于本次测评，不构成诊断或确定性判断。",
) -> DeterministicOutput:
    return DeterministicOutput(
        json.dumps({"summary": summary, "limitations": [limitation]}, ensure_ascii=False)
    )


def test_accepts_boundary_statement() -> None:
    candidate = output()
    checked = check_safety(candidate)
    assert checked.content_json == candidate.content_json
    assert checked.safety_validator_version.endswith("/v2")


@pytest.mark.parametrize(
    "claim,phrase", [(claim, phrase) for claim, phrases in RULES for phrase in phrases]
)
def test_all_original_forbidden_phrases(claim: str, phrase: str) -> None:
    with pytest.raises(InvalidOutput, match="forbidden_" + claim):
        check_safety(output(phrase))


def test_case_and_spacing_do_not_bypass_rules() -> None:
    with pytest.raises(InvalidOutput, match="forbidden_medication"):
        check_safety(output("TAKE  MEDICATION"))


@pytest.mark.parametrize(
    "limitation",
    [
        "仅基于本次测评。",
        "不构成诊断或确定性判断。",
        "本次测评不构成诊断，但属于确定性判断。",
        "本次测评不构成" + "观察" * 40 + "诊断或确定性判断。",
    ],
)
def test_missing_or_contradictory_boundaries_rejected(limitation: str) -> None:
    with pytest.raises(InvalidOutput, match="limitations_incomplete"):
        check_safety(output(limitation=limitation))


def test_english_boundary() -> None:
    check_safety(output(limitation="This assessment is not a diagnosis or definitive conclusion."))
