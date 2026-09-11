import itertools
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

from qs_ai.domain.evaluation.failure import DISPOSITIONS, KINDS, STAGES, ClassifiedFailure
from qs_ai.infrastructure.qs_server.evaluation_policies import load_execution_policy


def test_unknown_quality_contract_and_semantic_failure_recovery():
    policy = load_execution_policy()
    common = {"safe_message": "失败", "evidence_refs": ("execution:1",)}
    unknown = ClassifiedFailure(
        "generation_execution",
        "result_unknown",
        "provider_result_unknown",
        False,
        True,
        "manual_acknowledgement",
        **common,
    )
    quality = ClassifiedFailure(
        "semantic_evaluation",
        "quality_failure",
        "quality_failed",
        False,
        False,
        "retain_candidate",
        **common,
    )
    contract = ClassifiedFailure(
        "output_validation",
        "output_contract_conformance",
        "schema_invalid",
        False,
        False,
        "replace_generation",
        **common,
    )
    semantic = ClassifiedFailure(
        "semantic_evaluation",
        "semantic_execution",
        "semantic_provider_no_message",
        True,
        False,
        "retry_semantic",
        **common,
    )
    for failure in (unknown, quality):
        assert not policy.allows_automatic_generation_recovery(failure)
        assert not policy.allows_automatic_semantic_recovery(failure)
    assert policy.allows_automatic_generation_recovery(contract)
    assert not contract.candidate_exists()
    assert policy.allows_automatic_semantic_recovery(semantic)
    assert not policy.allows_automatic_generation_recovery(semantic)
    assert semantic.candidate_exists() and quality.candidate_exists()


def test_classification_matches_original_go_matrix():
    source = os.getenv("QS_AI_PROMPT_SOURCE")
    if not source or not shutil.which("go"):
        pytest.skip("Requires original QS source and Go")
    cases, expected = [], []
    for stage, kind, disposition, retryable, unknown in itertools.product(
        sorted(STAGES), sorted(KINDS), sorted(DISPOSITIONS), (False, True), (False, True)
    ):
        case = dict(
            stage=stage,
            kind=kind,
            disposition=disposition,
            retryable=retryable,
            result_unknown=unknown,
            code="failure_code",
            safe_message="失败",
            evidence_refs=["execution:1"],
            schema_version="ai-explanation-failure-taxonomy/v1",
        )
        cases.append(case)
        try:
            failure = ClassifiedFailure(**{**case, "evidence_refs": tuple(case["evidence_refs"])})
            expected.append(
                [
                    True,
                    failure.candidate_exists(),
                    failure.allows_generation_replacement(),
                    failure.allows_semantic_retry(),
                ]
            )
        except ValueError:
            expected.append([False, False, False, False])
    program = """package main
import("encoding/json";"os"; evaluation "QS_EVALUATION")
func main(){
 var failures []evaluation.ClassifiedFailure
 if err:=json.NewDecoder(os.Stdin).Decode(&failures);err!=nil{panic(err)}
 result:=[][4]bool{}
 for _,f:=range failures {result=append(result,[4]bool{f.Validate()==nil,f.CandidateExists(),
 f.AllowsGenerationReplacement(),f.AllowsSemanticRetry()})}
 json.NewEncoder(os.Stdout).Encode(result)
}
""".replace(
        "QS_EVALUATION",
        "github.com/FangcunMount/qs-server/internal/apiserver/"
        "domain/interpretation/aiexplanation/evaluation",
    )
    with tempfile.TemporaryDirectory(prefix="qs_ai_failure_", dir=Path(source) / "scripts") as name:
        path = Path(name) / "main.go"
        path.write_text(program)
        result = subprocess.run(
            ["go", "run", str(path)],
            cwd=source,
            input=json.dumps(cases),
            capture_output=True,
            text=True,
            timeout=120,
            check=True,
        )
    assert json.loads(result.stdout) == expected


@pytest.mark.parametrize(
    "change",
    [
        {"safe_message": "测" * 334},
        {"safe_message": "<script>"},
        {"evidence_refs": ("execution:1", " execution:1 ")},
        {"evidence_refs": ("ref with space",)},
        {"code": "UPPER"},
        {"provider_diagnostics": {"response_body": "not allowed"}},
    ],
)
def test_invalid_failure_metadata_cannot_enter_domain(change):
    values = dict(
        stage="generation_execution",
        kind="infrastructure_execution",
        code="provider_failed",
        retryable=False,
        result_unknown=False,
        disposition="no_action",
        safe_message="失败",
        evidence_refs=("execution:1",),
    )
    with pytest.raises(ValueError):
        ClassifiedFailure(**{**values, **change})
