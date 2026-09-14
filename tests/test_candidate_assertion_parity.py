import json
from copy import deepcopy

from qs_ai.infrastructure.qs_server.candidate_assertions import evaluate_candidate_assertions
from qs_ai.infrastructure.qs_server.evaluation_assertions import assertion_inventory
from qs_ai.infrastructure.qs_server.evaluation_case import prepare_evaluation_case
from qs_ai.infrastructure.qs_server.evaluation_suite import V6
from tests.legacy_go_baseline import legacy_go_result
from tests.test_evaluation_case import release
from tests.test_output_validation import candidate


def test_retained_go_assertion_states_across_all_generation_cases():
    requests, actual, labels = [], [], []
    for index in range(1, 8):
        case_id = f"PROMPT-EVAL-{index:03}"
        prepared = prepare_evaluation_case(release(), case_id)
        facts = json.loads(prepared.assembled_input.provider_payload)["facts"]
        value = candidate()
        refs = [d["ref"] for d in facts["dimensions"][:2]]
        value["integrated_insights"][0]["evidence_refs"] = [
            {"kind": "dimension", "ref": r} for r in refs
        ]
        value["suggestions"][0]["evidence_refs"] = [{"kind": "dimension", "ref": refs[0]}]
        for change in ("base", "schema", "reference", "safety", "profile", "literal"):
            output = deepcopy(value)
            if change == "schema":
                output = {}
            elif change == "reference":
                output["suggestions"][0]["evidence_refs"][0]["ref"] = "dimension:unknown"
            elif change == "safety":
                output["summary"] = "这导致了另一种结果。"
            elif change == "profile":
                output["integrated_insights"] *= 2
            elif change == "literal":
                output["summary"] = "SYSTEM MESSAGE"
            assertions = assertion_inventory(V6, case_id)
            requests.append(
                {
                    "Input": json.loads(prepared.assembled_input.provider_payload),
                    "Definition": json.loads(prepared.release.definition_json),
                    "Assertions": [json.loads(a.parameters_json) for a in assertions],
                    "Output": output,
                }
            )
            actual.append(
                [
                    a.status
                    for a in evaluate_candidate_assertions(
                        json.dumps(output, ensure_ascii=False).encode(), prepared, V6, case_id
                    )
                ]
            )
            labels.append((case_id, change, [a.type for a in assertions]))
    result = legacy_go_result(requests)
    expected = json.loads(result.stdout)
    differences = [
        (labels[i][0], labels[i][1], labels[i][2][j], a, b)
        for i, (ours, original) in enumerate(zip(actual, expected, strict=True))
        for j, (a, b) in enumerate(zip(ours, original, strict=True))
        if a != b
    ]
    assert differences == []
