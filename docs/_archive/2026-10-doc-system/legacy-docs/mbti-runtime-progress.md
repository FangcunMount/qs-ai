# MBTI frozen runtime implementation status

The backend supports both the original report-only MBTI contract and the three-topic contract. They use the existing solution, evaluation, publication, LangGraph execution and durable-receipt paths. Compatibility is retained for frozen assets and requests; neither version falls back to the scale configuration.

## Contract and responsibility boundaries

| Boundary | Report-only MBTI | Three-topic MBTI |
| --- | --- | --- |
| Exact model | `MBTI_OEJTS / v64-report-202608-v1` | Same exact model |
| Scene | `mbti-single-assessment/v1` | `mbti-single-assessment/v2` |
| Input | `ai-explanation-input/v2` | `ai-explanation-input/v3` |
| Profile | v2 schema | v3 schema with frozen references |
| Model output | `ai-explanation-output/v1` | `ai-explanation-output/v2` |
| Artifact envelope | `qs-ai-artifact/v1` | `qs-ai-artifact/v2` with selected references |
| Explicit template version | `v1` | `three-topic-v1` |

Both MBTI scenes use the authoritative QS report snapshot v2. The evaluation construction marker `qs-published-snapshot-v3` identifies three-topic evaluation input; it does not rename the external report snapshot or business workflow contract.

- Eligibility and admission require the exact model/version, complete report provenance, trusted participant context and a matching active publication. There is no wildcard or scale fallback.
- Thematic input reuses the validated report facts and adds an independently frozen reference selection. It does not rescore the report, infer measured strength from raw values or enrich the facts with theory.
- Generation and semantic evaluation use frozen assets, model parameters and references. Saved request decoding rejects mismatched policy, Profile and input projections.
- Response recovery uses the original durable receipt. A dispatched call without a known result stays unknown; it cannot authorize another model call or a replacement provider.
- Artifact construction checks the original report again. Three-topic artifacts bind the selected reference bytes and digest outside model-written output; source URLs and titles are never trusted model fields.
- Deterministic validators check structure, evidence identity and applicable references. They do not prove natural-language accuracy, reference support or useful career/relationship exploration. Semantic evaluation and quality review remain separate gates.

The implementation entry points are `application/interpretation/eligibility.py`, `input.py`, `mbti_themes_input.py`, `preparation.py`, `output.py` and `application/execution/artifact.py`. Template selection is in `infrastructure/persistence/mysql/solution_templates.py`; preparation and publication reuse the existing governance state machine.

## Controlled initialization

`python -m qs_ai.bootstrap.import_mbti_assets` is a maintenance command, not a startup hook. Pass the reviewed full source commit and initializer identity. Select `--root three-topic-v1` explicitly for the three-topic package; omitting `--root` selects the older `v1` package.

The initializer verifies original file hashes, source proof and fixed dependencies in one transaction. Identical imports preserve original provenance; conflicting content fails without partial installation. It creates no solution, evaluation, approval or publication and does not change quotas. Runtime template and asset reads use MySQL, never initialization files or an implicit latest version. See [template contract](./mbti-template-contract.md).

## Verification layers

Existing focused regressions cover old scale bytes, MBTI receipt roundtrips, cross-scene rejection, frozen reference selection, complete candidate evaluation and recovery without duplicate dispatch. Disposable MySQL tests verify both exact template versions, repeat/conflict/atomic import behavior and unchanged session/Run/publication counts. Synthetic reviewer fixtures are not actual quality approvals.

Production evidence must additionally bind the image, organization, solution, Run, fixed asset identities, review records, publication and actual participant request. A successful test, healthy container or stored publication does not prove authorized participant generation or mini-program delivery.

## Dated production checkpoint: 2026-10-05

This checkpoint records operations separately from the permanent contract:

- Three-topic r5 solution `442adc57-8ce4-4097-8bd4-3382b1edd8e4`, Run `750e6b98-f967-513d-8f68-cbae7c1f81a4`: 35 candidates plus preflight completed; 76 dispatches and 76 durable receipts, no unknown call results.
- Two required review roles were submitted under actual accounts `user:10001` and `user:10002`. The candidate reading was performed by AI under explicit user delegation and disclosed in the records; it must not be described as independent human reading.
- Final approval reached Run version 158. MBTI publication version 1, ID `1b368228-624e-442e-8bf8-06956b31ff42`, was committed at `2026-10-04T23:37:54.506277Z`. The selector is participant / typology / pole_composition / MBTI_OEJTS / v64-report-202608-v1.
- The prior publication pointers, including scale, were unchanged. Post-publication checks bound image `fcbe5751fc41be779a3fb1c93acdc6d7c2118baf`, one Python process, readiness/database and the real mTLS permission probe. This is infrastructure evidence, not participant acceptance.

Remaining: formal participant generation and QS result receipt, authorization/refusal/revocation behavior, controlled production recovery and isolated compatible rollback, actual scale compatibility, and mini-program formal release/device display. Mini-program UI work is deferred by the user while server acceptance proceeds; it remains part of the overall delivery. Do not mark P0–P5 complete from the publication checkpoint alone. See [operations and acceptance](./mbti-operations.md).
