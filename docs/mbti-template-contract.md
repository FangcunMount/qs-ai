# MBTI template source in SolutionManagement

This is an additive backend contract for P2. It does not claim Operating integration,
production initialization, model quality approval or an active MBTI publication.

## Read and create

The existing `SolutionManagement.List` response (`qs-ai-solutions/v1`) adds a
`templates` array. An uninitialized MBTI root yields an empty array. Database
failures and damaged assets fail explicitly rather than returning an empty list.
An installed template contains:

- `name`: `MBTI 单次解读首版`.
- `template_ref`: exact `id`, `version`, `fingerprint`; copy the returned reference.
- `scene_contract_version`: `mbti-single-assessment/v1`.
- `selector`: participant / typology / pole_composition / MBTI_OEJTS /
  v64-report-202608-v1.
- `published`: false (the template is never an approval).
- `reason`: `requires_evaluation_review_and_publication`.

The existing Create command accepts `template_ref` as its third, mutually exclusive
source alongside `publication_id` and `source_run_id`. The command ID, title,
reason, solution UUID and trusted organization/operator scope remain required.
No new RPC or separate workspace is introduced. Example shape (replace reference
values with the returned catalog entry; do not use a guessed digest):

```json
{
  "command_id": "00000000-0000-4000-8000-000000000101",
  "title": "MBTI 首版验收",
  "reason": "基于固定首版模板评测，不直接发布",
  "template_ref": {
    "id": "participant-mbti-single",
    "version": "v1",
    "fingerprint": "<copy exact catalog fingerprint>"
  }
}
```

Created/read/listed MBTI workspaces include `scene_contract_version`; `source`
retains the exact template reference with null publication/run source IDs.
`source_release` pins every original dependency, `prepared` starts as null and
`source_reviews` starts empty. Saving and preparing use existing CAS, command
receipts and model capabilities. Preparing fixes 7 × 5 candidates plus preflight
and creates an unstarted Run; it does not approve or publish.

## Invariants and errors

Only the controlled, global MBTI root is a template in this delivery. Guessing a
private/derived suite reference never exposes it as a template. Creation resolves
MySQL bytes and all pinned hashes; it never imports a missing root, reads an
initializer file, chooses latest, or accepts changed Prompt/schema content under
an existing name. The first pre-release initializer revision was tightened to
include all ten dependency references in the hashed root suite; neither local
intermediate revision has been installed in production.

Existing trusted QS authorization applies. Workspaces, draft commands and receipts
remain organization scoped; another operator cannot read a command receipt.
A template source cannot be replaced through saving a suite from another scene.
Profile derivation involving MBTI cannot change schema, scene or selector.

- Malformed or multiple sources: `INVALID_ARGUMENT`.
- Unknown template or inaccessible workspace/receipt: `NOT_FOUND`.
- Stale revision or reused command ID with a changed body: `ABORTED`.
- Missing/damaged template dependencies: explicit invalid configuration; no fallback.
- Unexpected database failure: `UNAVAILABLE`; reconcile the original command.

Old create commands omit the absent template field in their stored command body,
so original publication/run command receipts remain replayable. Source bytes and
published scale fingerprints remain unchanged.

## Verification boundary

Disposable MySQL tests exercise create/save/prepare/replay, organization isolation,
scene rejection, exact dependency checks and a complete 70-call synthetic
candidate-v2 evaluation ending at awaiting review. A persisted response interrupted
before projection recovers without a second dispatch. Synthetic judge results
are test fixtures, not human approvals or production-quality evidence.
