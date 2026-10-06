# MBTI operations and acceptance

The supported scope is one participant, one completed `MBTI_OEJTS / v64-report-202608-v1` report. The three-topic product covers self-understanding, career exploration and relationship communication. It does not provide job suitability, partner matching, diagnosis or multi-assessment trends. See [contract](./mbti-three-topic-contract.md) and [current evidence boundary](./mbti-runtime-progress.md).

## Administrator workflow

1. Open the existing Operating AI governance solution workspace and select the MBTI scene. An unpublished scene starts from the exact installed template; a published scene starts a modification from the current MBTI publication. Select the three-topic template explicitly, rather than the older report-only template. Do not copy a scale asset ID or edit database records.
2. Save the purpose and supported model parameters. Preserve the scene, report model/version and inherited reference package. Preparing a complete test freezes assets and creates a Run; it does not start paid calls.
3. Read the plan and available budget, then confirm and start through the normal page. Retain the Run ID. The complete obligation remains seven groups of five candidates plus preflight. A cancel request stops new calls and drains in-flight work; it does not erase unknown outcomes or refund the reserved daily budget.
4. Read saved execution records before starting another evaluation. Technical failures, semantic assertion failures, missing reviews and unknown call outcomes require different decisions. Neither replenishing provider balance nor editing the draft changes an existing Run's frozen configuration. Unknown outcomes must follow the existing disposition flow, not an automatic replacement call.
5. Review all candidate content, report evidence, references and exploration boundaries under both required review roles. Reviewer accounts must satisfy the existing separation rules. Disclose delegated AI reading; do not describe it as independent human reading. Final approval and publication are separate operations and cannot bypass failed gates.
6. Publish the approved Run for the exact MBTI selector through the existing confirmation page. Record the command receipt, publication ID/version, frozen release fingerprint and timestamp. Verify the scale pointer was not changed. A publication is not proof of an actual participant result.

## Formal participant acceptance

Use a dedicated participant session with a real ProfileLink/Testee relationship and a completed report. The administrator session cannot substitute for it. Do not copy access tokens, forge actors or derive report facts from the UI.

The formal collection API exposes source lookup, workflow request and status under `/api/v1/assessments/{id}/ai-workflows`, plus command-operation reconciliation under `/api/v1/interpretation/ai-workflow/operations/{command_id}`. Use the normal client to request once and retain its original command/request IDs. A `submitted`, `pending` or `accepted` response is not result generation or business acceptance. Reconcile the original operation rather than creating another paid request after a timeout.

Acceptance must bind all of the following:

- The authorized source identifies the expected assessment, report, Testee and exact model version.
- The accepted request freezes the intended MBTI publication and its original model/parameters/reference digest.
- The model dispatch has a durable result; the validated artifact has three topics and the selected original reference material.
- QS receives and persists the result, and the client reads that same result. Broker confirmation or a healthy worker is not the QS business receipt.
- The final view distinguishes measured report facts, sourced general references and optional exploration. Formal mini-program/device display is separately recorded from backend API success.

Repeated requests and status reads must reuse the original identity/result, without a second dispatch. An unrelated account must be refused; revocation and restoration must use the actual authorization boundary, not a test actor injected into the backend.

## Pause, recovery and rollback

Publication management already provides `Disable` and `Rollback` with explicit pointer expectation, command ID, reason and confirmation. These are separate from deployment rollback. Use the exact MBTI selector and retain history. Disabling removes that selector's active publication; it does not authorize fallback to scale. Existing accepted work and stored result access must be checked independently. Restore only a proven existing publication, not a guessed asset or latest version.

Before a controlled restart, bind the actual deployed image, the request's frozen release and dispatch/receipt identities. Preserve lease renewal and result commit during the existing 190-second evaluation drain and 210-second container grace. After restart, reconcile the same request: a saved receipt completes without another model call; dispatch without a durable result remains unknown. Do not delete evidence or reset a request to force success.

Keep the compatible current/previous image and release configuration as rollback material. Test unknown-outcome failures and compatible image rollback in isolation; production uses a controlled task and graceful restart. Do not roll back to an image unable to decode the persisted three-topic Profile, input and artifact contracts. No cleanup operation in this delivery authorizes deletion of business data, historical assets, receipts, backups or recovery containers.

## Completion checklist

| Evidence | Passing criterion |
| --- | --- |
| Code/CI | Exact contract, authorization, frozen reference and scale compatibility regressions pass on the relevant revision |
| Deployment | Exact image, one service/process, readiness/database and real mTLS probe |
| Quality/governance | Complete evaluation, actual review records with truthful provenance, final gate and exact-scene publication receipt |
| Participant | Authorized real generation, durable receipt, QS persistence, same-result read and request idempotency |
| Authorization | Unrelated account refusal and actual revoke/restore behavior |
| Recovery | Controlled restart without duplicate dispatch; isolated compatible failure/rollback evidence |
| Compatibility | Scale publication remains unchanged and a real scale path remains usable |
| Mini-program | Formal release/version plus actual device display; deferred UI work is not marked passed |

The permanent compatibility code for old MBTI/scale frozen requests and the two explicit initializer roots remains necessary. Remove only proven unused transition wiring. Documentation cleanup and publication success alone do not complete this checklist.
