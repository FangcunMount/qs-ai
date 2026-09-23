# MBTI frozen runtime implementation status

P2 now has a database-backed synthetic backend loop. It is not a production
release, real model quality approval or a completed four-client acceptance.
P1 compatibility deployment is verified separately in the project evidence.

## Implemented boundaries

- External admission selects the v1/v2 decoder from the bound report contract.
  Full source validation and an exact active publication are still mandatory.
  MBTI matches only its fixed model/version; it has no scale or wildcard fallback.
- Generation uses the existing published-configuration reader, LangGraph, durable
  model call and artifact builder. Stored requests retain the MBTI input policy
  and frozen Profile; decoding rejects disagreement between them.
- Response recovery uses the original receipt. Dispatched/unknown calls remain
  unknown and cannot authorize a replacement model call. Scale request encoding
  and deterministic validation retain their original version and behavior.
- Output remains `ai-explanation-output/v1`. The MBTI deterministic validator also
  rejects other MBTI type codes, including comparisons with another type. It does
  not detect all natural-language fact conflicts, strength errors or stereotypes;
  independent semantic evaluation and actual human review remain necessary.
- Authored MBTI roots include their own Prompt, Profile v2, judge, 7 × 5 candidate
  cases and unchanged preflight. Their manifest declares new qs-ai authorship,
  not an old QS export. The complete template release pins existing verified
  routes, common execution/gate policy and output schema.
- Existing solution Create accepts a third exclusive source, `template_ref`.
  The existing List response supplies the exact template entry after controlled
  initialization. Preparing keeps the existing CAS/receipt/Run state machine.
  Template creation is never approval; scene substitution is rejected.

## Controlled initialization

Run `python -m qs_ai.bootstrap.import_mbti_assets` with `--source-commit` and
`--imported-by` only after deploying the reviewed image. This is not a startup
hook. It verifies original file hashes, source proof and fixed dependencies in
one transaction. Identical imports preserve initial provenance; conflicts fail.
It creates no solution, evaluation, approval or publication. Runtime reads use
MySQL without falling back to initialization files or choosing latest assets.

The root package is still pre-release. Its template hash was tightened before
production import to include every dependency reference; intermediate local
commits are not separately deployed asset versions.

## Verification and remaining gates

Focused tests cover exact receipt roundtrips, cross-contract rejection, source
scene isolation, original command serialization, initializer repeat/conflict/
rollback and damaged assets. Disposable MySQL tests run the real preparation and
candidate-v2 graph for all 35 candidates, with synthetic generation/judge replies.
An interruption after response persistence recovers without duplicate dispatch.

A disposable test additionally exercises two synthetic reviewer identities and
the unchanged final gate/publication APIs, then external MBTI admission,
persistent generation response, publication pause and worker recovery to a stored
artifact and result outbox. These reviewer fixtures are not real human approvals.
An initialized but unpublished template rejects generation with
`configuration_unavailable`; it cannot bypass governance.

Before P2 delivery: complete required CI and applicable MySQL/Go-Python/scale
regressions, deploy, initialize roots in a controlled operation and verify live
compatibility. P3/P4 still require QS capability checks, Operating/mini-program
integration, real model evaluation, actual independent human approvals, a real
MBTI publication and authorized participant results. No production MBTI assets,
model calls, reviews or publication were created by the isolated tests.
