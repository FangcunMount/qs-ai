# MBTI frozen runtime implementation status

This is an implementation checkpoint inside P2, not a release or quality approval.
The P1 compatibility release remains the production boundary until the full backend
path, initialized roots and template-based solution workflow are verified.

## Implemented runtime boundaries

- A `qs-published-snapshot-v2` session binds an exact MBTI v2 report and Profile.
  Its selector has no scale or wildcard fallback. A v1 session rejects that snapshot.
- Generation uses the same published-configuration reader, LangGraph, durable model
  call and artifact builder. The stored request retains the MBTI input policy and
  frozen Profile; the decoder rejects disagreement between them.
- Response recovery uses the original receipt. Dispatched/unknown calls remain
  unknown and cannot trigger a replacement model call.
- Output remains `ai-explanation-output/v1`, with the existing evidence and safety
  gates. The MBTI deterministic validator additionally rejects other MBTI type codes
  (including comparisons with another type). It does not claim to detect all natural
  language fact conflicts, strength errors or stereotypes. Independent MBTI semantic
  evaluation and human review remain required.
- Scale request encoding remains byte-identical. Scale deterministic validation
  retains its original version and behavior.

## Verification and unfinished work

Focused synthetic tests cover preparation, exact receipt roundtrip, cross-contract
rejection, response/unknown recovery, deterministic artifact replay and type conflicts.
These tests do not establish database recovery, production generation, clinical or
product quality. No new model requests or publication changes were made.

Before the P2 release, finish and verify: MBTI-specific root Prompt/Profile/suite/judge,
controlled idempotent initialization, template source for the existing solution flow,
full evaluation and stored-asset validation, MySQL frozen configuration/recovery tests,
and the backend end-to-end loop. Do not expose an incomplete MBTI production path.

## Controlled root assets (implementation checkpoint)

The authored MBTI root now contains its own Prompt, Profile v2, semantic judge,
seven groups of five candidates and the unchanged preflight obligation. Its
manifest explicitly identifies new qs-ai authorship; it is not an old QS export.
The suite pins the existing approved generation and semantic routes. The common
execution policy, gate policy and output schema remain unchanged.

`python -m qs_ai.bootstrap.import_mbti_assets --source-commit <40-hex-commit>
--imported-by <operator>` is a controlled initializer, not a startup hook. It
verifies original file hashes, source proof and existing fixed dependencies in a
single transaction. Identical imports preserve initial provenance; conflicts
fail. It creates no solution, evaluation, approval or publication. Runtime asset
reads must use MySQL and cannot fall back to these initialization files.

Verification at this checkpoint: 127 focused unit tests passed, three disposable
MySQL 8.4 initialization tests passed (repeat, conflict and late rollback), plus
repository lint/format and source type checks. This is not yet a backend release:
template-based solution creation and the complete database-backed evaluation /
generation / recovery loop remain to be completed before P2 delivery. No live
MBTI assets or model calls were created by these tests.
