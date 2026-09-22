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
