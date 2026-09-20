# Structured logging delivery ledger

Baseline: `c61dfa5`, branch `codex/structured-logging`. Business state, snapshots,
leases, persistence receipts and retry semantics remain authoritative and unchanged.

## Fixed delivery gates

| Batch | Deliverable | State |
| --- | --- | --- |
| L0 | Contract and targeted baseline | Initial contract below; event catalogue expands with L2 |
| L1 | Bounded structured sink, task context, configuration and process metrics | Implemented locally; not released |
| L2 | Ingress, generation, evaluation, delivery and governance correlation | Existing lifecycle/model events migrated; remaining coverage pending |
| L3 | Alloy/Loki/Grafana, 14 days, restricted access | Pending |
| L4 | Mixed load, outage/recovery, deployment and operator acceptance | Pending |

## Contract v1

Every accepted event contains schema_version, UTC timestamp, level, service,
environment, release, instance_id, log_event_id, event and component.
`correlation_id` identifies a communication attempt; `request_id` remains the
existing business idempotency UUID. Session, run and invocation IDs associate
background work with durable state. No changes to API request_id semantics.

Only enumerated primitive fields are emitted. Arbitrary messages, exception text,
payloads, credentials and unknown third-party records are not serialized. This
is not a content classifier: callers must never put secrets into identifier fields.
Error codes and event names must be static, and identifiers must come from validated
business records. Logs do not replace persisted audit or recovery evidence.

Application logging configuration is `logging` in configs/default.yaml; environment
example: QS_AI_LOGGING__LEVEL=debug. The old http.log_level option is rejected.
The release field reads QS_AI_RELEASE_SHA, with explicit unknown until deployment
wiring is completed. No invented release identity.

The output thread has a bounded queue (1024, 128 reserved for warning/error).
Full queues drop diagnostics without affecting business decisions. The root output
handler only accepts qs_ai.structured events; Uvicorn access logs are disabled.
Shutdown gives the sink at most two seconds, inside the existing process deadline.
A permanently blocked output thread is daemonized and cannot prevent process exit.

Process metrics: qs_ai_logging_queued, qs_ai_logging_dropped_total,
qs_ai_logging_failed_total. No request identifiers are metric labels.

## Acceptance tracking

Initial checks cover 100 interleaved tasks, context restoration, sensitive-field
sentinels, blocked and failed output, reserved capacity, bounded shutdown,
configuration rejection, and existing process signal/lifecycle tests.

Not yet claimed: complete error taxonomy and safe stack frames, repeated-error rate
limiting, all business events, gRPC propagation, measured performance, production
SHA wiring, centralized ingestion, retention enforcement, operator acceptance.

Final gates remain those approved in the plan: three 15-minute load comparisons;
health P95 overhead <=20ms; throughput loss <=5%; normal-load drops zero; 30-minute
collector outage recovery within buffering capacity; 95% searchable within 30s,
all test events within 60s; five operator diagnosis scenarios within 10 minutes each.

## L2 increment (local, not deployed)

The application owns diagnostic context/events; infrastructure owns the queued
output adapter. The architecture dependency/cycle test protects this boundary.
Every unary business/management RPC records completion status and elapsed time,
without reading request bodies or exception details. Metadata contract is
`x-correlation-id`; incoming `x-request-id` remains accepted as transport fallback.
Neither header is an authorization input. Missing/malformed values get a fresh ID.

Generation attempt, committed result, evaluation dispatch/receipt/recovery and
result delivery events reuse durable identifiers. Delivery starts a new transport
correlation for each attempt and keeps the original request/event IDs. Logs are
emitted after persistence for committed receipts and scheduled retries.

Checked locally: 50 targeted tests, architecture rules and full-source mypy;
real local gRPC success and permission-denied status preservation; delivery failure
then success without altering persistence calls. Subsequent metadata adjustments
passed the focused nine-test contract suite. Production, load and MySQL matrix
acceptance still pending. Fine-grained fact/authorization stage coverage, safe stack
frames, log rate limiting and full model/evaluation evidence remain open; do not
mark all L2 requirements complete from RPC completion events alone.

## First release gate completion

Image builds now embed QS_AI_RELEASE_SHA from the exact workflow revision. Only
repeated `attempt_failed` idle-loop diagnostics are collapsed (30-second default,
64 bounded component keys). Business state/receipt events are never sampled.
`qs_ai_logging_suppressed_total` and the next emitted record's suppressed_count
expose suppressed repetitions. Logging settings remain deployment-only.

The initial first-release gate consists of CI matrix/interop, safe log failures,
matching metadata, release identity and rate limiting. Centralized production
collection, Grafana access, load and outage acceptance remain the second batch.
