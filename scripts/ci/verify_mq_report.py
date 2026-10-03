"""Reject empty, incomplete or skipped required MQ scenario evidence."""

import json
import sys
from pathlib import Path
from xml.etree import ElementTree

required = {
    "test_mq_health_exposes_only_committed_organization_outbox",
    "test_mq_health_disabled_never_reads_messaging_tables",
    "test_mq_health_failure_propagates_without_partial_or_global_counts",
    "test_mq_health_reader_uses_original_di_without_bootstrap_changes",
    "test_runtime_health_counts_are_organization_scoped",
    "test_mq_metrics_use_existing_di_and_never_claim_missing_storage_is_empty",
    "test_mq_snapshot_distinguishes_due_confirmation_hold_and_org",
    "test_mq_snapshot_sees_only_commit_and_never_settles_or_waits_for_writer",
    "test_mq_security_records_are_gauges_without_invented_duplicate_history",
    "test_mq_snapshot_failure_discards_core_and_partial_mq_values",
    "test_start_change_and_duplicate_preserve_first_effect_receipt_and_wire",
    "test_failure_after_real_effect_rolls_back_business_inbox_and_state",
    "test_rejected_cas_rolls_back_local_command_reservation_but_commits_inbox",
    "test_evaluation_start_cancel_replay_and_final_projection",
    "test_participant_retry_retains_frozen_request_and_replay",
    "test_failed_evaluation_event_rolls_back_projection_and_sequence",
    "test_legacy_handoff_preserves_original_identity_and_never_marks_delivered",
    "test_real_nsq_bootstrap_original_admission_and_shutdown",
    "test_full_candidate_worker_commits_monotone_state_events_without_extra_calls",
    "test_physical_failure_never_admits_and_cannot_forge_logical_budget",
    "test_failed_ack_remains_unknown_and_storage_failure_propagates",
    "test_closed_local_savepoint_preserves_root_and_refusal_receipt",
    "test_oversized_body_authorization_and_exact_acknowledgement",
    "test_preprovisioned_failure_topology_with_real_nsq",
    "test_five_execution_writes_cannot_bypass_mq_admission",
    "test_mq_cutover_keeps_workload_authorization_before_mode_error",
    "test_queries_and_governance_preparation_stay_registered_in_mq_mode",
    "test_legacy_result_settles_only_with_atomic_original_business_ack",
    "test_legacy_handoff_and_ack_share_lock_order_without_deadlock",
}
root = ElementTree.parse(Path(sys.argv[1])).getroot()
cases = root.findall(".//testcase")
if (
    not cases
    or any(c.find(tag) is not None for c in cases for tag in ("failure", "error", "skipped"))
    or not required.issubset({c.get("name", "").split("[")[0] for c in cases})
    or not all(
        root.find(f".//testcase[@name='test_go_python_messaging_interop[{kind}]']") is not None
        for kind in range(1, 10)
    )
    or len([c for c in cases if c.get("classname", "").endswith("test_grpc_mq_cutover")]) != 24
    or not all(
        root.find(
            ".//testcase[@name='test_legacy_result_settles_only_with_atomic_original_business_ack"
            f"[{scenario}]']"
        )
        is not None
        for scenario in (
            "stored",
            "wrong_hash",
            "held",
            "rollback",
            "source_conflict",
            "unowned",
            "storage_error",
        )
    )
):
    raise SystemExit("Required MQ report is empty, incomplete, failed or skipped")
print(json.dumps({"passed": len(cases), "failed": 0, "errors": 0, "skipped": 0}))
