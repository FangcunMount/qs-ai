"""Reject empty, incomplete or skipped required MQ scenario evidence."""

import json
import sys
from pathlib import Path
from xml.etree import ElementTree

required = {
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
):
    raise SystemExit("Required MQ report is empty, incomplete, failed or skipped")
print(json.dumps({"passed": len(cases), "failed": 0, "errors": 0, "skipped": 0}))
