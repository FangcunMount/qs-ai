"""Require existing execution-risk evidence separately from MQ transport evidence."""

import json
import sys
from pathlib import Path
from xml.etree import ElementTree

required = {
    "test_recovery_reuses_response_and_original_release",
    "test_cancellation_after_dispatch_is_unknown_on_recovery",
    "test_response_commit_failure_does_not_authorize_another_send",
    "test_shared_capacity_wait_creates_no_dispatch_and_receipt_bypasses_wait",
    "test_requested_cancellation_is_atomic_audited_and_competes_with_start",
    "test_dispatched_call_must_finish_before_cancel_and_keeps_accepted_output",
    "test_unknown_calls_require_the_original_resolution_path",
    "test_original_request_replay_reserves_one_call_and_cancel_does_not_refund",
    "test_cancelled_generation_waiter_does_not_consume_capacity",
    "test_evaluations_share_global_limit_and_preserve_generation_capacity",
}
parameterized = {
    "test_process_kill_recovers_durable_call_without_another_send": (
        "dispatched",
        "response_received",
    ),
    "test_failure_recovery_does_not_redispatch_even_when_retryable": ("False", "True"),
    "test_dispatched_unknown_blocks_and_preserves_audit": ("False", "True"),
    "test_active_limit_defers_work_and_lease_recovery_reuses_slot": (
        "org",
        "user",
        "assessment",
    ),
}
cases = ElementTree.parse(Path(sys.argv[1])).getroot().findall(".//testcase")
names = {case.get("name", "") for case in cases}
if (
    not cases
    or any(case.find(tag) is not None for case in cases for tag in ("failure", "error", "skipped"))
    or not required.issubset(names)
    or not all(
        f"{name}[{parameter}]" in names
        for name, parameters in parameterized.items()
        for parameter in parameters
    )
):
    raise SystemExit("Required execution report is empty, incomplete, failed or skipped")
print(json.dumps({"scope": "execution_semantics", "passed": len(cases), "skipped": 0}))
