"""Require real mTLS, exact original legacy result and bounded restart proof."""

import json
import re
import sys
from pathlib import Path

report = json.loads(Path(sys.argv[1]).read_text())
if (
    report.get("status") != "passed"
    or report.get("scope") != "historical transport fixture only"
    or report.get("model_calls") != 0
    or report.get("artifact_bytes") != 131_072
    or not 131_072 < report.get("body_bytes", 0) < 16 * 1024 * 1024
    or not 0 < report.get("wire_bytes", 0) <= 262_144
    or not 0 < report.get("failure_wire_bytes", 0) <= 262_144
    or report.get("reference_rejections") != 5
    or report.get("wrong_workload_rejected") is not True
    or report.get("reference_after_restart") is not True
    or report.get("legacy_delivered") is not True
    or report.get("original_time_preserved") is not True
    or report.get("durable_effects") != 1
    or report.get("lost_channel_depth", 0) < 1
    or not 0 < report.get("recovery_seconds", 0) <= 120
    or any(
        not re.fullmatch(r"[a-f0-9]{64}", report.get(k, ""))
        for k in ("body_sha256", "wire_sha256", "probe_sha256")
    )
):
    raise SystemExit("Required MQ payload evidence is failed or incomplete")
print(json.dumps({"legacy_payload_bytes": 131_072, "failed": 0, "model_calls": 0}))
