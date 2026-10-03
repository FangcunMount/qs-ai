"""Fail closed on missing two-process and volatile-Broker recovery evidence."""

import json
import re
import sys
from pathlib import Path

report = json.loads(Path(sys.argv[1]).read_text())
scenarios = report.get("scenarios", [])
if (
    report.get("status") != "passed"
    or report.get("model_calls") != 0
    or len(scenarios) != 3
    or {item.get("name") for item in scenarios} != {"command_loss", "receipt_loss", "ack_loss"}
):
    raise SystemExit("Required MQ process evidence is failed or incomplete")
for item in scenarios:
    if (
        not 0 < item["recovery_seconds"] <= 120
        or item["durable_inbox_effects"] != 1
        or item["lost_channel_depth"] < 1
        or item["recovered_channel_depth"] != 0
        or item["command"]["stage"] != "confirmed"
        or item["receipt"]["stage"] != "confirmed"
        or item["receipt"]["decision"] != "rejected"
        or any(
            not re.fullmatch(r"[a-f0-9]{64}", item[role][field])
            for role in ("command", "receipt")
            for field in ("wire_sha256", "body_sha256")
        )
    ):
        raise SystemExit("Required original message recovery reconciliation failed")
print(json.dumps({"process_fault_scenarios": 3, "failed": 0, "model_calls": 0}))
