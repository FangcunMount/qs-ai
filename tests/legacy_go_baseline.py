"""Read synthetic results captured from the retired Go implementation before removal."""

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

BASELINE_SHA256 = "7e3223be35fb8818004ec90fd662e3f87ecb5c3219f4a15df7067532cb80eea8"


def legacy_go_result(request):
    raw = (Path(__file__).parent / "fixtures/legacy_go_contracts.json").read_bytes()
    assert hashlib.sha256(raw).hexdigest() == BASELINE_SHA256
    baseline = json.loads(raw)
    assert baseline["source_commit"] == "1b52081ea42c94dc5653ce91e8c7a1db9f85fc44"
    key = hashlib.sha256(
        json.dumps(request, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()
    return SimpleNamespace(stdout=json.dumps(baseline["cases"][key]["response"]))
