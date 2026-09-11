"""Explicit immutable model route baseline; credentials and endpoints remain external."""

import hashlib
import json
from pathlib import Path

from qs_ai.application.interpretation.provider import ModelRoute


def route_directory() -> Path:
    bundled = Path(__file__).resolve().parent / "route_assets"
    if bundled.is_dir():
        return bundled
    return Path(__file__).resolve().parents[4] / "integrations" / "qs_server" / "routes"


def load_route(route: str, revision: str, *, directory: Path | None = None) -> ModelRoute:
    if (route, revision) != ("balanced_text_v1", "v8"):
        raise ValueError("Unknown migrated model route version")
    directory = directory if directory is not None else route_directory()
    filename = "balanced_text_v1-v8.json"
    try:
        raw = (directory / filename).read_bytes()
        manifest = json.loads((directory / "manifest.json").read_bytes())
        if hashlib.sha256(raw).hexdigest() != manifest["files"][filename]:
            raise ValueError("Model route package checksum mismatch")
        envelope = json.loads(raw)
        definition = ModelRoute(**envelope["definition"])
        if (definition.route, definition.revision) != (route, revision):
            raise ValueError("Model route identity mismatch")
        if definition.fingerprint() != envelope["fingerprint"]:
            raise ValueError("Model route fingerprint mismatch")
        return definition
    except (OSError, KeyError, TypeError, ValueError):
        raise ValueError("Cannot load verified model route") from None
