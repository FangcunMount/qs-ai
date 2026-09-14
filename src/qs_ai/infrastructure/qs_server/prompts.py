"""Load explicitly selected, checksum-verified QS migration Prompt packages."""

import hashlib
import json
from pathlib import Path

from qs_ai.application.interpretation.prompts import InvalidPrompt, PromptPackage


def prompt_directory() -> Path:
    bundled = Path(__file__).resolve().parent / "prompt_assets"
    if bundled.is_dir():
        return bundled
    return Path(__file__).resolve().parents[4] / "integrations" / "qs_server" / "prompts"


def load_prompt(template_id: str, version: str, *, directory: Path | None = None) -> PromptPackage:
    # Explicit supported versions also prevent path traversal; never silently use latest.
    if template_id != "cross-dimension-participant-scale" or version != "v6":
        raise InvalidPrompt("Unknown Prompt identity")
    directory = directory if directory is not None else prompt_directory()
    filename = f"{version}.json"
    try:
        manifest = json.loads((directory / "manifest.json").read_bytes())
        raw = (directory / filename).read_bytes()
        if hashlib.sha256(raw).hexdigest() != manifest["files"][filename]:
            raise InvalidPrompt("Prompt file checksum mismatch")
        data = json.loads(raw)
        ref = data["Ref"]
        if (ref["TemplateID"], ref["Version"]) != (template_id, version):
            raise InvalidPrompt("Prompt package identity mismatch")
        fields = [
            ref["Fingerprint"],
            ref["GitBlobSHA"],
            data["SystemMessage"],
            data["TaskTemplate"],
            data["DataPreamble"],
        ]
        placeholders = data["AllowedPlaceholders"]
        if not all(isinstance(x, str) and x.strip() for x in fields):
            raise InvalidPrompt("Invalid Prompt package fields")
        if not isinstance(placeholders, list) or any(not isinstance(x, str) for x in placeholders):
            raise InvalidPrompt("Invalid Prompt placeholders")
        return PromptPackage(
            template_id=template_id,
            version=version,
            fingerprint=ref["Fingerprint"],
            git_blob_sha=ref["GitBlobSHA"],
            system_message=data["SystemMessage"],
            task_template=data["TaskTemplate"],
            data_preamble=data["DataPreamble"],
            allowed_placeholders=tuple(placeholders),
        )
    except (OSError, ValueError, KeyError, TypeError):
        raise InvalidPrompt("Cannot load verified Prompt package") from None
