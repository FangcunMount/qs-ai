"""Explicit key-file preflight only; no database, broker, model or lifecycle start."""

import json
import os
import stat
from pathlib import Path

from qs_ai.bootstrap.messaging import read_key
from qs_ai.config import MessagingOptions


def check_keys(options: MessagingOptions) -> None:
    if not options.enabled:
        return
    assert options.signing_key_file and options.qs_recipient_key_file
    entries = [
        (options.signing_key_file, True, "sig", None),
        (options.qs_recipient_key_file, False, "enc", None),
        *[(path, True, "enc", kid) for kid, path in options.decrypt_key_files.items()],
        *[(path, False, "sig", kid) for kid, path in options.qs_signer_files.items()],
    ]
    try:
        for filename, private, purpose, expected in entries:
            path = Path(filename)
            role = ("ai" if private else "qs") + (".sign." if purpose == "sig" else ".encrypt.")
            if path.parent != Path("/run/qs-ai-jose") or not path.name.startswith(role):
                raise ValueError
            identity = path.name.removesuffix(".json")
            if expected is not None and expected != identity:
                raise ValueError
            info = path.stat()
            if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= 65536:
                raise ValueError
            if info.st_mode & 0o022 or (private and info.st_mode & 0o007):
                raise ValueError
            key = read_key(str(path), private=private, expected_id=identity)
            if key.get("use") not in {None, purpose}:
                raise ValueError
            if key.get("alg") not in {None, "ES256" if purpose == "sig" else "ECDH-ES+A256KW"}:
                raise ValueError
            operations = key.get("key_ops")
            allowed = (
                {"sign" if private else "verify"}
                if purpose == "sig"
                else {
                    "deriveKey",
                    "deriveBits",
                    "decrypt" if private else "encrypt",
                    "unwrapKey" if private else "wrapKey",
                }
            )
            if operations is not None and (
                not isinstance(operations, list) or not operations or not set(operations) <= allowed
            ):
                raise ValueError
            # The library validates curve points/private components; never retain or emit PEM.
            key.export_to_pem(private_key=private, password=None)
    except Exception:
        raise ValueError("MQ key preflight failed; material withheld") from None


def main() -> None:
    try:
        check_keys(MessagingOptions.model_validate_json(os.environ.get("QS_AI_MESSAGING", "{}")))
    except Exception:
        raise SystemExit("MQ key preflight failed; material withheld") from None
    print(json.dumps({"messaging_key_files": "valid", "read_only": True}))


if __name__ == "__main__":
    main()
