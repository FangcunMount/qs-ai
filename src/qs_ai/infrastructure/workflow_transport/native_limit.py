"""Deny-only native audit references; never business or Broker confirmation proof."""

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

ERROR = "Exact read-only native transaction-limit evidence required"


@dataclass(frozen=True, slots=True, repr=False)
class NativeLimitEvidence:
    digest: bytes
    indices: tuple[bytes, ...]

    def __post_init__(self) -> None:
        if type(self.digest) is not bytes or len(self.digest) != 32 or not any(self.digest):
            raise ValueError(ERROR)
        if (
            type(self.indices) is not tuple
            or not 1 <= len(self.indices) <= 64
            or len(set(self.indices)) != len(self.indices)
            or any(type(v) is not bytes or len(v) != 32 or not any(v) for v in self.indices)
        ):
            raise ValueError(ERROR)


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(ERROR)
        result[key] = value
    return result


def _depth(value: Any, level: int = 0) -> None:
    if level > 4:
        raise ValueError(ERROR)
    if isinstance(value, dict):
        for child in value.values():
            _depth(child, level + 1)
    elif isinstance(value, list):
        for child in value:
            _depth(child, level + 1)


def parse_native_limit_evidence(raw: bytes, expected: bytes) -> NativeLimitEvidence:
    """Preserve exact private export bytes; a hash alone is no authorization.

    The operational caller establishes provenance separately. The report cannot
    choose an owner, primary database, credentials or transaction decision.
    """
    try:
        if (
            type(raw) is not bytes
            or not 0 < len(raw) <= 65536
            or type(expected) is not bytes
            or len(expected) != 32
            or not any(expected)
            or hashlib.sha256(raw).digest() != expected
        ):
            raise ValueError(ERROR)
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_object)
        _depth(value)
        fields = {
            "topic",
            "key_sha256",
            "records",
            "raw_admin_sha256",
            "read_only",
            "body_exported",
            "truncated_possible",
            "alert",
            "severity",
        }
        if (
            not isinstance(value, dict)
            or set(value) != fields
            or value["topic"] != "TRANS_CHECK_MAX_TIME_TOPIC"
            or value["alert"] != "native_transaction_check_limit_reached"
            or value["severity"] != "critical"
            or value["read_only"] is not True
            or value["body_exported"] is not False
            or type(value["truncated_possible"]) is not bool
        ):
            raise ValueError(ERROR)
        for key in ("key_sha256", "raw_admin_sha256"):
            if not isinstance(value[key], str) or not re.fullmatch("[0-9a-f]{64}", value[key]):
                raise ValueError(ERROR)
        rows = value["records"]
        if (
            not isinstance(rows, list)
            or not 1 <= len(rows) <= 64
            or value["truncated_possible"] != (len(rows) == 64)
        ):
            raise ValueError(ERROR)
        indices: list[bytes] = []
        numbers = {"queue_id", "queue_offset", "commitlog_offset", "index_hour"}
        for row in rows:
            if not isinstance(row, dict) or set(row) != numbers | {"broker_message_id"}:
                raise ValueError(ERROR)
            identity = row["broker_message_id"]
            if (
                not isinstance(identity, str)
                or len(identity) % 2
                or not re.fullmatch("[0-9A-Fa-f]{32,64}", identity)
            ):
                raise ValueError(ERROR)
            identity = identity.lower()
            if any(type(row[key]) is not int or not 0 <= row[key] < 2**63 for key in numbers):
                raise ValueError(ERROR)
            index = hashlib.sha256(b"m3-native-limit-index-v1\x00")
            for part in (
                value["topic"].encode(),
                bytes.fromhex(value["key_sha256"]),
                bytes.fromhex(identity),
            ):
                index.update(len(part).to_bytes(4, "big"))
                index.update(part)
            for name in ("queue_id", "queue_offset", "commitlog_offset", "index_hour"):
                index.update(row[name].to_bytes(8, "big"))
            indices.append(index.digest())
        # Native copies retain UNIQ_KEY. The evidence constructor rejects
        # duplicate full v1 fingerprints, without treating client IDs as offsets.
        return NativeLimitEvidence(expected, tuple(sorted(indices)))
    except (ValueError, TypeError, KeyError, RecursionError):
        raise ValueError(ERROR) from None


def native_notice_key(owner: str, store_id: str, epoch: str, revision: str, proof: bytes) -> bytes:
    """Same length-prefixed original UTF-8 bytes as the Go host adapters."""
    result = hashlib.sha256(b"m3-native-limit-notice-v1\x00")
    for value in (owner.encode(), store_id.encode(), epoch.encode(), revision.encode(), proof):
        result.update(len(value).to_bytes(4, "big"))
        result.update(value)
    return result.digest()
