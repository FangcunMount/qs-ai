"""Finite MBTI reference data; validation is not content approval or activation."""

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any
from urllib.parse import urlsplit

from qs_ai.domain.governance.scenes import MBTI_AXES, MBTI_MODEL, MBTI_VERSION

MBTI_TOPICS = ("personality", "career", "relationships")
_IDENTITY = re.compile(r"[a-z][a-z0-9._-]{0,127}")
_VERSION = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,127}")


class InvalidMBTIReferences(ValueError):
    """Safe category messages only; never echo supplied reference text or URLs."""


def _object(value: Any, fields: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise InvalidMBTIReferences("Invalid reference fields")
    return value


def _text(value: Any, limit: int) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > limit
        or "<" in value
        or ">" in value
    ):
        raise InvalidMBTIReferences("Invalid reference text")
    return value


def _identity(value: Any) -> str:
    if not isinstance(value, str) or not _IDENTITY.fullmatch(value):
        raise InvalidMBTIReferences("Invalid reference identity")
    return value


def _items(value: Any, minimum: int, maximum: int) -> list[Any]:
    if not isinstance(value, list) or not minimum <= len(value) <= maximum:
        raise InvalidMBTIReferences("Invalid reference item count")
    return value


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


@dataclass(frozen=True)
class MBTIReferenceSource:
    source_id: str
    title: str
    url: str
    accessed_on: str
    support_scope: str


@dataclass(frozen=True)
class MBTIReferenceEntry:
    entry_id: str
    topic: str
    axis: str
    pole: str
    content: str
    source_ids: tuple[str, ...]
    usage_boundary: str


@dataclass(frozen=True)
class SelectedMBTIReferences:
    """Values retained by the caller's frozen input, independent of current assets."""

    version: str
    model_code: str
    model_version: str
    type_code: str
    sources: tuple[MBTIReferenceSource, ...]
    entries: tuple[MBTIReferenceEntry, ...]

    @property
    def canonical_json(self) -> str:
        return _canonical({"schema_version": "mbti-reference-selection/v1", **asdict(self)})

    @property
    def fingerprint(self) -> str:
        return "sha256:" + hashlib.sha256(self.canonical_json.encode()).hexdigest()

    def validate_reference(self, ref: str, topic: str, evidence_refs: tuple[str, ...]) -> None:
        """Existence/applicability only; semantic support still requires review."""
        entry = next((item for item in self.entries if ref == "reference:" + item.entry_id), None)
        if entry is None or entry.topic != topic:
            raise InvalidMBTIReferences("Reference is not available for this topic")
        if "dimension:" + entry.axis not in evidence_refs and "model_result" not in evidence_refs:
            raise InvalidMBTIReferences("Reference lacks its report evidence")


@dataclass(frozen=True)
class MBTIReferenceMaterial:
    version: str
    model_code: str
    model_version: str
    sources: tuple[MBTIReferenceSource, ...]
    entries: tuple[MBTIReferenceEntry, ...]

    def select(self, type_code: str) -> SelectedMBTIReferences:
        if (
            not isinstance(type_code, str)
            or len(type_code) != 4
            or any(pole not in axis[1:] for pole, axis in zip(type_code, MBTI_AXES, strict=True))
        ):
            raise InvalidMBTIReferences("Invalid MBTI type for reference selection")
        poles = {axis[0]: pole for axis, pole in zip(MBTI_AXES, type_code, strict=True)}
        entries = tuple(item for item in self.entries if poles[item.axis] == item.pole)
        used_sources = {source for item in entries for source in item.source_ids}
        return SelectedMBTIReferences(
            self.version,
            self.model_code,
            self.model_version,
            type_code,
            tuple(item for item in self.sources if item.source_id in used_sources),
            entries,
        )


def decode_mbti_reference_material(value: Any) -> MBTIReferenceMaterial:
    document = _object(
        value, {"schema_version", "version", "model_code", "model_version", "sources", "entries"}
    )
    if (
        document["schema_version"] != "mbti-reference-material/v1"
        or document["model_code"] != MBTI_MODEL
        or document["model_version"] != MBTI_VERSION
    ):
        raise InvalidMBTIReferences("Unsupported MBTI reference contract")
    version = document["version"]
    if not isinstance(version, str) or not _VERSION.fullmatch(version):
        raise InvalidMBTIReferences("Invalid reference version")
    sources: list[MBTIReferenceSource] = []
    for raw in _items(document["sources"], 1, 8):
        source = _object(raw, {"source_id", "title", "url", "accessed_on", "support_scope"})
        url = _text(source["url"], 2048)
        try:
            parsed = urlsplit(url)
            if (
                parsed.scheme != "https"
                or not parsed.hostname
                or parsed.username
                or parsed.password
                or any(character.isspace() for character in url)
            ):
                raise ValueError
        except ValueError:
            raise InvalidMBTIReferences("Invalid public reference source URL") from None
        accessed_on = source["accessed_on"]
        try:
            if (
                not isinstance(accessed_on, str)
                or date.fromisoformat(accessed_on).isoformat() != accessed_on
            ):
                raise ValueError
        except ValueError:
            raise InvalidMBTIReferences("Invalid reference source date") from None
        sources.append(
            MBTIReferenceSource(
                _identity(source["source_id"]),
                _text(source["title"], 255),
                url,
                accessed_on,
                _text(source["support_scope"], 1000),
            )
        )
    source_ids = {item.source_id for item in sources}
    if len(source_ids) != len(sources):
        raise InvalidMBTIReferences("Duplicate reference source")
    entries: list[MBTIReferenceEntry] = []
    valid_axes = {axis[0]: axis[1:] for axis in MBTI_AXES}
    for raw in _items(document["entries"], 24, 48):
        entry = _object(
            raw, {"entry_id", "topic", "axis", "pole", "content", "source_ids", "usage_boundary"}
        )
        topic, axis, pole = entry["topic"], entry["axis"], entry["pole"]
        if (
            not isinstance(topic, str)
            or topic not in MBTI_TOPICS
            or not isinstance(axis, str)
            or axis not in valid_axes
            or not isinstance(pole, str)
            or pole not in valid_axes[axis]
        ):
            raise InvalidMBTIReferences("Invalid reference topic or pole")
        refs = tuple(_identity(item) for item in _items(entry["source_ids"], 1, 3))
        if len(set(refs)) != len(refs) or not set(refs) <= source_ids:
            raise InvalidMBTIReferences("Invalid reference source links")
        entries.append(
            MBTIReferenceEntry(
                _identity(entry["entry_id"]),
                topic,
                axis,
                pole,
                _text(entry["content"], 1000),
                tuple(sorted(refs)),
                _text(entry["usage_boundary"], 500),
            )
        )
    if len({entry.entry_id for entry in entries}) != len(entries):
        raise InvalidMBTIReferences("Duplicate reference entry")
    required = {
        (topic, axis[0], pole) for topic in MBTI_TOPICS for axis in MBTI_AXES for pole in axis[1:]
    }
    if not required <= {(item.topic, item.axis, item.pole) for item in entries}:
        raise InvalidMBTIReferences("Reference material lacks three-topic pole coverage")
    if {source for item in entries for source in item.source_ids} != source_ids:
        raise InvalidMBTIReferences("Unreferenced source in reference material")
    if len(_canonical(document).encode()) > 131072:
        raise InvalidMBTIReferences("Reference material exceeds size limit")
    return MBTIReferenceMaterial(
        version,
        MBTI_MODEL,
        MBTI_VERSION,
        tuple(sorted(sources, key=lambda item: item.source_id)),
        tuple(sorted(entries, key=lambda item: item.entry_id)),
    )
