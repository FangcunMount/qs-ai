"""Private, checksum-bound native-byte records; never executable SQL or credentials."""

import base64
import gzip
import hashlib
import json
import os
import stat
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, BinaryIO

FORMAT = "qs-ai-native-backup/v1"
MAX_RECORD_BYTES = 512 * 1024 * 1024


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def native_digest(values: list[bytes | None]) -> bytes:
    cells = [
        "SQL_NULL" if value is None else f"{len(value)}:{hashlib.sha256(value).hexdigest()}"
        for value in values
    ]
    return canonical(cells) + b"\n"


def encode(values: list[bytes | None]) -> list[str | None]:
    return [None if value is None else base64.b64encode(value).decode("ascii") for value in values]


def decode(values: Any, size: int) -> list[bytes | None]:
    if not isinstance(values, list) or len(values) != size:
        raise ValueError("Native record column shape mismatch")
    result: list[bytes | None] = []
    for value in values:
        if value is None:
            result.append(None)
        elif isinstance(value, str):
            result.append(base64.b64decode(value, validate=True))
        else:
            raise ValueError("Native record encoding mismatch")
    return result


def _pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, value in values:
        if name in result:
            raise ValueError("Ambiguous backup record")
        result[name] = value
    return result


def private_parent(path: Path) -> None:
    if not path.is_absolute() or path.name in ("", ".", ".."):
        raise ValueError("Backup path must be absolute")
    parent = path.parent
    if (
        parent.is_symlink()
        or not parent.is_dir()
        or parent.stat().st_mode & 0o077
        or path.is_symlink()
    ):
        raise ValueError("Backup parent must be an existing private directory")


@contextmanager
def private_file(path: Path) -> Iterator[BinaryIO]:
    private_parent(path)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_nlink != 1:
            raise ValueError("Backup must be a private regular file")
        yield stream


def fsync_parent(path: Path) -> None:
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@contextmanager
def unpublished(path: Path) -> Iterator[tuple[Path, BinaryIO]]:
    """Publish via an exclusive hard link; an existing artifact is never overwritten."""
    private_parent(path)
    if path.exists():
        raise ValueError("Backup output already exists")
    descriptor, name = tempfile.mkstemp(prefix=".backup-", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "w+b") as stream:
            os.fchmod(stream.fileno(), 0o600)
            yield temporary, stream
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)
        temporary.unlink()
        fsync_parent(path)
    finally:
        temporary.unlink(missing_ok=True)


def write_private(path: Path, value: dict[str, Any]) -> None:
    with unpublished(path) as (_, stream):
        stream.write(canonical(value) + b"\n")


def checksum(stream: BinaryIO) -> str:
    stream.seek(0)
    result = hashlib.sha256()
    for block in iter(lambda: stream.read(1024 * 1024), b""):
        result.update(block)
    stream.seek(0)
    return result.hexdigest()


def records(stream: BinaryIO) -> Iterator[dict[str, Any]]:
    stream.seek(0)
    with gzip.GzipFile(fileobj=stream, mode="rb") as decoded:
        while line := decoded.readline(MAX_RECORD_BYTES + 1):
            if len(line) > MAX_RECORD_BYTES or not line.endswith(b"\n"):
                raise ValueError("Oversized or incomplete backup record")
            value = json.loads(line, object_pairs_hook=_pairs)
            if not isinstance(value, dict):
                raise ValueError("Backup record must be an object")
            yield value


def emit(stream: BinaryIO | gzip.GzipFile, value: dict[str, Any]) -> None:
    line = canonical(value) + b"\n"
    if len(line) > MAX_RECORD_BYTES:
        raise ValueError("Native row exceeds the backup record limit")
    stream.write(line)


def receipt_path(path: Path) -> Path:
    return path.with_name(path.name + ".receipt.json")


def read_receipt(path: Path) -> dict[str, Any]:
    with private_file(receipt_path(path)) as stream:
        raw = stream.read(1024 * 1024 + 1)
    if len(raw) > 1024 * 1024:
        raise ValueError("Oversized backup receipt")
    value = json.loads(raw, object_pairs_hook=_pairs)
    if not isinstance(value, dict) or value.get("format") != FORMAT:
        raise ValueError("Unknown backup receipt")
    return value
