"""Loopback Responses fixture with a process-external, append-only HTTP ledger.

The invocation header correlates tests only: every POST is counted, never deduped.
No request body, authorization header or provider response is saved in the ledger.
"""

import argparse
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import time
from contextlib import closing
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from uuid import UUID


class ProviderProbe:
    def __init__(self, directory: Path, output: dict) -> None:
        self.directory = directory
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.directory.chmod(0o700)
        self.ledger = directory / "requests.sqlite3"
        self.ready = directory / "ready.json"
        self.reply = directory / "synthetic-output.json"
        self.reply.write_text(json.dumps(output))
        self.reply.chmod(0o600)
        self.process: subprocess.Popen | None = None
        self.endpoint = ""

    def start(self) -> "ProviderProbe":
        if self.process is not None:
            raise RuntimeError("Provider already started")
        self.ready.unlink(missing_ok=True)
        self.process = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "--directory", str(self.directory)],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError("Isolated counting provider failed to start")
            if self.ready.exists():
                value = json.loads(self.ready.read_text())
                if value["pid"] != self.process.pid:
                    raise RuntimeError("Provider readiness belongs to another process")
                self.endpoint = value["endpoint"]
                return self
            time.sleep(0.01)
        self.close()
        raise TimeoutError("Counting provider startup budget exceeded")

    def counts(self) -> dict[str, int]:
        with closing(sqlite3.connect(self.ledger, timeout=2)) as db:
            return dict(db.execute("SELECT invocation,COUNT(*) FROM requests GROUP BY invocation"))

    def close(self) -> None:
        if self.process is not None:
            self.process.terminate()
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=2)
            self.process = None

    def __enter__(self) -> "ProviderProbe":
        return self.start()

    def __exit__(self, *_: object) -> None:
        self.close()


def serve(directory: Path) -> None:
    os.umask(0o077)
    ledger = directory / "requests.sqlite3"
    output = json.loads((directory / "synthetic-output.json").read_text())
    with closing(sqlite3.connect(ledger)) as db, db:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute(
            "CREATE TABLE IF NOT EXISTS requests ("
            "request_no INTEGER PRIMARY KEY AUTOINCREMENT, invocation TEXT NOT NULL, "
            "request_hash TEXT NOT NULL)"
        )
    ledger.chmod(0o600)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_: object) -> None:
            pass

        def setup(self) -> None:
            super().setup()
            self.connection.settimeout(3)

        def do_POST(self) -> None:
            invocation = self.headers.get("X-Test-Invocation", "")
            try:
                valid = str(UUID(invocation)) == invocation
            except ValueError:
                valid = False
            raw, model = b"", None
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 2_097_152:
                    raise ValueError
                raw = self.rfile.read(length)
                if len(raw) != length:
                    raise ValueError
                body = json.loads(raw)
                model = body["model"]
                if not isinstance(model, str) or not 0 < len(model) <= 128:
                    raise ValueError
            except (ValueError, KeyError, TypeError, OSError):
                model = None
            with closing(sqlite3.connect(ledger, timeout=2)) as db, db:
                # Commit the attempt before writing a response, without dedupe.
                request_no = db.execute(
                    "INSERT INTO requests(invocation,request_hash) VALUES(?,?)",
                    (invocation if valid else "unattributed", hashlib.sha256(raw).hexdigest()),
                ).lastrowid
            if not valid or model is None:
                self.send_error(400)
                return
            reply = json.dumps(
                dict(
                    id=f"synthetic-response-{request_no}",
                    status="completed",
                    model=model,
                    output=[
                        dict(
                            type="message",
                            content=[dict(type="output_text", text=json.dumps(output))],
                        )
                    ],
                    usage=dict(input_tokens=2, output_tokens=3),
                )
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(reply)))
            self.end_headers()
            try:
                self.wfile.write(reply)
            except OSError:
                pass  # A killed worker cannot erase the already committed attempt.

    with HTTPServer(("127.0.0.1", 0), Handler) as server:
        ready = directory / "ready.json"
        temporary = directory / "ready.tmp"
        temporary.write_text(
            json.dumps(
                dict(pid=os.getpid(), endpoint=f"http://127.0.0.1:{server.server_port}/responses")
            )
        )
        temporary.chmod(0o600)
        temporary.replace(ready)
        server.serve_forever(poll_interval=0.05)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=Path, required=True)
    serve(parser.parse_args().directory)
