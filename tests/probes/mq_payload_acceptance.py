"""Isolated legacy 128-KiB Unicode result, real mTLS reference and restart.

Only transport fixtures are created. This is not model, report-quality or
production authorization acceptance. The normal AI server is never patched.
"""

import argparse
import asyncio
import hashlib
import json
import time
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import grpc
from jwcrypto import jwk
from reliable_messaging.protected import TrustedSigner
from reliable_messaging.wire import decode, encode_failure
from sqlalchemy import insert, select

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import messaging_pb2_grpc as rpc
from qs_ai.infrastructure.persistence.mysql.schema import result_outbox, sessions
from qs_ai.infrastructure.workflow_transport.messaging import EVENTS, authenticate
from tests.probes.mq_fault_acceptance import TOPICS, Probe


def artifact(session_id):
    digest = "sha256:" + "a" * 64
    value = {
        "id": str(uuid4()),
        "session_id": session_id,
        "run_id": str(uuid4()),
        "evidence_set_id": str(uuid4()),
        "evidence_fingerprint": "a" * 64,
        "invocation_id": str(uuid4()),
        "provider_request_id": "isolated-transport-fixture",
        "content_json": "",
        "content_fingerprint": digest,
        "input_fingerprint": digest,
        "profile_id": "isolated-transport-fixture",
        "profile_version": "v1",
        "profile_fingerprint": digest,
        "prompt_fingerprint": digest,
        "route_fingerprint": digest,
        "output_validator_version": "v1",
        "safety_validator_version": "v1",
        "assessment_id": "42",
        "report_id": "99",
        "source_version": "standard-v1:101",
        "schema_version": "qs-ai-artifact/v1",
    }

    def encode(padding):
        value["content_json"] = json.dumps(
            {"schema_version": "ai-explanation-output/v1", "text": padding},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        value["content_fingerprint"] = (
            "sha256:" + hashlib.sha256(value["content_json"].encode()).hexdigest()
        )
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

    padding = "边界🙂" * 12_000
    raw = encode(padding)
    missing = 131_072 - len(raw.encode())
    assert missing > 0
    raw = encode(padding + "x" * missing)
    assert len(raw.encode()) == 131_072
    return raw


class PayloadProbe(Probe):
    async def seed(self):
        self.request_id, self.session_id, self.event_id = (str(uuid4()) for _ in range(3))
        self.artifact = artifact(self.session_id)
        self.original_time = datetime(2026, 9, 30, 3, 4, 5, 123456)
        fixture = self.root / "historical-request.json"
        fixture.write_text(
            json.dumps(
                {
                    "Request": {
                        "request_id": self.request_id,
                        "actor": {"org_id": "1", "subject_id": "42"},
                        "testee_id": "7",
                        "assessment_ids": ["42"],
                        "goal": "隔离历史结果传输验证",
                        "evidence": [
                            {
                                "assessment_id": "42",
                                "testee_id": "7",
                                "report_id": "99",
                                "source_version": "standard-v1:101",
                                "facts": [],
                            }
                        ],
                    },
                    "Receipt": {"session_id": self.session_id, "version": 1},
                }
            )
        )
        p = await asyncio.create_subprocess_exec(
            self.args.qs_binary,
            "-mode",
            "stage-history",
            "-input",
            str(fixture),
            "-config",
            str(self.config),
            env=self.qs_env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await asyncio.wait_for(p.communicate(), 15)
        assert p.returncode == 0, "original host historical staging failed"
        payload = {
            "event_id": self.event_id,
            "request_id": self.request_id,
            "session_id": self.session_id,
            "actor": {"org_id": "1", "subject_id": "42"},
            "testee_id": "7",
            "version": 3,
            "status": "completed",
            "artifact_json": self.artifact,
        }
        async with self.ai.begin() as conn:
            await conn.execute(
                insert(sessions).values(
                    id=self.session_id,
                    org_id=1,
                    owner_subject_id="42",
                    testee_id=7,
                    assessment_ids=["42"],
                    goal="isolated historical transport fixture",
                    status="completed",
                    version=3,
                    workflow_version="historical-transport-fixture",
                )
            )
            await conn.execute(
                insert(result_outbox).values(
                    event_id=self.event_id,
                    session_id=self.session_id,
                    version=3,
                    payload=payload,
                    created_at=self.original_time,
                    available_at=self.original_time,
                )
            )

    async def event(self):
        return await self.sql(
            self.ai,
            "SELECT message_id,stage,wire,body,wire_sha256,body_sha256 "
            "FROM ai_messaging_outbox WHERE message_id=:id",
            id=self.event_id,
        )

    def authenticated(self, row):
        envelope = authenticate(
            row["wire"],
            EVENTS,
            decrypt_keys={
                "qs.encrypt": jwk.JWK.from_json((self.root / "qs.encrypt.private.json").read_text())
            },
            trusted_signers={
                "ai.sign": TrustedSigner(
                    "qs-ai", jwk.JWK.from_json((self.root / "ai.sign.public.json").read_text())
                )
            },
        )
        assert envelope.message_id == self.event_id
        assert envelope.aggregate_key == self.request_id
        assert envelope.original_occurred_at == "2026-09-30T03:04:05.123456+00:00"
        assert envelope.HasField("payload_reference")
        return envelope.payload_reference

    async def fetch(self, reference, *, wrong_workload=False):
        name = "ai" if wrong_workload else "qs"
        credentials = grpc.ssl_channel_credentials(
            root_certificates=(self.root / "ca.pem").read_bytes(),
            private_key=(self.root / f"{name}.key").read_bytes(),
            certificate_chain=(self.root / f"{name}.pem").read_bytes(),
        )
        async with grpc.aio.secure_channel(self.args.ai_grpc, credentials) as channel:
            return await rpc.MessagePayloadsStub(channel).Get(reference, timeout=5)

    async def rejected_reference(self, reference, field, value):
        changed = pb.MessagePayloadReference()
        changed.CopyFrom(reference)
        setattr(changed, field, value)
        try:
            await self.fetch(changed)
        except grpc.aio.AioRpcError as error:
            assert error.code() == grpc.StatusCode.NOT_FOUND
        else:
            raise AssertionError("changed reference was accepted")

    async def reconciled(self):
        event = await self.event()
        projection = await self.sql(
            self.qs,
            "SELECT version,status,projection FROM ai_bridge_requests WHERE request_id=:id",
            id=self.request_id,
        )
        old = await self.sql(
            self.ai,
            "SELECT delivered,mq_owned FROM result_outbox WHERE event_id=:id",
            id=self.event_id,
        )
        return bool(
            event
            and event[0]["stage"] == "confirmed"
            and projection[0]["status"] == "completed"
            and old[0]["delivered"]
            and old[0]["mq_owned"]
        )

    async def run(self):
        self.report = {
            "status": "running",
            "model_calls": 0,
            "scope": "historical transport fixture only",
        }
        try:
            assert self.args.nsq_container.startswith("rm-ai-mq-dual-nsq-")
            info = json.loads(await self.docker("inspect", self.args.nsq_container))[0]
            assert info["Config"]["Image"] == "nsqio/nsq:v1.3.0"
            assert "/data" in info["HostConfig"]["Tmpfs"]
            tcp = info["NetworkSettings"]["Ports"]["4150/tcp"][0]
            http = info["NetworkSettings"]["Ports"]["4151/tcp"][0]
            assert self.args.nsq_tcp == f"127.0.0.1:{tcp['HostPort']}"
            assert self.args.nsq_http == f"http://127.0.0.1:{http['HostPort']}"
            self.configure()
            await self.topology()
            await self.seed()
            await self.http("/channel/pause", {"topic": EVENTS, "channel": TOPICS[EVENTS]})
            await self.start("ai")
            rows, _ = await self.poll(
                self.event,
                lambda x: bool(x) and x[0]["stage"] == "awaiting_receipt",
                "original result PUB",
            )
            first = rows[0]
            reference = self.authenticated(first)
            response = await self.fetch(reference)
            assert response.reference == reference and response.body == first["body"]
            assert hashlib.sha256(response.body).hexdigest() == reference.body_sha256
            assert len(response.body) == reference.body_length
            for field, value in (
                ("destination", "qs-ai"),
                ("organization_id", "2"),
                ("body_sha256", "0" * 64),
                ("message_id", str(uuid4())),
                ("body_length", reference.body_length + 1),
            ):
                await self.rejected_reference(reference, field, value)
            try:
                await self.fetch(reference, wrong_workload=True)
            except grpc.aio.AioRpcError as error:
                assert error.code() == grpc.StatusCode.PERMISSION_DENIED
            else:
                raise AssertionError("untrusted workload could read original body")
            failed = encode_failure(
                decode(first["wire"]),
                topic=EVENTS,
                channel=TOPICS[EVENTS],
                transport_id="0" * 16,
                attempts=65535,
                timestamp=0,
                cause="handler_failed",
            )
            assert len(first["wire"]) <= 262144 and len(failed) <= 262144
            broker = await self.lose_broker()
            lost = sum(
                c["depth"]
                for t in broker["before"]["topics"]
                if t["topic_name"] == EVENTS
                for c in t["channels"]
                if c["channel_name"] == TOPICS[EVENTS]
            )
            assert lost > 0
            started = time.monotonic()
            await self.start("ai")
            await self.start("qs")
            await self.poll(self.reconciled, bool, "original result and final ACK", 120)
            recovered = (await self.event())[0]
            assert all(
                first[k] == recovered[k]
                for k in ("message_id", "wire", "body", "wire_sha256", "body_sha256")
            )
            assert (await self.fetch(reference)).body == first["body"]
            projection = (
                await self.sql(
                    self.qs,
                    "SELECT projection FROM ai_bridge_requests WHERE request_id=:id",
                    id=self.request_id,
                )
            )[0]["projection"]
            if isinstance(projection, str):
                projection = json.loads(projection)
            assert (
                projection["event_id"] == self.event_id
                and projection["artifact_json"] == self.artifact
            )
            count = (
                await self.sql(
                    self.qs,
                    "SELECT COUNT(*) n FROM ai_bridge_events WHERE event_id=:id",
                    id=self.event_id,
                )
            )[0]["n"]
            assert count == 1
            old = (
                await self.sql(
                    self.ai,
                    "SELECT payload,created_at FROM result_outbox WHERE event_id=:id",
                    id=self.event_id,
                )
            )[0]
            assert old["created_at"] == self.original_time
            payload = (
                json.loads(old["payload"]) if isinstance(old["payload"], str) else old["payload"]
            )
            assert payload["artifact_json"] == self.artifact
            async with self.ai.connect() as conn:
                assert (
                    await conn.execute(
                        select(sessions.c.version).where(sessions.c.id == self.session_id)
                    )
                ).scalar_one() == 3
            self.report.update(
                status="passed",
                event_id=self.event_id,
                request_id=self.request_id,
                artifact_bytes=len(self.artifact.encode()),
                body_bytes=len(first["body"]),
                wire_bytes=len(first["wire"]),
                failure_wire_bytes=len(failed),
                body_sha256=first["body_sha256"],
                wire_sha256=first["wire_sha256"],
                reference_rejections=5,
                wrong_workload_rejected=True,
                reference_after_restart=True,
                durable_effects=count,
                lost_channel_depth=lost,
                recovery_seconds=round(time.monotonic() - started, 3),
                legacy_delivered=True,
                original_time_preserved=True,
            )
        except BaseException as error:
            self.report.update(status="failed", error_type=type(error).__name__)
            raise
        finally:
            await self.kill()
            await self.qs.dispose()
            await self.ai.dispose()
            for stream in self.streams:
                stream.close()
            self.report["probe_sha256"] = hashlib.sha256(
                await asyncio.to_thread(Path(__file__).read_bytes)
            ).hexdigest()
            (self.root / "acceptance.json").write_text(json.dumps(self.report, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in (
        "output",
        "qs-binary",
        "qs-dsn",
        "qs-url",
        "ai-url",
        "nsq-container",
        "nsq-tcp",
        "nsq-http",
        "ai-grpc",
        "qs-payload-address",
    ):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--ai-http-port", type=int, required=True)
    asyncio.run(PayloadProbe(parser.parse_args()).run())


if __name__ == "__main__":
    main()
