"""Isolated two-process acceptance; original evaluation refusal, zero model calls.

Requires explicitly provisioned disposable schemas and a dedicated NSQ container.
Never run against shared resources. No execution/configuration source is patched.
This proves transport persistence/recovery, not accepted-task/model/UI semantics.
"""

import argparse
import asyncio
import hashlib
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from uuid import uuid4

from jwcrypto import jwk
from reliable_messaging.wire import FAILED_CHANNEL, failed_topic
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from tests.integration.test_delivery import certificates

TOPICS = {
    "qs.ai.commands.v1": "qs-ai.commands.v1",
    "qs.ai.events.v1": "qs-server.ai-events.v1",
    "qs.ai.acks.v1": "qs-ai.acks.v1",
}


class Probe:
    def __init__(self, args):
        self.args = args
        self.root = Path(args.output).resolve()
        self.root.mkdir(parents=True, exist_ok=False)
        self.qs = create_async_engine(args.qs_url, pool_size=2, max_overflow=0)
        self.ai = create_async_engine(args.ai_url, pool_size=2, max_overflow=0)
        self.processes = {}
        self.streams = []
        self.env = {k: v for k, v in os.environ.items() if not k.startswith("QS_AI_")}
        self.report = {"status": "running", "model_calls": 0, "scenarios": []}
        self.generation = 0

    async def sql(self, engine, query, **params):
        async with engine.connect() as conn:
            return (await conn.execute(text(query), params)).mappings().all()

    async def docker(self, *args):
        p = await asyncio.create_subprocess_exec(
            "docker", *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        out, _ = await asyncio.wait_for(p.communicate(), 30)
        if p.returncode:
            raise RuntimeError("disposable Docker operation failed")
        return out.decode()

    async def http(self, path, params=None):
        def request():
            url = self.args.nsq_http + path
            if params:
                url += "?" + urllib.parse.urlencode(params)
            with urllib.request.urlopen(
                urllib.request.Request(url, method="GET" if path == "/stats" else "POST"),
                timeout=3,
            ) as response:
                return response.read()

        return await asyncio.to_thread(request)

    async def topology(self):
        for topic, channel in TOPICS.items():
            for t, c in ((topic, channel), (failed_topic(topic, channel), FAILED_CHANNEL)):
                await self.http("/topic/create", {"topic": t})
                await self.http("/channel/create", {"topic": t, "channel": c})

    async def poll(self, read, predicate, label, budget_seconds=90):
        started = time.monotonic()
        while time.monotonic() - started < budget_seconds:
            for name, p in self.processes.items():
                if p.returncode is not None:
                    raise RuntimeError(f"{name} exited during {label}")
            value = await read()
            if predicate(value):
                return value, round(time.monotonic() - started, 3)
            await asyncio.sleep(0.25)
        raise RuntimeError(f"acceptance timeout: {label}")

    def configure(self):
        certificates(self.root)
        paths = {}
        for kid in ("qs.sign", "qs.encrypt", "ai.sign", "ai.encrypt"):
            key = jwk.JWK.generate(kty="EC", crv="P-256", kid=kid)
            for role, raw in (("private", key.export_private()), ("public", key.export_public())):
                path = self.root / f"{kid}.{role}.json"
                path.write_text(raw)
                path.chmod(0o600)
                paths[kid, role] = str(path)
        nsqd = {self.args.nsq_tcp: self.args.nsq_http}
        self.config = self.root / "qs.json"
        self.config.write_text(
            json.dumps(
                {
                    "Options": {
                        "enabled": True,
                        "nsqd": nsqd,
                        "signing_key_file": paths["qs.sign", "private"],
                        "decrypt_key_files": {"qs.encrypt": paths["qs.encrypt", "private"]},
                        "ai_signer_files": {"ai.sign": paths["ai.sign", "public"]},
                        "ai_recipient_key_file": paths["ai.encrypt", "public"],
                    },
                    "Address": self.args.ai_grpc,
                    "CA": str(self.root / "ca.pem"),
                    "Cert": str(self.root / "qs.pem"),
                    "Key": str(self.root / "qs.key"),
                }
            )
        )
        self.ai_env = {
            **self.env,
            "QS_AI_DATABASE_URL": self.args.ai_url,
            "QS_AI_GENERATION__ENABLED": "false",
            "QS_AI_EVALUATION__ENABLED": "false",
            "QS_AI_HTTP__PORT": str(self.args.ai_http_port),
            "QS_AI_GRPC__BIND_ADDRESS": self.args.ai_grpc,
            # The refusal sample has no reference/evidence fetch; never invent a QS service.
            "QS_AI_GRPC__ACCESS_ADDRESS": self.args.qs_payload_address,
            "QS_AI_GRPC__CA_FILE": str(self.root / "ca.pem"),
            "QS_AI_GRPC__CERT_FILE": str(self.root / "ai.pem"),
            "QS_AI_GRPC__KEY_FILE": str(self.root / "ai.key"),
            "QS_AI_MESSAGING": json.dumps(
                {
                    "enabled": True,
                    "nsqd": nsqd,
                    "signing_key_file": paths["ai.sign", "private"],
                    "decrypt_key_files": {"ai.encrypt": paths["ai.encrypt", "private"]},
                    "qs_signer_files": {"qs.sign": paths["qs.sign", "public"]},
                    "qs_recipient_key_file": paths["qs.encrypt", "public"],
                }
            ),
        }
        self.qs_env = {**self.env, "QS_AI_MQ_PROBE_DSN": self.args.qs_dsn}

    async def start(self, name):
        self.generation += 1
        log = (self.root / f"{name}-{self.generation}.log").open("wb")
        self.streams.append(log)
        cmd, env = (
            ([self.args.qs_binary, "-config", str(self.config)], self.qs_env)
            if name == "qs"
            else ([sys.executable, "-m", "qs_ai.bootstrap.server"], self.ai_env)
        )
        self.processes[name] = await asyncio.create_subprocess_exec(
            *cmd, env=env, stdout=log, stderr=log
        )
        if name == "qs":

            async def ready():
                return (self.root / f"qs-{self.generation}.log").read_text(errors="replace")

            await self.poll(ready, lambda x: "READY" in x, "QS runtime readiness")
        else:

            async def ready():
                def get():
                    try:
                        with urllib.request.urlopen(
                            f"http://127.0.0.1:{self.args.ai_http_port}/readyz", timeout=1
                        ) as r:
                            return r.status == 200
                    except Exception:
                        return False

                return await asyncio.to_thread(get)

            await self.poll(ready, bool, "AI runtime readiness")

    async def kill(self):
        for p in self.processes.values():
            if p.returncode is None:
                p.kill()
        await asyncio.gather(*(p.wait() for p in self.processes.values()))
        self.processes.clear()

    async def lose_broker(self):
        await self.kill()
        before = json.loads(await self.http("/stats", {"format": "json"}))
        await self.docker("kill", "--signal=KILL", self.args.nsq_container)
        await self.docker("start", self.args.nsq_container)
        for _ in range(40):
            try:
                await self.http("/stats", {"format": "json"})
                break
            except Exception:
                await asyncio.sleep(0.25)
        else:
            raise RuntimeError("isolated Broker did not recover")
        await self.topology()
        after = json.loads(await self.http("/stats", {"format": "json"}))
        return {"before": before, "after": after}

    async def stage(self):
        identity, aggregate = str(uuid4()), str(uuid4())
        p = await asyncio.create_subprocess_exec(
            self.args.qs_binary,
            "-mode",
            "stage-cancel",
            "-config",
            str(self.config),
            "-id",
            identity,
            "-aggregate",
            aggregate,
            env=self.qs_env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await asyncio.wait_for(p.communicate(), 15)
        assert p.returncode == 0, "original host staging failed"
        return identity

    async def command(self, identity):
        return await self.sql(
            self.qs,
            "SELECT stage,wire_sha256,body_sha256 FROM ai_messaging_outbox WHERE message_id=:id",
            id=identity,
        )

    async def receipt(self, identity):
        return await self.sql(
            self.ai,
            "SELECT o.message_id,o.stage,o.wire_sha256,o.body_sha256,i.decision "
            "FROM messaging_inbox i JOIN messaging_outbox o "
            "ON o.message_id=i.receipt_id WHERE i.message_id=:id",
            id=identity,
        )

    async def settled(self, identity):
        command = await self.command(identity)
        receipt = await self.receipt(identity)
        return bool(
            command and receipt and command[0]["stage"] == receipt[0]["stage"] == "confirmed"
        )

    async def scenario(self, kind):
        await self.start("qs")
        if kind != "command_loss":
            await self.start("ai")
            topic = "qs.ai.events.v1" if kind == "receipt_loss" else "qs.ai.acks.v1"
            await self.http("/channel/pause", {"topic": topic, "channel": TOPICS[topic]})
        identity = await self.stage()
        initial = (await self.command(identity))[0]
        if kind == "command_loss":
            await self.poll(
                lambda: self.command(identity),
                lambda x: x[0]["stage"] == "awaiting_receipt",
                "command PUB without AI",
            )
        else:
            await self.poll(
                lambda: self.receipt(identity),
                lambda x: bool(x) and x[0]["stage"] == "awaiting_receipt",
                "durable AI refusal PUB",
            )
            if kind == "receipt_loss":
                assert (await self.command(identity))[0]["stage"] != "confirmed"
            else:
                await self.poll(
                    lambda: self.command(identity),
                    lambda x: x[0]["stage"] == "confirmed",
                    "durable QS receipt before ACK loss",
                )
                # A committed receipt precedes ACK publication. Wait for PUB OK
                # before killing NSQ so this is a real Broker-loss sample.
                await self.poll(
                    lambda: self.sql(
                        self.qs,
                        "SELECT o.stage FROM ai_messaging_inbox i "
                        "JOIN ai_messaging_outbox o ON o.message_id=i.ack_id "
                        "WHERE i.message_id=(SELECT receipt_id FROM ai_messaging_operations "
                        "WHERE command_id=:id)",
                        id=identity,
                    ),
                    lambda x: bool(x) and x[0]["stage"] == "confirmed",
                    "final ACK PUB before volatile loss",
                )
        receipt_before = await self.receipt(identity)
        broker = await self.lose_broker()
        start = time.monotonic()
        await self.start("qs")
        await self.start("ai")
        _, delay = await self.poll(
            lambda: self.settled(identity),
            bool,
            "original event durable reconciliation",
            budget_seconds=120,
        )
        recovery = round(time.monotonic() - start, 3)
        assert recovery <= 120 and delay <= 90
        final_command = (await self.command(identity))[0]
        final_receipt = (await self.receipt(identity))[0]
        assert initial["wire_sha256"] == final_command["wire_sha256"]
        assert initial["body_sha256"] == final_command["body_sha256"]
        assert final_receipt["decision"] == "rejected"
        if receipt_before:
            assert all(
                receipt_before[0][k] == final_receipt[k]
                for k in ("message_id", "wire_sha256", "body_sha256")
            )
        effect = await self.sql(
            self.ai, "SELECT COUNT(*) n FROM messaging_inbox WHERE message_id=:id", id=identity
        )
        assert effect[0]["n"] == 1
        # Confirm loss actually happened on the Broker's volatile paused channel.
        topic = {
            "command_loss": "qs.ai.commands.v1",
            "receipt_loss": "qs.ai.events.v1",
            "ack_loss": "qs.ai.acks.v1",
        }[kind]

        def depth(stats):
            return sum(
                c["depth"]
                for t in stats["topics"]
                if t["topic_name"] == topic
                for c in t["channels"]
                if c["channel_name"] == TOPICS[topic]
            )

        assert depth(broker["before"]) > 0 and depth(broker["after"]) == 0
        self.report["scenarios"].append(
            {
                "name": kind,
                "command_id": identity,
                "command": dict(final_command),
                "receipt": dict(final_receipt),
                "recovery_seconds": recovery,
                "durable_inbox_effects": 1,
                "lost_channel_depth": depth(broker["before"]),
                "recovered_channel_depth": 0,
            }
        )
        await self.kill()

    async def run(self):
        try:
            assert self.args.nsq_container.startswith("rm-ai-mq-dual-nsq-"), (
                "shared Broker prohibited"
            )
            info = json.loads(await self.docker("inspect", self.args.nsq_container))[0]
            assert info["Config"]["Image"] == "nsqio/nsq:v1.3.0"
            assert "/data" in info["HostConfig"]["Tmpfs"]
            tcp = info["NetworkSettings"]["Ports"]["4150/tcp"][0]
            http = info["NetworkSettings"]["Ports"]["4151/tcp"][0]
            assert self.args.nsq_tcp == f"127.0.0.1:{tcp['HostPort']}"
            assert self.args.nsq_http == f"http://127.0.0.1:{http['HostPort']}"
            self.configure()
            await self.topology()
            for kind in ("command_loss", "receipt_loss", "ack_loss"):
                await self.scenario(kind)
            self.report["status"] = "passed"
        except BaseException as error:
            self.report["status"] = "failed"
            self.report["error_type"] = type(error).__name__
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
    asyncio.run(Probe(parser.parse_args()).run())


if __name__ == "__main__":
    main()
