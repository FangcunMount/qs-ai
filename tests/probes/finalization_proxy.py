"""Control the pinned SDK's existing loopback packet proxy; no production targets."""

import asyncio
import json


class RPCProxy:
    def __init__(self, broker):
        self.broker = broker
        self.process = None
        self.address = ""

    async def receive(self, event):
        async with asyncio.timeout(10):
            while True:
                line = await self.process.stdout.readline()
                if not line:
                    raise RuntimeError("Owned RPC proxy exited before required event")
                if line.startswith(b"FAULTPROXY "):
                    value = json.loads(line[len(b"FAULTPROXY ") :])
                    if value["event"] == event:
                        return value
                    if value["event"] == "failed":
                        raise RuntimeError("Owned RPC proxy failed")

    async def command(self, command, event):
        self.process.stdin.write(json.dumps(command).encode() + b"\n")
        await self.process.stdin.drain()
        return await self.receive(event)

    async def start(self):
        tools = self.broker["fault_tools"]
        self.process = await asyncio.create_subprocess_exec(
            tools["binary"],
            "--kind",
            "rocketmq",
            "--upstream",
            self.broker["endpoint"],
            "--cert",
            tools["cert"],
            "--key",
            tools["key"],
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        self.address = (await self.receive("ready"))["address"]
        await self.command(
            dict(Action="arm", Boundary="before", Method="SendMessage", Nth=16), "armed"
        )
        return self

    async def stats(self):
        return await self.command(dict(Action="stats"), "stats")

    async def close(self):
        if self.process is None:
            return
        try:
            if self.process.returncode is None:
                self.process.stdin.write(b'{"Action":"stop"}\n')
                await self.process.stdin.drain()
                await asyncio.wait_for(self.process.wait(), 5)
        finally:
            if self.process.returncode is None:
                self.process.kill()
                await asyncio.wait_for(self.process.wait(), 5)
            self.process.stdin.close()
