"""Use the existing pinned SSH transport; publish only non-secret handoff receipts."""

import importlib.util
import json
import os
import re
import tempfile
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "handoff_client", Path(__file__).with_name("deploy.py")
)
client = importlib.util.module_from_spec(spec)
spec.loader.exec_module(client)


def arguments(env):
    operation = env.get("HANDOFF_OPERATION")
    if operation == "inspect":
        return [operation]
    name = env.get("HANDOFF_RELEASE", "")
    if not re.fullmatch(r"[a-f0-9]{40}-[0-9]+-[0-9]+", name):
        raise ValueError("Explicit immutable checkpoint release required")
    if operation == "prepare":
        values = [
            env.get(key, "") for key in ("HANDOFF_IMAGE", "HANDOFF_STATE_SHA", "HANDOFF_CONFIG_SHA")
        ]
        if not re.fullmatch("sha256:[a-f0-9]{64}", values[0]) or any(
            not re.fullmatch("[a-f0-9]{64}", v) for v in values[1:]
        ):
            raise ValueError("Exact inspected image, state and config hashes required")
        return [operation, name, *values]
    if operation == "adopt" and re.fullmatch("[a-f0-9]{64}", env.get("HANDOFF_PREPARED_SHA", "")):
        return [operation, name, env["HANDOFF_PREPARED_SHA"]]
    raise ValueError("Expected inspect, prepare or adopt")


def main():
    os.umask(0o077)
    env = dict(os.environ)
    if env.get("AUTO_DEPLOY_ENABLED") != "false":
        raise ValueError("Runtime handoff requires an explicit production deployment freeze")
    args = arguments(env)
    run_id, attempt = (
        client.required(env, "GITHUB_RUN_ID"),
        client.required(env, "GITHUB_RUN_ATTEMPT"),
    )
    if not run_id.isdigit() or not attempt.isdigit():
        raise ValueError("Invalid run identifier")
    with tempfile.TemporaryDirectory(prefix="ai-handoff-", dir=env.get("RUNNER_TEMP")) as temporary:
        work = Path(temporary)
        ssh, scp, host, user = client.ssh_transport(env, work)
        if client.run("server identity", [*ssh, "hostname -s"]).strip().lower() != "servera":
            raise ValueError("SSH target is not serverA")
        directory = f"/tmp/qs-ai-handoff-{run_id}-{attempt}"
        client.run("private helper directory", [*ssh, f"umask 077 && mkdir {directory}"])
        try:
            for filename in ("deploy.py", "runtime_handoff.py"):
                client.run(
                    "upload reviewed handoff helper",
                    [
                        *scp,
                        str(client.ROOT / "deploy/serverA" / filename),
                        f"{user}@{host}:{directory}/{filename}",
                    ],
                )
            raw = client.remote_operation(
                "runtime handoff", ssh, directory + "/runtime_handoff.py", args, env
            )
            result = json.loads(raw)
            if result.get("deployed") is not False:
                raise ValueError("Unexpected handoff receipt")
            Path("handoff-receipt.json").write_text(json.dumps(result, indent=2) + "\n")
            print(json.dumps(result, sort_keys=True))
        finally:
            client.run(
                "remove private helper",
                [
                    *ssh,
                    f"rm -f {directory}/deploy.py {directory}/runtime_handoff.py "
                    f"&& rmdir {directory}",
                ],
            )


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # Raw driver, SSH and Compose output can contain private configuration.
        message = (
            str(error)
            if isinstance(error, (ValueError, client.CommandFailure))
            else type(error).__name__
        )
        raise SystemExit("Runtime handoff failed: " + message) from None
