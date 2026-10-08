"""Read-only production Prompt retirement operations in a fixed application image."""

from uuid import uuid4


def run(maintenance, command, args):
    """Keep full plans, backup bytes and proofs inside the private context directory."""
    if command not in {"plan", "backup", "verify", "restore"}:
        raise ValueError("Production Prompt apply is not exposed")
    maintenance.bound()
    maintenance.assert_idle()
    plan = maintenance.directory / "prompt-plan.json"
    backup = maintenance.directory / "prompt-backup.jsonl.gz"
    receipt = maintenance.directory / "prompt-receipt.json"
    invocation = [
        "-m",
        "qs_ai.maintenance.prompt_retirement",
        command,
        "--schema",
        "ai",
        "--revision",
        maintenance.executor_revision,
        "--image-id",
        maintenance.executor_image,
        "--plan",
        "/maintenance/" + plan.name,
    ]
    if command in {"backup", "restore"} or (command == "verify" and backup.exists()):
        invocation += ["--backup", "/maintenance/" + backup.name]
    if command == "backup" or (command == "verify" and receipt.exists()):
        invocation += ["--receipt", "/maintenance/" + receipt.name]
    if command in {"backup", "restore"}:
        invocation += ["--target", "ai_refactor_prompt_" + uuid4().hex]
    return maintenance.tools.container(
        maintenance.executor_image, invocation, source=False, timeout=1200
    )
