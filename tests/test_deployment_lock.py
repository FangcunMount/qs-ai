"""Bootstrap the shared lock with the deployed account's existing sudo commands."""

import os
from pathlib import Path

import pytest

from tests.test_deployment import load


@pytest.fixture
def locked_deploy(tmp_path, monkeypatch):
    module = load("deploy/serverA/deploy.py")
    root, retention = tmp_path / "qs-ai", tmp_path / "retention"
    root.mkdir()
    monkeypatch.setattr(module, "ROOT", root)
    monkeypatch.setattr(module, "RETENTION_ROOT", retention)
    commands = []

    def run(phase, args):
        commands.append(args)
        assert args[:2] == ["sudo", "-n"]
        command = args[2]
        if command == "mkdir":
            Path(args[-1]).mkdir(exist_ok=True)
        elif command == "chmod":
            Path(args[-1]).chmod(int(args[-2], 8))
        elif command == "chown":
            assert args[-2] == "root:root"
        elif command == "ln":
            try:
                os.link(args[-2], args[-1])
            except FileExistsError:
                raise module.DeploymentError("deployment lock already exists") from None
        else:
            raise module.DeploymentError(f"{command} is not allowed by production sudoers")
        return ""

    monkeypatch.setattr(module, "run", run)
    return module, commands


def test_bootstrap_uses_allowed_commands_and_keeps_shared_inode(locked_deploy):
    module, commands = locked_deploy
    path = module.RETENTION_ROOT / "deploy.lock"
    with module.global_deploy_lock():
        inode = path.stat().st_ino
        assert path.stat().st_mode & 0o777 == 0o666
        assert not list(module.ROOT.iterdir())
        with pytest.raises(BlockingIOError):
            with module.global_deploy_lock():
                pytest.fail("a second deployment entered the shared lock")
    with module.global_deploy_lock():
        assert path.stat().st_ino == inode
    assert sum(args[2] == "ln" for args in commands) == 1


def test_existing_lock_is_not_replaced_or_truncated(locked_deploy):
    module, commands = locked_deploy
    module.RETENTION_ROOT.mkdir()
    path = module.RETENTION_ROOT / "deploy.lock"
    path.write_text("existing lock")
    inode = path.stat().st_ino
    with module.global_deploy_lock():
        assert path.stat().st_ino == inode
        assert path.read_text() == "existing lock"
    assert not any(args[2] in ("ln", "touch") for args in commands)


def test_concurrent_bootstrap_uses_winner_inode(locked_deploy, monkeypatch):
    module, _ = locked_deploy
    original = module.run
    path = module.RETENTION_ROOT / "deploy.lock"

    def raced(phase, args):
        if args[2] == "ln":
            path.write_text("other deployment initialized")
        return original(phase, args)

    monkeypatch.setattr(module, "run", raced)
    with module.global_deploy_lock():
        assert path.read_text() == "other deployment initialized"
    assert not list(module.ROOT.iterdir())


def test_failed_link_never_enters_deployment_and_cleans_temporary_file(locked_deploy, monkeypatch):
    module, _ = locked_deploy
    original = module.run

    def failed(phase, args):
        if args[2] == "ln":
            raise module.DeploymentError("link failed")
        return original(phase, args)

    monkeypatch.setattr(module, "run", failed)
    with pytest.raises(module.DeploymentError, match="link failed"):
        with module.global_deploy_lock():
            pytest.fail("deployment began without its shared lock")
    assert not list(module.ROOT.iterdir())
