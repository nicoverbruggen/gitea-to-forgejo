from __future__ import annotations

from pathlib import Path
import subprocess


def git_head_text(repo_path: Path) -> str:
    head_path = repo_path / "HEAD"
    return head_path.read_text(encoding="utf-8").strip() if head_path.exists() else ""


def git_ref_map(repo_path: Path) -> dict[str, str]:
    result = subprocess.run(
        ["git", "--git-dir", str(repo_path), "for-each-ref", "--format=%(refname)%09%(objectname)", "refs"],
        check=True,
        capture_output=True,
        text=True,
    )
    refs: dict[str, str] = {}
    for line in result.stdout.splitlines():
        if not line:
            continue
        ref_name, object_name = line.split("\t", 1)
        refs[ref_name] = object_name
    return refs


def git_fsck_output(repo_path: Path) -> tuple[int, str]:
    result = subprocess.run(
        ["git", "--git-dir", str(repo_path), "fsck", "--strict", "--no-progress"],
        capture_output=True,
        text=True,
    )
    return result.returncode, (result.stdout + result.stderr).strip()


def git_origin_url(repo_path: Path) -> str:
    try:
        return subprocess.check_output(
            ["git", "--git-dir", str(repo_path), "config", "--get", "remote.origin.url"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except subprocess.CalledProcessError:
        return ""
