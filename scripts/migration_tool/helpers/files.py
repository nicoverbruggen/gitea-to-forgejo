from __future__ import annotations

from pathlib import Path
import hashlib
import os
import shutil


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def replace_tree(source_path: Path, target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists():
        shutil.rmtree(target_path)
    shutil.copytree(source_path, target_path, symlinks=True)


def replace_optional_tree(source_path: Path, target_path: Path) -> None:
    if target_path.exists():
        shutil.rmtree(target_path)
    if source_path.exists():
        shutil.copytree(source_path, target_path, symlinks=True)
    else:
        target_path.mkdir(parents=True, exist_ok=True)


def copy_avatar_if_present(source_path: Path, target_path: Path) -> None:
    if source_path.exists():
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, target_path)
        os.chmod(target_path, 0o644)
