from __future__ import annotations

import json

from .common import RepoWarning


def load_warnings(state_path):
    if not state_path.exists():
        return []
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    return [RepoWarning(**item) for item in payload.get("warnings", [])]


def write_warnings(state_path, warnings):
    payload = {"warnings": [warning.__dict__ for warning in warnings]}
    state_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
