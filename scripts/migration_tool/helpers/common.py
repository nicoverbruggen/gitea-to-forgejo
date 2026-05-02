from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable
import urllib.parse


class ImportErrorWithContext(RuntimeError):
    pass


class ForgejoAPIError(RuntimeError):
    def __init__(self, method: str, path: str, status: int, body: str) -> None:
        super().__init__(f"{method} {path} failed with {status}: {body}")
        self.method = method
        self.path = path
        self.status = status
        self.body = body


SUPPORTED_ACTIVITY_OP_TYPES = {1, 2, 5, 6, 8, 9, 10, 12, 16, 17, 18, 19, 20, 24}


@dataclass
class RepoWarning:
    owner: str
    name: str
    reason: str


@dataclass
class ValidationFailure:
    check: str
    detail: str


def log(message: str) -> None:
    print(f"[import] {message}")


def visibility_from_int(value: Any) -> str:
    mapping = {0: "public", 1: "limited", 2: "private"}
    return mapping.get(int(value or 0), "public")


def bool_value(value: Any) -> bool:
    return bool(int(value or 0))


def nullable_text(value: Any) -> str:
    return "" if value is None else str(value)


def normalize_text(value: Any) -> str:
    return "" if value is None else str(value)


def normalize_int(value: Any) -> int:
    return int(value or 0)


def format_duration_from_ns(value: Any) -> str:
    total_seconds = int((value or 0)) // 1_000_000_000
    if total_seconds <= 0:
        return "0s"
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    parts: list[str] = []
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds or not parts:
        parts.append(f"{seconds}s")
    return "".join(parts)


def path_join(*segments: str) -> str:
    return "/".join(urllib.parse.quote(segment, safe="") for segment in segments)


def repo_warning_key(owner: Any, name: Any) -> tuple[str, str]:
    return (normalize_text(owner).lower(), normalize_text(name).lower())


def sample_values(values: Iterable[Any], limit: int = 10) -> str:
    items = [str(value) for value in values]
    if not items:
        return "(none)"
    if len(items) <= limit:
        return ", ".join(items)
    head = ", ".join(items[:limit])
    return f"{head}, ... (+{len(items) - limit} more)"
