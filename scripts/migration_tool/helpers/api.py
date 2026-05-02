from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

from .common import ForgejoAPIError


class ForgejoAPI:
    def __init__(self, base_url: str, token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token

    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        expected: tuple[int, ...] = (200, 201, 204),
    ) -> Any:
        data = None
        headers = {"Authorization": f"token {self.token}"}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request) as response:
                raw = response.read()
                if response.status not in expected:
                    raise ForgejoAPIError(method, path, response.status, raw.decode("utf-8", "replace"))
                if not raw:
                    return None
                return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")
            raise ForgejoAPIError(method, path, exc.code, body) from exc
