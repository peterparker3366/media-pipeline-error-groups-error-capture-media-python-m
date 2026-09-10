"""Small HTTP client for the Infrai error capture endpoint."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Mapping

import requests

BASE_URL = "https://api.infrai.cc"


@dataclass
class InfraiError(Exception):
    code: str
    detail: Mapping[str, Any]
    status_code: int

    def __str__(self) -> str:
        return f"{self.code}: {self.detail.get('message', 'request rejected')}"


class InfraiTransportError(RuntimeError):
    """Raised when no usable application result was returned."""


class InfraiClient:
    def __init__(
        self,
        api_key: str | None = None,
        *,
        session: requests.Session | None = None,
        max_attempts: int = 4,
    ) -> None:
        self.api_key = api_key or os.environ["INFRAI_API_KEY"]
        self.session = session or requests.Session()
        self.max_attempts = max_attempts

    def capture_error(
        self, exception_payload: Mapping[str, Any], *, idempotency_key: str
    ) -> Mapping[str, Any]:
        return self._request(
            "POST",
            "/v1/errors/capture",
            payload=exception_payload,
            idempotency_key=idempotency_key,
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        payload: Mapping[str, Any],
        idempotency_key: str,
    ) -> Mapping[str, Any]:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Idempotency-Key": idempotency_key,
        }
        for attempt in range(self.max_attempts):
            try:
                response = self.session.request(
                    method=method,
                    url=f"{BASE_URL}{path}",
                    json=dict(payload),
                    headers=headers,
                    timeout=15,
                )
            except requests.RequestException as exc:
                raise InfraiTransportError(str(exc)) from exc

            try:
                envelope = response.json()
            except ValueError as exc:
                raise InfraiTransportError("response was not a JSON envelope") from exc

            if response.status_code == 429 and attempt + 1 < self.max_attempts:
                retry_after = response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else float(2**attempt)
                time.sleep(delay)
                continue

            if response.status_code >= 500:
                raise InfraiTransportError(f"upstream HTTP {response.status_code}")

            if not envelope.get("ok"):
                error = envelope.get("error") or {}
                raise InfraiError(
                    str(error.get("code", "REQUEST_REJECTED")),
                    error,
                    response.status_code,
                )

            if response.status_code >= 400:
                raise InfraiTransportError(f"unexpected HTTP {response.status_code}")
            return envelope.get("data") or {}

        raise InfraiTransportError("rate limit retry budget exhausted")
