"""Typed service boundary for media pipeline exceptions."""

from __future__ import annotations

from enum import Enum
from typing import Any, Mapping, Protocol

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .infrai_client import InfraiClient, InfraiError, InfraiTransportError


class PipelineStage(str, Enum):
    INGESTION = "ingestion"
    PROCESSING = "processing"
    CREATOR_DELIVERY = "creator_delivery"


class MediaFailure(BaseModel):
    request_id: str = Field(min_length=1)
    asset_id: str = Field(min_length=1)
    stage: PipelineStage
    exception_type: str = Field(min_length=1)
    message: str = Field(min_length=1)
    traceback: str = Field(min_length=1)
    job_id: str | None = None
    creator_id: str | None = None


class CaptureResult(BaseModel):
    event_id: str
    error_group_id: str
    fingerprint: list[str]


class ErrorCapture(Protocol):
    def capture_error(
        self, exception_payload: Mapping[str, Any], *, idempotency_key: str
    ) -> Mapping[str, Any]: ...


def build_exception_payload(failure: MediaFailure) -> dict[str, Any]:
    """Keep retries of one asset stage in the same operational group."""
    fingerprint = ["media-pipeline", failure.asset_id, failure.stage.value]
    context: dict[str, str] = {
        "request_id": failure.request_id,
        "asset_id": failure.asset_id,
        "stage": failure.stage.value,
    }
    if failure.job_id:
        context["job_id"] = failure.job_id
    if failure.creator_id:
        context["creator_id"] = failure.creator_id
    return {
        "title": f"Media {failure.stage.value} failed",
        "message": failure.message,
        "level": "error",
        "fingerprint": fingerprint,
        "exception": f"{failure.exception_type}: {failure.message}\n{failure.traceback}",
        "context": context,
    }


def capture_pipeline_failure(
    failure: MediaFailure, client: ErrorCapture
) -> CaptureResult:
    payload = build_exception_payload(failure)
    data = client.capture_error(payload, idempotency_key=failure.request_id)
    return CaptureResult(
        event_id=str(data["event_id"]),
        error_group_id=str(data["error_group_id"]),
        fingerprint=list(payload["fingerprint"]),
    )


app = FastAPI(title="Media pipeline error capture")


@app.post("/pipeline-errors", response_model=CaptureResult)
def capture(failure: MediaFailure) -> CaptureResult:
    try:
        return capture_pipeline_failure(failure, InfraiClient())
    except InfraiError as exc:
        status = exc.status_code if 400 <= exc.status_code < 500 else 502
        raise HTTPException(
            status_code=status,
            detail={"code": exc.code, "message": str(exc.detail.get("message", "rejected"))},
        ) from exc
    except (InfraiTransportError, KeyError) as exc:
        raise HTTPException(status_code=502, detail="error capture unavailable") from exc
