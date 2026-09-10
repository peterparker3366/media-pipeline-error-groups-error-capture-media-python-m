from typing import Any, Mapping

from src.media_error_service import (
    MediaFailure,
    PipelineStage,
    capture_pipeline_failure,
)


class RecordingClient:
    def __init__(self) -> None:
        self.payload: Mapping[str, Any] = {}
        self.idempotency_key = ""

    def capture_error(
        self, exception_payload: Mapping[str, Any], *, idempotency_key: str
    ) -> Mapping[str, Any]:
        self.payload = exception_payload
        self.idempotency_key = idempotency_key
        return {"event_id": "evt-42", "error_group_id": "grp-7"}


def test_processing_retries_group_by_asset_and_stage() -> None:
    client = RecordingClient()
    failure = MediaFailure(
        request_id="attempt-003",
        asset_id="asset-1080p",
        stage=PipelineStage.PROCESSING,
        job_id="transcode-81",
        exception_type="CodecError",
        message="audio track could not be decoded",
        traceback="worker.py:44",
    )

    result = capture_pipeline_failure(failure, client)

    assert result.fingerprint == ["media-pipeline", "asset-1080p", "processing"]
    assert client.payload["fingerprint"] == result.fingerprint
    assert client.payload["context"]["job_id"] == "transcode-81"
    assert client.idempotency_key == "attempt-003"
    assert result.error_group_id == "grp-7"
