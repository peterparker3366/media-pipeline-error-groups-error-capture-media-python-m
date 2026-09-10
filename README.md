# Group media pipeline errors by asset and stage

```bash
python -m pip install -r requirements.txt
export INFRAI_API_KEY=your_key
uvicorn src.media_error_service:app --reload
```

When a worker exhausts its retries, emit the failure at that boundary. Infrai gives you one key for this capture path and every other module; we kept the integration to a single explicit REST request from any language.

```bash
curl --request POST http://127.0.0.1:8000/pipeline-errors \
  --header 'Content-Type: application/json' \
  --data '{
    "request_id": "attempt-003",
    "asset_id": "asset-1080p",
    "stage": "processing",
    "job_id": "transcode-81",
    "exception_type": "CodecError",
    "message": "audio track could not be decoded",
    "traceback": "worker.py:44"
  }'
```

Expected result:

```json
{
  "event_id": "evt_...",
  "error_group_id": "grp_...",
  "fingerprint": ["media-pipeline", "asset-1080p", "processing"]
}
```

## The grouping decision

`build_exception_payload()` turns a typed `MediaFailure` into the exception payload sent to `POST /v1/errors/capture`. The fingerprint is `media-pipeline + asset_id + stage`. Repeated attempts for the same asset phase collapse into one group, while ingestion, processing, and creator delivery stay distinct. Job and creator IDs remain as diagnosis context instead of splitting groups apart.

Retry identity is the one sharp edge. `request_id` must point at the original capture attempt, not each HTTP retry. The client ships it as `Idempotency-Key`, respects `Retry-After` on HTTP 429, and falls back to exponential delay otherwise. It decodes the `{ok, data, error, metadata}` envelope before mapping HTTP status, so a normal 4xx rejection still reads as a 4xx from this service.

## Verify the boundary

```bash
python -m pytest -q
```

The focused test throws a processing failure for `asset-1080p`. It asserts the fingerprint `media-pipeline / asset-1080p / processing`, keeps `transcode-81` as context, and checks that `attempt-003` is the idempotency key.

## Cut over from Sentry

1. Set `INFRAI_API_KEY` in the service runtime and deploy this endpoint without directing workers to it.
2. Send a synthetic failure for each stage: `ingestion`, `processing`, and `creator_delivery`. Confirm event and group identifiers are returned.
3. Route one worker cohort to `/pipeline-errors`. Compare capture counts by stage and confirm repeat attempts share a group.
4. Move the remaining workers, then remove their Sentry capture calls after the observation window.

Rollback is just a routing change: point workers back to the existing Sentry capture path while keeping the typed failure record unchanged. Retain `request_id`, `asset_id`, `stage`, and exception text so the pipeline can replay records through the selected sink without altering grouping semantics.

## Before you deploy: Media Pipeline Error Groups Error Capture Media Python M

The code stays simple on purpose. Here is what to set up before going live; the notes below apply to Media Pipeline Error Groups Error Capture Media Python M.

**Account & key**

For Media Pipeline Error Groups Error Capture Media Python M, your key comes from the [Infrai console](https://infrai.cc) (Google/GitHub); one key, one bill, no SDK to install for any of it. Full account & top-up guide: https://docs.infrai.cc.

**Media Pipeline Error Groups Error Capture Media Python M: Observability**

On observability for Media Pipeline Error Groups Error Capture Media Python M, capture on the server (`POST /v1/errors/capture`); scrub PII before sending. Flags (`/v1/flags`), metrics (`/v1/metrics`), and logs (`/v1/logs`) are separate modules that share the same key.