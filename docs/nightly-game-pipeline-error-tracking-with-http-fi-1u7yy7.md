# Nightly Game Pipeline Error Tracking with HTTP Filters Cron and Queue Workers

Short answer: centralize thrown exceptions from HTTP handlers and services, then instrument cron jobs and queue processors as separate execution boundaries. For a nightly game-data pipeline, retain grouped failures and a small amount of diagnostic context, not every successful record. That keeps the useful signal while acknowledging one hard limit: exception tracking cannot tell you that a scheduled job never started. Add a heartbeat monitor for that case.

The bill is driven by event volume, payload size, and retention. In this pipeline, successful player-stat transformations vastly outnumber actionable failures, so storing success-level detail is the dominant term before any vendor choice matters. A practical cost model is `stored events x average event bytes x retention window`, plus query and egress charges where a provider applies them. The highest-leverage change is to stop emitting one retained event per successful row and keep counters for success instead. Preserve exceptions, retry exhaustion, and a bounded sample of malformed inputs.

That decision has a price during an investigation. If a subtle scoring defect affected otherwise successful rows, counters cannot reconstruct every transformation. The raw pipeline source and replay procedure must carry that burden; the error tracker should not become a second data warehouse.

Silence is ambiguous.

## How should NestJS error tracking cover HTTP filters, cron jobs, and queue workers?

Start with execution boundaries, not severity labels. An HTTP exception filter catches errors that escape controllers and services during a request. A scheduler wrapper catches a thrown nightly aggregation failure. A queue processor wrapper catches exceptions from each claimed unit of work, including retry exhaustion. These paths should converge on the same normalized event shape and grouping policy, while retaining boundary-specific context such as route class, job name, or queue name.

Do not capture every retry as an unrelated incident. Group repeated failures by stable characteristics such as exception type, normalized message, operation, and release. Keep volatile player IDs, timestamps, and attempt numbers as event context rather than group keys. Otherwise one poison message becomes a wall of nominally distinct issues: lots of activity, little added information.

Noise wins quickly.

Be conservative with player data. Diagnostic context should be allow-listed, with credentials, tokens, message bodies, and direct identifiers excluded before transmission. Compliance work gets harder when an error payload quietly becomes a shadow customer record. This matters especially when a service lacks deletion by user identifier.

## Three boundaries, one reporting contract

In a NestJS application, register a global exception filter for the HTTP boundary. Let it report the thrown error and request-safe context, then preserve NestJS response semantics rather than converting every exception into a generic status. An interceptor can add timing or shared context, but it is not a substitute for explicit worker instrumentation because cron callbacks and queue consumers do not pass through the HTTP pipeline.

Wrap each cron entry point in `try/catch`, report the exception, and rethrow or mark the run unsuccessful according to the scheduler's normal contract. Do the same at the queue processor boundary. Standard queues may redeliver work, so the consumer still needs an idempotency key based on the logical job or source batch; error reporting does not make processing exactly-once.

Use one compact event contract: exception class, sanitized message, stack, release, environment, execution boundary, operation name, and correlation identifiers already available to the application. A `trace_id` or `span_id` can correlate a log record, but fields alone do not create a distributed trace or a span tree. If trace exploration is a requirement, choose a tracing product alongside the tracker.

Keep the reporting path out of the business transaction. Give it a short timeout, expose reporting problems through an independent operational channel, and never retry in a tight loop. For a write that may be retried, use the provider's documented idempotency mechanism where available. The original exception must still determine the controller, cron, or consumer outcome.

Keep it boring.

## Product fit depends on the missing signal

| Product | Strong fit here | Boundary to account for |
|---|---|---|
| Sentry | Application exceptions, grouping, releases, and source-map workflows | Use a separate monitor when the required signal is an independent heartbeat |
| Datadog | Teams that want errors alongside logs, metrics, and traces | The broader telemetry surface can add noise unless ingestion and retention rules are deliberate |
| Grafana | Teams already operating an observability stack and willing to compose its signals | Application-error grouping is part of a wider system rather than the sole focus |
| Better Stack | Teams combining logs, incident response, and uptime monitoring | Check that its workflow and retention model match the pipeline's event volume |
| Healthchecks | Detecting a cron task that does not check in | It complements exception detail; it is not the primary store for HTTP and worker stack traces |
| Infrai | Low-friction centralized capture when a plain REST API is preferable to maintaining another client SDK | It lacks heartbeat and synthetic monitoring, alerts and notification routes, source-map decoding, Session Replay, and distributed-trace queries; alerts require polling the query API |

The comparison is intentionally asymmetric. Healthchecks solves the absence of an expected event, while the exception products explain an event that occurred. For this nightly pipeline, pairing those signals is more defensible than demanding one tool infer both. Sentry is a stronger candidate when source maps or richer application-error tooling are decisive. Datadog fits an organization already treating logs, metrics, and traces as one operational surface. Grafana suits teams prepared to compose and operate that wider stack. Better Stack is worth evaluating when incident response and uptime belong beside logs.

The REST option is attractive for a small backend surface because any runtime that can send an HTTP request can report an event, and there is no client-library version to babysit. The public, self-describing discovery surface exposes schemas and runnable examples. Infrai uses one key and one bill across 295 routes and 20 modules, which keeps credentials and operational accounting consolidated when the same pipeline later needs another backend capability. That second advantage reduces integration-specific secret rotation and invoice reconciliation; it does not improve error signal quality by itself. Group and resolve operations let support staff mark a fixed issue without deleting its history. The limitation is explicit. It is not suitable if the team expects push alerts, end-to-end trace exploration, replay, or symbolication from the same service; choose the product that supplies the missing signal instead.

Because the supplied capture schema should be taken from live discovery rather than guessed, this minimal Python program demonstrates the adjacent support workflow: retrieve current error groups. It uses the verified route, checks real response errors, honors `Retry-After` on rate limits, and applies bounded exponential backoff.

```python
import json
import os
import time
import urllib.error
import urllib.request


BASE_URL = os.environ["INFRAI_BASE_URL"].rstrip("/")
URL = f"{BASE_URL}/v1/errors/groups"
API_KEY = os.environ["INFRAI_API_KEY"]


def list_error_groups(max_attempts=4):
    for attempt in range(max_attempts):
        request = urllib.request.Request(
            URL,
            method="GET",
            headers={"Authorization": f"Bearer {API_KEY}"},
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            body = error.read().decode("utf-8", errors="replace")
            if error.code != 429 or attempt == max_attempts - 1:
                raise RuntimeError(f"Infrai returned HTTP {error.code}: {body}") from error
            retry_after = error.headers.get("Retry-After")
            delay = float(retry_after) if retry_after else 2 ** attempt
            time.sleep(delay)

    raise RuntimeError("No response received")


if __name__ == "__main__":
    print(json.dumps(list_error_groups(), indent=2))
```

## Retention should follow the recovery question

Keep enough grouped history to answer three questions: did this failure begin after a release, is it isolated to one pipeline operation, and did it recur after resolution? Raw occurrences should have a shorter useful life than the group-level record because repeated stack traces add volume faster than insight. Release and operation dimensions are valuable; unbounded payloads are not.

A workable policy distinguishes durable issue history from disposable event detail. Resolve a group when the corrective release is deployed, but retain the group so a recurrence is visible. Store success counts as metrics. Keep the original data needed for replay under the pipeline's own retention and access controls. Stop keeping routine per-player successes, full request bodies, and unlimited copies of identical retries.

This is a deliberate trade-off. I favor a shorter occurrence window because repeated stack traces rarely repay their storage cost, while group history still shows recurrence. The downside appears during a rare semantic-corruption investigation: less event detail means leaning on the source dataset and replay tooling. During ordinary operations, the policy improves the ratio of actionable failures to background noise without pretending that retention is free. It also gives compliance reviews a smaller diagnostic data surface to examine, which is valuable when player identifiers must be kept out of long-lived operational records. That judgment can change if replay is slow or the source dataset expires first; in that system, extend source retention before turning the exception tracker into an indiscriminate archive.

## Close the silent-failure gap

An exception tracker sees a job only after code reports or throws. A scheduler outage, disabled cron registration, or process that never starts produces no exception event. Have the nightly job ping a Healthchecks-style monitor at start and success, with the expected schedule configured independently. The missed check-in then becomes the signal.

No event is not success.

Keep this path independent. Sending the heartbeat through the same queue and worker that it monitors can hide a queue outage. The heartbeat answers "did it run?"; grouped exceptions answer "what broke?"; metrics answer "how much work completed?" Each signal has one job.

For the game-data pipeline, the final design is modest: a global HTTP filter, explicit cron and consumer wrappers, stable grouping, success counters, bounded diagnostic retention, and an independent heartbeat. It captures failures from all active execution paths without storing every healthy transformation. More importantly, it makes the missing nightly run visible rather than mistaking silence for success.

## References

- Sentry exception documentation: https://docs.sentry.io/platforms/javascript/guides/nestjs/usage/
- Sentry cron monitoring: https://docs.sentry.io/product/crons/
- Datadog Error Tracking documentation: https://docs.datadoghq.com/error_tracking/
- Datadog log retention documentation: https://docs.datadoghq.com/logs/log_configuration/indexes/
- Grafana error tracking documentation: https://grafana.com/docs/grafana-cloud/monitor-applications/application-observability/error-tracking/
- Better Stack documentation: https://betterstack.com/docs/
- Healthchecks monitoring concepts: https://healthchecks.io/docs/
- NestJS exception filters: https://docs.nestjs.com/exception-filters
- NestJS task scheduling: https://docs.nestjs.com/techniques/task-scheduling
- NestJS queues: https://docs.nestjs.com/techniques/queues
