# Protected sharing HTTP boundary

Implemented within **4.4 — Critical / Must have — shared pilot** in
[sharing_api.py](../src/application/sharing_api.py). The opt-in ASGI callable connects
the existing [owned worker](SHARING_OPERATION_HOST.md) to HTTP requests. This
records the original sharing HTTP slice. The final [4.4 runtime gates](WORKFLOW_RUNTIME_GATES.md)
complete the remaining implementation/isolated verification scope; Priority 4 is
**4/7 complete, 3/7 remaining**. Defaults and serving activation remain disabled.

## Request and authorization contract

Seven fixed POST routes use JSON bodies:

| Route | Body | Existing executor behavior |
| --- | --- | --- |
| `/v1/sharing/issue_intent` | `workspace_id`, `conversation_id`, `visibility`, `expected_membership_revision`, `expected_conversation_revision`, `expected_workspace_revision`, `retry_key` | Authenticate current creator/owner access and seal the original intent |
| `/v1/sharing/submit` | `token` | Submit/reconcile the sealed original attempt |
| `/v1/sharing/recover` | `token` | Read original-job outcome without submission |
| `/v1/sharing/issue_retry` | `token` | Require fresh positive ancestor noncommit and current controls before issuing a retry |
| `/v1/sharing/submit_retry` | `token` | Dispatch/reconcile the sealed retry |
| `/v1/sharing/recover_retry` | `token` | Read retry-job outcome without submission |
| `/v1/sharing/recover_receipt` | `token` | Recover a business-intent receipt with fresh access; absence remains UNKNOWN |

Submission/recovery semantics remain in the existing executor. The API does not
retry these calls, start workers, remint IDs or infer commit from HTTP delivery.

API, host, runtime and executor are disabled by default and require explicit
trusted composition. The server must prestart the host outside request handling,
pin the actual IAP audience and configure exact HTTPS origins. No default company
domain, marketing role, personal-account grant or development identity is supplied.
This change installs no ASGI server, launch command, Cloud Run deployment or IAP
configuration. Lifecycle prewarm is explicitly server-owned; the callable accepts
HTTP scopes only. Company sign-in activation remains deferred.

Each route requires an exact allowed `Origin`, JSON content type and one IAP
assertion. Duplicate sensitive headers, aliases, malformed header values and query
strings are rejected. Only `x-goog-iap-jwt-assertion` reaches the worker; cookies,
email, roles, forwarded host, caller deadlines and other identity headers do not.
The existing verifier checks the assertion signature/issuer/audience/time and the
existing executor rereads current BigQuery authorization and signing controls.
The origin allowlist is a browser request control, not identity or a permission.
Google documents the need to validate [IAP signed assertions](https://docs.cloud.google.com/iap/docs/signed-headers-howto).

Bodies are limited to 64 KiB before accumulation, reject duplicate JSON keys,
nonfinite values and unexpected fields, and reuse the fixed host projection.
Declared content length must match received bytes. Only complete bodies can call
the host. Header count/combined bytes are bounded. Browser-supplied principals,
roles, worker executables, typed attempts and timeout settings cannot alter server
composition. No wildcard CORS headers or credential forwarding are added.

## Response and recovery contract

Responses use `version = sharing-api-v1`, a server-generated `request_id`, and
either `operation`/`result` or a safe `error`. Successful results are the host's
existing typed minimal projections; internal records, keys and identities are
withheld. UNKNOWN remains an explicit outcome and is not a rollback or retry grant.
Responses include JSON content type, `Cache-Control: no-store`, `nosniff` and exact
content length. Sealed tokens are returned only by authorized issuance methods.
The code does not log request headers, bodies, tokens or raw provider exceptions.

Invalid identity, inaccessible/absent resources, stale controls and invalid seals
share HTTP 403 `ACCESS_DENIED`. Disabled/unstarted/retired/unavailable supervisors
return 503 `SERVICE_UNAVAILABLE`; capacity exhaustion returns 503 `SERVICE_BUSY`.
Malformed requests, oversize bodies, unsupported media/methods and unknown routes
have bounded 400/413/415/405/404 responses. Unexpected failures have a safe 503
when a response can still start within the budget. A started response is never
replaced by a second error body. An exhausted/disconnected request withholds
further ASGI messages; the hosting server owns transport closure.

The [ASGI specification](https://asgi.readthedocs.io/en/stable/specs/www.html)
defines body events, disconnect events and send-side connection errors. This
adapter watches both disconnect notification and send failures. Completing ASGI
`send` does not prove that a marketing client received or saved a token/result.
A disconnected or killed submission may already have committed in BigQuery.
Explicit recovery uses the original token; a retired host requires new trusted
server composition with fresh authoritative checkpoints. HTTP never restarts it
or resubmits automatically. Native job cancellation and rollback are not inferred.

## Deadline, ownership and teardown

The overall server-created operation budget starts before request parsing/body
receipt and covers admission, body waiting, worker lock waiting, cloud work and
response delivery. HTTP waiting polls cancellation within 20 milliseconds and
does not renew the deadline. Trusted `copy_context` transfers the existing budget
into an owned, bounded supervisor thread pool; providers still execute in the
owned process. The pool cannot be starved by unrelated default asyncio executor
work. Request admission caps active body/host waits at 4 by default (1–32 allowed),
with one event loop owning each application instance. Capacity rejection does not
cancel another owner's operation.

On disconnect/request cancellation, the server-owned signal reaches the host.
The request joins its supervisor work and drains its disconnect watcher before
returning, withholding late success. A cancelled lock waiter cannot retire the
active owner's worker. `aclose()` explicitly stops admission, signals admitted
requests, drains body/host waits, closes the owned host and shuts down the thread
pool. It returns the host's cleanup confirmation; `False` does not reopen admission
or authorize replacement. Cleanup waits resist repeated task cancellation.

ASGI receive/send must be cooperative asynchronous server methods, with normal
event-loop/OS scheduling. This is not a real-time guarantee for a blocked event
loop or noncooperative server coroutine. Startup process creation stays outside
request deadlines. Hosting-server shutdown, container signal handling, ingress
restrictions, multi-replica operation and deployed IAP remain unverified. No local
diagnostic substitutes for BigQuery access, receipt or checkpoint authority.

## Audit boundary

`ApiDeliveryObservation` contains only server request ID, fixed operation, whether
the host was requested, ASGI response completion, attempted HTTP status and a safe
code. A bounded 256-entry server-only diagnostic deque retains these values; it
has no HTTP retrieval route. It contains no actor, raw assertion/token, resource
payload, access decision or commit proof. `worker_requested` does not prove SQL
dispatch; `response_completed` does not prove client receipt.

These observations are **not durable authorization/domain audits**. Atomic sharing
audits remain owned by the existing transaction. Fresh captured access/service
audit producers and durable publication remain **4.5–4.6**. General protected
response/audit linkage is now implemented in the [4.4 read boundary](WORKFLOW_RUNTIME_GATES.md).
This API still must not fabricate an actor or successful mutation from HTTP status.

## Verification

All **795 affected offline tests passed**, including **31 new API tests** in
[test_sharing_api.py](../tests/test_sharing_api.py). Seven API cases use actual
prestarted local subprocesses with real SDK objects and mocked cloud calls.
One uses ephemeral real ES256 signatures and the real IAP verifier; its public
certificate fixture is local, and no private key is saved. The tests cover:

- Disabled defaults/no implicit startup, seven route/result projections, strict
  JSON/header/body/origin handling and ignored browser roles/deadlines.
- Uniform denial/private-error redaction, minimal no-cache responses, bounded
  admission, inherited budgets and withheld late/blocked responses.
- Disconnect before dispatch/during work/during response, request cancellation,
  body-wait and active-worker teardown, supervisor joining and waiter isolation.
- Duplicate concurrent HTTP submission with one original fixture job, lost
  committed reply and explicit original-job recovery without a second insert.
- Current disabled-membership denial, valid/tampered real IAP signatures and
  tampering rejection before any mocked BigQuery read.

```bash
.venv/bin/python -m unittest tests.test_sharing_api tests.test_sharing_operation_host tests.test_operation_transport tests.test_operation_budget tests.test_sharing_runtime_integration tests.test_sharing_runtime_executor tests.test_sharing_runtime tests.test_signing_bootstrap tests.test_signing_bootstrap_integration tests.test_signing_control_writer tests.test_signing_write_integration tests.test_signing_storage_io tests.test_signing_read_integration tests.test_sharing_publication_storage tests.test_sharing_publication tests.test_workflow_schema tests.test_setup_workflow_tables tests.test_sharing_secret_loader tests.test_sharing_signing tests.test_sharing_retry tests.test_sharing_retry_integration tests.test_sharing_retry_executor tests.test_sharing_integration tests.test_sharing_executor tests.test_sharing_receipt tests.test_sharing_query tests.test_sharing_coordinator_contract tests.test_access_reader tests.test_identity
```

Syntax, dependency consistency and changed documentation links were checked.
The 31 API tests were rerun and passed after the final post-wait deadline check;
the broader 795-test run was not repeated for that isolated tightening.
No full suite, native cloud probe, live ADC discovery, secret read, business-data
write, billable model call, IAM change, service enablement, deployment, UI change
or serving activation occurred. Tests use an in-process ASGI harness, not a live
HTTP server; native transactions and deployed identity/teardown are not claimed.
An initial response-race test was corrected to accept cancellation before either
the start event or final body, while requiring that no final body be released.

## Fixed next milestone

The remaining 4.4 work recorded at this slice is now complete within the
[final runtime-gate implementation and verification](WORKFLOW_RUNTIME_GATES.md).
Continue with **4.5 — Critical / Must have — shared pilot**, fresh evidence/audit
producers; then **4.6 — Critical / Must have**, durable workflow/result/artifact
writers and owned model binding; then **4.7 — High / Must have**, protected
submission/history/UI reconnect. Company identity activation remains priority 12.
The [fixed IDs](IMPLEMENTATION_PLAN.md#fixed-remaining-priority-4-milestones) are unchanged.
