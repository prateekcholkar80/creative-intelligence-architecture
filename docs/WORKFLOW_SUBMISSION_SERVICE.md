# Priority 4.7 — protected submission service

Implemented on 2026-10-11 as **High / Must have — shared pilot**, batches 1–9 of
the fixed Priority 4.7 milestone.

## Implemented boundary

[`workflow_submission_service.py`](../src/application/workflow_submission_service.py)
provides one default-disabled server method for starting a campaign workflow. It:

1. rejects writes before authentication when submission is disabled;
2. verifies the current IAP identity without a personal-account or role fallback;
3. reads workspace, matching membership and conversation together from BigQuery;
4. validates ACTIVE state, exact issuer/subject binding, CREATOR permission and
   conversation ownership;
5. derives stable request, job, intent, session, trace and audit identities from
   the verified principal and caller retry key;
6. creates exact destination-schema request/job rows with pinned server model and
   renderer profiles;
7. produces separate access, request-create and job-create audit envelopes;
8. submits the existing typed 4.6 workflow-start plan through the default-disabled
   writer executor using a bounded 512 MiB cumulative script budget; and
9. reauthenticates and rereads access before returning a minimal receipt containing
   only scope IDs, persistence classification, queued state and submission flag.

No model call occurs in this boundary. `UNKNOWN` and `NOT_COMMITTED` never claim a
queued job. Browser input cannot provide an owner, principal, role, profile,
renderer, workflow row, audit row or native job ID.

## Owned host and worker

[`workflow_submission_host.py`](../src/application/workflow_submission_host.py)
and
[`workflow_submission_worker.py`](../src/application/workflow_submission_worker.py)
add the default-disabled process boundary for the submission service. The host:

1. accepts only one fixed `submit` operation and exact bounded fields;
2. transfers only headers, workspace/conversation IDs, canonical preferences and
   a retry key to a prestarted owned worker;
3. derives the monotonic deadline in the trusted parent and propagates cancellation;
4. validates the minimal receipt and exact response scope before returning it; and
5. permanently retires a timed-out or uncertain worker without restarting or
   replaying a potentially committed mutation.

The child constructs ADC-backed SDK clients only after explicit host startup and
composes the protected service from server configuration. It has no model client,
browser role/profile input or recovery fallback.

## Protected HTTP route

[`workflow_submission_api.py`](../src/application/workflow_submission_api.py)
exposes only `POST /v1/submissions/submit` over the prestarted host. It reuses the
shared bounded ASGI lifecycle for exact HTTPS-origin checks, IAP assertion
forwarding, JSON framing, admission limits, disconnect cancellation, joined worker
cleanup and no-store responses. Browser owner, role, model, renderer, deadline and
writer fields are rejected. Authorization details are reduced to
`403 ACCESS_DENIED`; host/provider failures are reduced to
`503 SERVICE_UNAVAILABLE`. The API never starts or replaces a worker.

## Authoritative history and cold reconnect

[`workflow_history_service.py`](../src/application/workflow_history_service.py)
adds a separate default-disabled BigQuery reader. Each page authenticates the IAP
assertion and reads the current workspace, exact membership, ACTIVE owned
conversation, jobs and their request parents in one bounded read transaction. Full
request rows are used only to validate the existing job contract and are never
returned.

Pages use `(created_at, job_id)` keyset ordering and fetch at most 51 records. The
HMAC cursor pins the verified principal, workspace, conversation, original page
snapshot and last key, expires within a configured interval and requires canonical
base64url encoding. New jobs after the first-page snapshot do not enter later pages.
The response contains only current conversation presentation metadata and minimal
job status fields needed to restore the UI. Archived conversations, revoked or
mismatched memberships, foreign owners, duplicate parents, malformed jobs,
unordered rows and tampered/cross-scope cursors fail closed.

The cursor key has no default and must come from trusted runtime configuration.
Host/API composition and managed key loading are not part of this batch. BigQuery
is the only history source; no local cache or JSON fallback is used.

[`workflow_history_host.py`](../src/application/workflow_history_host.py) and
[`workflow_history_worker.py`](../src/application/workflow_history_worker.py) now
place that reader behind the same prestarted owned-process supervisor. Only the
fixed `list_jobs` operation crosses IPC. The host bounds IDs, page size and cursor,
encodes the cursor key only inside trusted local configuration, validates every
minimal result field and timestamp, preserves descending key order and checks the
returned scope/page size. The worker constructs server-side ADC clients lazily and
has no model, write or local-history path. Timed-out or uncertain workers retire
without automatic restart. The protected history HTTP route remains separate.

## Idempotency and recovery limit

The authenticated principal, workspace, conversation and retry key determine the
stable request and workflow-job IDs. A bounded in-process cache retains the exact
short-lived plan so an immediate duplicate can recover the same deterministic
native job. Changed preferences under the same retry key fail as a conflict before
another writer submission.

This cache is coordination state, not a source of truth. After a process restart,
the service must use the forthcoming BigQuery history/reconnect reader. It must not
reconstruct a new timestamped plan or treat a missing local cache as permission to
submit again. BigQuery remains authoritative.

## Verification

The affected submission, ASGI, host/supervisor, operation transport,
workflow-start, writer-executor, workflow-contract, access-policy and
audit-producer slice passes **187 tests**. Coverage includes:

- disabled-default denial before identity or database access;
- exact queued request/job construction and minimal receipt projection;
- stable same-key plan reuse and changed-input conflict rejection;
- conservative `UNKNOWN` and `NOT_COMMITTED` receipts;
- non-CREATOR, foreign-owner and changed-identity denial;
- malformed preferences and incomplete trusted settings; and
- proof that no model client is constructed;
- trusted configuration round-trip and disabled-default startup denial;
- a real local subprocess using signed IAP assertions and fake authoritative rows;
- strict request/result projections and response-scope enforcement; and
- deadline expiry with bounded worker retirement and no automatic resubmission;
- exact route/origin/body-size and browser-policy rejection before host work;
- uniform public denial/unavailable errors without private details; and
- HTTP disconnect propagation through a real hung local worker.

The subsequent history, identity, codec, workflow, membership, conversation,
workspace, management and operation-budget slice passes **277 affected tests**,
including eight new history tests. It verifies disabled defaults, empty reconnect,
stable pagination, canonical signed-cursor tamper rejection, current access denial,
parent/job validation, ordering, future snapshots and bounded page sizes.

The history host/worker plus supervisor, transport and those underlying contract
slices pass **341 affected tests**, including six new owned-process tests. The new
coverage verifies secret-preserving configuration round-trip, real signed-IAP
pagination in one subprocess, typed timestamp reconstruction, tamper denial without
worker reset, pre-worker input rejection, result-scope enforcement and terminal
cleanup after a hung read.

[`workflow_history_api.py`](../src/application/workflow_history_api.py) now exposes
only `POST /v1/history/list_jobs` over the prestarted history host. It applies the
same exact-origin, bounded-body, admission, parent-deadline, disconnect and joined
teardown lifecycle as the submission route. Only the IAP assertion reaches the
worker. Typed timestamps are serialized to ISO 8601, response caching is disabled,
and private failures are reduced to uniform `403 ACCESS_DENIED` or
`503 SERVICE_UNAVAILABLE` responses. HTTP input cannot start, replace or configure
the host, key or worker.

The combined history API/host/service, shared ASGI/supervisor/transport,
submission-API regression and underlying identity/workflow/access contract slices
pass **385 affected tests**, including six new history-HTTP tests. They verify real
signed-IAP two-page reconnect, minimal timestamp serialization, pre-host origin,
route, size, page and browser-policy rejection, safe errors, foreign-identity
denial, disabled admission and joined retirement after a disconnected hung read.

[`workflow_session.py`](../src/ui/workflow_session.py) now defines the pure durable
chat state boundary. It consumes only validated submission receipts and history
pages, keeps uncertain commits distinct from queued jobs, reuses identical receipts,
rejects conflicting IDs, cross-scope pages, wrong cursors, overlaps, unordered pages
and revision regression, and produces a minimal rendering projection. The existing
Streamlit UI initializes this state behind the exact
`CI_DURABLE_WORKFLOW_UI_ENABLED=true` opt-in. Default local behavior remains
session-only. Opting in before authenticated runtime composition now disables
catalog, interpretation and generation and shows a safe unavailable state rather
than falling through to direct local execution.

The affected workflow-state, Streamlit, local chat and protected
submission/history service/host/API slices pass **74 tests**, including six new
state/UI tests. In batch 7, no durable API client or runtime host was composed into
Streamlit, so no shared writes or reconnect reads were activated.

[`workflow_runtime.py`](../src/ui/workflow_runtime.py) now supplies the server-only
Streamlit bridge for batch 8. Trusted bootstrap may install exactly one runtime
made from two already-started typed submission/history hosts and a fixed
workspace/conversation/page-size scope. Request handling copies only one IAP JWT
assertion from `st.context.headers`; cookies, email headers and browser-selected
scope or policy never reach either worker. History pages and submission receipts
flow through the pure state adapter. Host/provider failures become safe runtime
codes, while `UNKNOWN` receipts remain visible without claiming a queued job.

When durable mode is explicitly enabled and that runtime is installed, the chat
restores the first authoritative BigQuery history page before accepting input,
supports cursor-bound older-page loading, offers an explicit history refresh and sends reviewed preferences through the
protected submission host. It does not call the local synchronous generator for
that submission. Missing identity, unavailable/retired hosts, denied access and
invalid results fail closed. The runtime does not start, replace or close workers.
The default personal-account local path remains unchanged, and no runtime is
installed by the current `app.py` entry point.

The combined UI runtime/state/chat and protected submission/history
service/host/API slice passes **82 affected tests**, including eight new runtime/UI
tests. They cover exact assertion isolation, duplicate-header rejection, trusted
scope, prestarted-host enforcement, one-time installation, authoritative history
restore, uncertain receipt rendering, safe error reduction and durable submission
routing without a local generation call.

Batch 9 connects the existing prestarted protected-read host to the same installed
runtime. Job refresh rereads current authoritative status. A successful status is
followed by a separately authorized output-summary read, and the runtime rejects
job/output/SKU/conversation mismatches. Nonterminal and failed jobs never request
an output summary.

Output summaries now include a bounded `downloads` projection containing only
artifact ID, kind, safe filename, MIME type, byte size and SHA-256. GCS URI,
generation, owner, principal and full output payload remain inside the protected
reader. The host validates every descriptor and the UI offers only those
server-returned banner/brief downloads. Streamlit uses deferred download callables:
no artifact bytes are read while rendering the page. A click invokes the protected
worker, which retains its existing fresh access/manifest rereads and pinned
generation/hash/size/format checks before returning bytes. The runtime also binds
the returned filename, MIME type, hash, size, job and output metadata back to the
selected descriptor.

The complete affected UI, submission/history/read host/API, access-reader and
protected-download slice passes **177 tests**. Coverage includes nonterminal status,
successful output binding, safe descriptor projection, deferred byte reads,
descriptor mismatch rejection, cross-user and revoked-access denial, corrupt or
changed artifacts, large bounded IPC downloads, disconnect cleanup and unchanged
local UI behavior. No serving runtime was installed and no cloud or model call ran.

Python compilation and whitespace checks pass. The subprocess fixture uses mocked
authoritative rows and writer outcomes; no ADC, BigQuery, GCS or model call ran.
No serving rows, products, approvals, vectors, artifacts, IAM settings or defaults
changed.

## Closing verification

[`test_workflow_ui_integration.py`](../tests/test_workflow_ui_integration.py)
closes the remaining **High / Must have — shared pilot** verification gap through
one shared authoritative fixture behind the three typed host contracts. Real ES256
IAP assertions bind the owner across the runtime flow. The tests verify:

1. stable duplicate submission identity without a second UI submission record;
2. cursor-bound history pagination, cold reconnect and current-page refresh;
3. successful status/output binding and deferred download with zero render-time
   byte reads;
4. uniform denial for a foreign principal and revoked membership; and
5. denial before byte release when the authoritative artifact manifest changes.

The full 4.7 affected slice passes **179 tests**. Python compilation and whitespace
checks pass. The closing tests use only in-process authoritative fixtures; no ADC,
BigQuery, GCS, model, IAM or serving write occurred. Priority 4.7 is complete within
its default-disabled implementation and offline/owned-process verification scope.
The next fixed delivery item is **Priority 5 — Critical / Must have — shared pilot:
prompt library and core guardrails**.

Each implementation batch remains limited to two or three files. Company Workspace
domain/group setup and deployed IAP activation remain deferred to priority 12.
