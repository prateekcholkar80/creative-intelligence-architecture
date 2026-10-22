# Serialized signing runtime

Implemented 2026-10-11 under **4.3 — Critical / Must have — shared pilot** in
[sharing_runtime.py](../src/application/sharing_runtime.py). This is the runtime
sealer slice of the existing milestone. The subsequent
[native probe](SHARING_RUNTIME_INTEGRATION.md) passed all 14 cases and verified
owned cleanup. **4.3 is complete within its isolated scope; Priority 4 is
3/7 complete, 4/7 remaining. Next: 4.4.** No baseline IDs or release gates changed.

## Behavior and boundaries

`RuntimeSharingSealer` implements the existing initial/retry sealer interface.
Trusted server composition supplies the authoritative `BigQuerySigningReader`,
regional `SharingSecretLoader`, explicit consumer/project-number pins and a new
process instance ID. `SigningRuntimeSettings.enabled` defaults to false. The
runtime does not create clients, authenticate users, publish controls, provision
secrets or alter executor defaults. Caller-owned SDK transports stay caller-owned.

One process-local lock spans the complete read/load/install/sign-or-resolve/result
operation. A refresh invalidates readiness and releases the prior sealer reference
before calling the reader. Every public sealer call, including settings lookup,
performs a new bounded native BigQuery read and fresh all-or-nothing access to the
exact nonrevoked Secret Manager versions. The runtime has no cached-success or
local-file fallback. It returns tokens, validated attempts, settings or immutable
diagnostic state; it never returns the raw loaded sealer or key material.

A cold process accepts its checkpoint anchor only from the authoritative reader.
The complete chain is validated against its in-process revision/digest floor. The
consumer's authoritative checkpoint must equal the current head before keys can
load: a missing or lagging checkpoint denies signing until the separate authorized
[checkpoint writer](SHARING_PUBLICATION_WRITER.md) advances it. The runtime does
not write that checkpoint automatically or infer writer authority. This trades
availability during rotation for an explicit durable floor before readiness.
Deletion or administrative rollback of authoritative controls is outside this
process lock and still requires deployment IAM/resource protection.

A valid higher observed head is pinned before loading. Read/load/profile failures,
expired leases, invalid tokens and operation failures remove runtime key references
and deny use while retaining the last validated head. STOPPED retains its revoked
history, returns no load ticket and loads no saved stopped configuration. A later
fresh read/load may recover; close permanently disables the instance. Reference
removal does not claim Python memory zeroization or revoke independently retained
objects created outside this runtime.

Installation binds the actual pinned loader result to the exact policy. Readiness
uses the original authority timestamp and active issuance boundary. An independent
monotonic bound begins before the read, preventing backward wall-clock adjustments
from extending the maximum window. Signing/resolution checks readiness both before
use and before releasing its result. No late load can extend that original lease.
Concurrent refreshes cannot replace state while another operation owns the lock;
reentrant callbacks fail closed.

Planned rotations preserve original key/profile cohorts and deterministic job IDs
for retained initial and nested retry tokens. Revocation skips the revoked secret
version and rejects its previously valid tokens. Changed settings between settings
lookup and initial sealing reject the stale query rather than reinterpreting it.

## Remaining integration limits

The [runtime executor adapter](SHARING_RUNTIME_EXECUTOR.md) now composes fresh
authority/access checks at dispatch and result release, with serialized operation
leases and 613 affected offline tests passing. The plain executor rejects runtime
composition without the required hooks. Neither adapter is wired into a serving
HTTP route or UI. A resolved attempt alone is not a later dispatch grant. Real
resource checks passed in the isolated 4.3 probe. No instantaneous cross-replica or in-flight
revocation is claimed.
No background refresh scheduler or unbounded automatic retries are introduced.

The supplied numeric project number must be independently verified against the
project ID; the runtime does not establish that association. The native probe now
verifies that binding, owned regional pins, actual ADC Secret Manager access and
restart/rotation/stop behavior. Least-privilege deployed service-account permissions,
serving pins and controlled deployment composition remain unverified. Offline fixtures
use a deliberately fictitious project number and anonymous clients.

SDK RPC/query limits are inherited from the reader and loader. The
[4.4 cooperative budget](OPERATION_BUDGET.md) now bounds runtime lock acquisition
and checks cumulative operation time/cancellation at boundaries. The
[provider/ADC slice](OPERATION_TRANSPORT.md) and [host/worker slice](SHARING_OPERATION_HOST.md)
now propagate timeouts and enforce admitted request/cleanup limits in opt-in
prestarted workers; 764 affected offline tests pass. Unwrapped sealer calls stay
cooperative; startup and deployed teardown remain separate/unverified. Each public sealer call incurs
one fresh read and up to eight pinned key-access RPCs. Settings lookup followed by
sealing therefore performs two compositions. Future optimizations must retain
fresh authority, exact pins and failure invalidation; no cache shortcut is enabled.

## Verification

All **575 affected offline tests passed**, including **29 new runtime tests** in
[test_sharing_runtime.py](../tests/test_sharing_runtime.py):

```bash
.venv/bin/python -m unittest tests.test_sharing_runtime tests.test_signing_bootstrap tests.test_signing_bootstrap_integration tests.test_signing_control_writer tests.test_signing_write_integration tests.test_signing_storage_io tests.test_signing_read_integration tests.test_sharing_publication_storage tests.test_sharing_publication tests.test_workflow_schema tests.test_setup_workflow_tables tests.test_sharing_secret_loader tests.test_sharing_signing tests.test_sharing_retry tests.test_sharing_retry_integration tests.test_sharing_retry_executor tests.test_sharing_integration tests.test_sharing_executor tests.test_sharing_receipt tests.test_sharing_query tests.test_sharing_coordinator_contract tests.test_access_reader tests.test_identity
```

Tests compose real SDK clients/messages/jobs with mocked RPCs and real sealers.
They cover fresh read/load per call, disabled settings, scope checks, missing and
lagging checkpoints, cold restart above genesis, higher floors after partial load
failure, rollback rejection, STOPPED, revocation, planned rotation, original and
nested retries, profile drift, settings races, lease expiry, concealed elapsed time,
result-release denial, safe errors, close, reentrancy and two concurrently queued
refreshes observing successive heads. No live credentials or provider RPCs ran.

Python syntax and local documentation links were checked; `pip check` reported
no broken requirements. The full suite and native probes were not rerun for this
offline slice. No cloud data/resource writes, secret access/provisioning, IAM,
users, approvals, model calls, UI changes or default activation occurred.

## Next work within the fixed plan

1. **4.4 — Critical / Must have — shared pilot:** Connect protected response/audit
   endpoints and disconnect/cancellation handling through the verified server boundary,
   preserving original-job recovery when expiry occurs after dispatch.
2. **4.4 — Critical / Must have — shared pilot:** Continue protected response/audit,
   management and generation admission/dispatch gates after that runtime protection.
   Evidence producers, writers and UI reconnect remain the existing **4.5–4.7**.

See the [fixed baseline](IMPLEMENTATION_PLAN.md#fixed-remaining-priority-4-milestones).
