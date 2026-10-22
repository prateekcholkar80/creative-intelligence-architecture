# Runtime sharing executor gates

Implemented 2026-10-11 under **4.3 — Critical / Must have — shared pilot** in
[sharing_runtime_executor.py](../src/application/sharing_runtime_executor.py),
with hooks in [sharing_executor.py](../src/application/sharing_executor.py)
and serialized gates in [sharing_runtime.py](../src/application/sharing_runtime.py).
This completed the offline executor-gate slice of 4.3. The subsequent
[native runtime verification](SHARING_RUNTIME_INTEGRATION.md) passed all 14 cases
with owned cleanup. **4.3 is complete within its isolated scope; Priority 4 is
3/7 complete, 4/7 remaining. Next: 4.4.**

## Dispatch and release behavior

`RuntimeProtectedSharingExecutor` requires an actual `RuntimeSharingSealer` whose
metadata reader uses the same scoped BigQuery client as the access reader and
mutation executor. Both execution settings and runtime settings default disabled.
The plain executor rejects runtime-sealer composition unless both dispatch and
release hooks are overridden; its existing injected-key fixture paths retain
their behavior. This prevents accidentally using the runtime with no gates.
Trusted Python composition is still required; overridden methods or typed objects
are not proofs of a correct deployment, authentication or current authority.

Every public method authenticates request headers and resolves the signed initial
or retry token before using its attempt, except initial issuance, which constructs
the command from the verified actor. The new retained-sealer `check_attempt`
validates a previously resolved server attempt against its current identity,
acceptance cohort, key revocation/retention, installed profile and exact original
query settings. It does not verify a token signature or grant access by itself.
No HTTP endpoint accepts a caller-supplied typed attempt or authorization callback.

Dispatch enters the runtime's serialized `attempt_gate` after preflight. It performs
a fresh authoritative metadata read, requires a checkpoint equal to the head,
loads the pinned nonrevoked key versions, checks the exact attempt, rechecks
current conversation access and checks readiness again after that access I/O.
No external operation separates successful gate entry from the native jobs.insert
call. The runtime lock remains held through submission, original-job reconciliation
and subsequent authorization. No raw sealer or key bytes are exposed to the executor.

Gate exit first checks the original wall/monotonic lease. If provider I/O crossed
that lease or active issuance boundary, a refresh cannot erase the expiry. Otherwise
it refreshes authority/material again, revalidates the cohort, rereads access after
loading and checks readiness before allowing exit. Protected result release also
performs a fresh policy/load/attempt check, current access check and readiness check.
Initial issuance, retry issuance, initial/retry submit, initial/retry recovery and
receipt recovery all use this exit hook, including early known-job, REUSED and
UNKNOWN returns.

STOPPED, revoked/unavailable original keys, stale/missing checkpoints, incompatible
settings, read/load failures, access removal and lease expiry deny use or result
release. Failures remove key references while retaining the validated revision
floor. Safe identity/access exceptions retain their existing codes. Cancellation
exceptions invalidate runtime references and propagate; this does not implement
overall deadlines or remote job cancellation.

## Recovery and rotation

The SQL, original seven-day envelopes, native job IDs, bounded positive ancestor
rollback checks, receipt semantics and retry limits are unchanged. The runtime
checks do not create new mutation attempts or select a replacement ID.

A denial after jobs.insert can occur after the job committed. It withholds the
response and does **not** turn the commit into NOT_COMMITTED, undo the transaction
or authorize retry. Original-job recovery remains read-only and uses the same token
and ID when current signing policy and application access permit it. Missing or
uncertain native metadata stays UNKNOWN. Positive correlated COMMIT/ROLLBACK child
evidence retains its existing meaning.

Forward publications retain the exact original cohort/settings; a compatible
rotation does not remap tokens or retry IDs. Crossing the old active issuance
boundary invalidates the in-flight lease and withholds the result. A later fresh
authorized recovery can resolve the retained original token under the new active
policy and return the original job outcome. Revoked tokens and STOPPED policies
remain denied, including historical receipt recovery through this adapter.

## Verification

All **613 affected offline tests passed**, including **38 new executor-gate tests**
in [test_sharing_runtime_executor.py](../tests/test_sharing_runtime_executor.py):

```bash
.venv/bin/python -m unittest tests.test_sharing_runtime_executor tests.test_sharing_runtime tests.test_signing_bootstrap tests.test_signing_bootstrap_integration tests.test_signing_control_writer tests.test_signing_write_integration tests.test_signing_storage_io tests.test_signing_read_integration tests.test_sharing_publication_storage tests.test_sharing_publication tests.test_workflow_schema tests.test_setup_workflow_tables tests.test_sharing_secret_loader tests.test_sharing_signing tests.test_sharing_retry tests.test_sharing_retry_integration tests.test_sharing_retry_executor tests.test_sharing_integration tests.test_sharing_executor tests.test_sharing_receipt tests.test_sharing_query tests.test_sharing_coordinator_contract tests.test_access_reader tests.test_identity
```

Tests use actual SDK clients/messages/jobs, actual sealers, mocked provider RPCs,
fresh access-reader fixtures and a real ES256 IAP assertion case. They verify:

- Both disabled defaults and rejected incomplete/foreign-client composition.
- Dispatch and release reads, preflight stop/revocation/checkpoint denial, and
  access removal during preflight or Secret Manager loading.
- Stop/revocation/access removal and lease expiry after native submission; committed
  fixture jobs remain recoverable without resubmission when policy/access permit.
- Rotation across the issuance boundary, lost responses, UNKNOWN preservation,
  positive COMMIT classification after parent failure and stable retry dispatch.
- Release gates on every known-job, REUSED, UNKNOWN, issuance and receipt branch.
- Original principal binding, cancellation cleanup, assertion tampering and a
  concurrent refresh blocked while the dispatch gate owns the runtime lock.

Python syntax, local documentation links and `pip check` passed. No full-suite or
native probes ran in this slice. No provider RPCs, cloud row/resource writes,
Secret Manager access/provisioning, IAM/users/approvals, model calls, UI changes,
deployments or default activation occurred.

## Limits and fixed next work

This is a server-only callable adapter, not a serving HTTP/IAP deployment. The
real project-number association and owned regional secret/version resources are
now verified with local ADC by the native probe; deployed service-account IAM and
serving configuration remain unverified. Gate observations and access reads are sequential bounded checks;
the process lock does not lock remote policy or application rows. No instantaneous
cross-replica revocation, atomic policy/dispatch fencing or cancellation of already
submitted native jobs is claimed. Sharing SQL retains its transaction-local
application-access checks. Stronger rollout behavior must be verified in the
deployment/release gates before activation.

There is no success cache: a successful initial submit currently performs four
signing read/load compositions (resolve, dispatch entry, dispatch exit, release),
plus fresh access reads. Each composition may access up to eight pinned key versions.
Inherited RPC/query limits do not bound cumulative work, lock waits or ADC refresh.
The [4.4 cooperative budget slice](OPERATION_BUDGET.md) now bounds runtime lock
waits and checks cumulative time/cancellation at runtime/executor boundaries.
The [provider/ADC slice](OPERATION_TRANSPORT.md) now caps RPCs and adds opt-in HTTP
composition with post-refresh wire checks; 732 affected offline tests pass. The
[host/worker slice](SHARING_OPERATION_HOST.md) now contains ignored timeouts and
bounds cleanup for opt-in prestarted worker requests; 764 affected offline tests
pass. Startup and deployed teardown remain separate/unverified. Unwrapped executor
calls stay cooperative; client transports remain caller-owned.

1. **4.4 — Critical / Must have — shared pilot:** Connect protected response/audit
   endpoints and disconnect/cancellation handling through the verified server boundary.
   Expiry after dispatch preserves original-job recovery and UNKNOWN outcomes.
2. **4.4 — Critical / Must have — shared pilot:** Then connect protected response,
   audit, management and generation admission/dispatch gates; verify duplicate/lost
   responses before billable dispatch. Company identity activation remains deferred.
   Evidence/audit producers, durable writers and UI reconnect remain **4.5–4.7**.

See the [fixed baseline](IMPLEMENTATION_PLAN.md#fixed-remaining-priority-4-milestones).
