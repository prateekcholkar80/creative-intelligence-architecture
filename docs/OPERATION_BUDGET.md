# Cooperative operation budgets

Implemented under **4.4 — Critical / Must have — shared pilot** in
[operation_budget.py](../src/application/operation_budget.py), with runtime and
executor integration. This completes the cooperative budget/boundary slice.
The final [4.4 runtime gates](WORKFLOW_RUNTIME_GATES.md) complete the remaining
implementation/isolated verification scope. Priority 4 is **4/7 complete,
3/7 remaining**; serving identity/deployment remains deferred.

## Implemented behavior

Trusted server configuration sets `SigningRuntimeSettings.operation` to immutable
`OperationSettings`. The default is 120 seconds; integer values from 1 through 600
are accepted. These are configurable host-operation limits, not a measured latency
target. They do not alter signing leases, sealed query settings, native job IDs,
seven-day intent windows or retry limits. Disabled execution defaults remain.

Each public runtime-executor call creates a monotonic budget before authentication.
The seven entry points cover intent/retry issuance, initial/retry submission,
initial/retry recovery and receipt recovery. Nested runtime calls inherit the
parent deadline and cancellation signals; shorter child limits are allowed but
longer limits cannot extend a parent. Refreshing keys cannot renew the operation
budget. A fresh independent recovery call gets a new host budget while retaining
the exact original sealed attempt and job ID.

`CancellationSignal` is a one-way, server-owned event. The executor accepts it as
the optional `cancellation` keyword. It is not a browser request field, application
role, authentication credential or durable cancellation record. Standalone runtime
calls can inherit a trusted `operation_scope`; otherwise each call creates its own
budget. Context is local to execution and does not automatically cross threads;
the subsequent [sharing HTTP adapter](SHARING_API.md) explicitly copies the trusted
context into its owned supervisor pool.

Runtime lock acquisition polls for at most 50 milliseconds per attempt, capped by
remaining time, and checks cancellation after acquisition. A stopped waiter never
invalidates another active lock owner's material. An owned operation failure drops
key references and preserves the validated revision floor. Nonblocking executor
cleanup discards idle material without waiting for or modifying another owner.
Python reference removal does not guarantee memory zeroization.

Checkpoints cover authentication, access authorization, each executor preflight
resource lookup, dispatch, original-job/result reconciliation, child-job outcome
checks, receipts, rollback ancestors, runtime refresh and final response release.
Already validated runtime metadata observations retain their higher floor before
the post-read budget check; expired reads cannot proceed to key loading. Delayed
loads cannot install material or return a usable result. Late provider errors
cannot hide an already stopped budget at the runtime/executor boundary.

Safe stop codes are `OPERATION_CANCELLED` and `OPERATION_DEADLINE_EXCEEDED`.
Generic recovery/store handlers propagate these rather than treating them as
UNKNOWN success or admitting another write. Cancellation exceptions also release
idle references. Stops after jobs.insert withhold responses: they do not prove
rollback, reverse a commit, change a stable ID, authorize a retry or cancel the
remote job. Subsequent authorized recovery continues to read the original job;
missing/uncertain evidence remains UNKNOWN.

## Verification and limits

All **685 affected offline tests passed**, including **46 new tests** across
[test_operation_budget.py](../tests/test_operation_budget.py),
[test_sharing_runtime.py](../tests/test_sharing_runtime.py) and
[test_sharing_runtime_executor.py](../tests/test_sharing_runtime_executor.py).
They cover cumulative timing, nested budgets/signals, exact expiry, scope restoration,
cancellable lock acquisition, an active owner's preserved state, validated-floor
retention, delayed reads/loads/errors, late intent issuance, expiry before dispatch,
committed/uncertain post-dispatch stops, cancelled discarded responses, original-ID
recovery, retry/receipt paths and final scope-exit cleanup. Provider fixtures use
actual SDK objects with mocked RPCs; concurrency tests use actual threads/locks.

```bash
.venv/bin/python -m unittest tests.test_operation_budget tests.test_sharing_runtime_integration tests.test_sharing_runtime_executor tests.test_sharing_runtime tests.test_signing_bootstrap tests.test_signing_bootstrap_integration tests.test_signing_control_writer tests.test_signing_write_integration tests.test_signing_storage_io tests.test_signing_read_integration tests.test_sharing_publication_storage tests.test_sharing_publication tests.test_workflow_schema tests.test_setup_workflow_tables tests.test_sharing_secret_loader tests.test_sharing_signing tests.test_sharing_retry tests.test_sharing_retry_integration tests.test_sharing_retry_executor tests.test_sharing_integration tests.test_sharing_executor tests.test_sharing_receipt tests.test_sharing_query tests.test_sharing_coordinator_contract tests.test_access_reader tests.test_identity
```

Syntax and local documentation links were checked. No full-suite or native probe
ran for this slice. No cloud calls, data writes, model invocations, IAM/API changes,
deployments, UI changes or activation occurred. Prior 4.3 native evidence is
[recorded separately](SHARING_RUNTIME_INTEGRATION.md) and does not verify these new
budget boundaries against deployed transports.

The subsequent [provider/ADC slice](OPERATION_TRANSPORT.md) now propagates remaining
time into RPCs, lazy iteration, certificates and opt-in ADC HTTP transports; all
732 affected offline tests pass. Late provider responses cannot become validated
observations. These remain **cooperative checks**, not a hard host completion
deadline. A provider call ignoring its timeout can block until it returns.
Diagnostic `state` and `close` are not bounded
request paths. No worker is detached, Python thread is killed, or native job is
automatically cancelled. HTTP disconnects/worker termination and durable audit
records are not implemented here.

## Remaining work under the existing 4.4 ID

The subsequent [host/worker slice](SHARING_OPERATION_HOST.md) now contains ignored
timeouts and bounds cleanup for admitted requests in opt-in prestarted workers;
764 affected offline tests pass. Unwrapped runtimes remain cooperative.

The subsequent [sharing HTTP adapter](SHARING_API.md) now connects sharing routes,
bounded admission and disconnect/teardown handling with 795 affected offline tests.

The remaining 4.4 work recorded at this slice is now complete within the
[final runtime-gate implementation and verification](WORKFLOW_RUNTIME_GATES.md).
Continue with **4.5 — Critical / Must have — shared pilot**, fresh evidence/audit
producers; then **4.6 — Critical / Must have**, durable workflow/result/artifact
writers and owned model binding; then **4.7 — High / Must have**, protected
submission/history/UI reconnect. Company identity activation remains priority 12.
The [fixed IDs](IMPLEMENTATION_PLAN.md#fixed-remaining-priority-4-milestones) are unchanged.
