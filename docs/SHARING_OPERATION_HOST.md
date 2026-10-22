# Sharing worker deadline enforcement

Implemented under **4.4 — Critical / Must have — shared pilot** after the
[provider/ADC timeout slice](OPERATION_TRANSPORT.md). This completes the opt-in
local host/worker enforcement and bounded cleanup slice. The final
[4.4 runtime gates](WORKFLOW_RUNTIME_GATES.md) complete protected reads, management
and admission/dispatch within implementation/isolated verification scope.
Priority 4 is **4/7 complete, 3/7 remaining**. Serving deployment stays deferred.

## Delivered boundary

[SharingOperationHost](../src/application/sharing_operation_host.py) owns one
prestarted POSIX worker and admits one operation at a time. Host, runtime and
executor settings must all explicitly enable execution. Configuration is typed,
immutable through public properties, and supplied by trusted server composition.
No browser setting can select a worker executable, method, identity or deadline.

`start()` is an explicit server prewarm step. It launches the fixed installed
Python module without a shell, passes only an owned local socket descriptor,
and checks a correlated readiness handshake. No ADC discovery, secret read or
native query occurs during prewarm. Request methods never start/restart a worker
implicitly. Python documents that [initial process creation cannot always be
interrupted](https://docs.python.org/3.13/library/subprocess.html#subprocess.run);
startup therefore remains outside the request completion guarantee. Startup
handshake waits are bounded and a failed startup retires the host.

The seven supported methods are intent issuance, submission, original-job recovery,
retry issuance/submission/recovery and receipt recovery. The
[worker](../src/application/sharing_operation_worker.py) composes the existing
`RuntimeProtectedSharingExecutor`, real IAP verifier, authoritative BigQuery readers,
runtime sealer and pinned Secret Manager loader. It uses the opt-in budget-aware
ADC clients. Construction/discovery occurs inside the first operation's inherited
deadline; clients and runtime are owned and cleaned up in the worker. Failed
partial composition closes its own resources instead of accumulating clients.

Each admitted call inherits the existing parent budget and cancellation signals.
The worker receives a same-host monotonic deadline reduced by the cleanup reserve;
nested scopes cannot renew it. Cancellation remains parent-owned. Polling checks
it at most every 20 milliseconds while communicating; waiting callers use the
existing bounded lock. A cancelled waiter sends no command and cannot retire an
active owner's worker. Successful calls keep the same serial runtime and validated
signing revision floor; a timely application denial also preserves that state.

Cleanup reserves `SharingHostSettings.cleanup_seconds` from the existing operation
budget: default 0.5 seconds, permitted range 0.1–2 seconds. Consequently productive
work stops slightly before the overall limit; the timeout is not silently extended
for cleanup. The supervisor closes IPC, sends TERM, then KILL to its owned process
group, and bounds the reap wait. The leader is not reaped before the final group
signal, preventing PID reuse between signals. Repeated close never signals a
reaped group again. Ordinary same-group credential helpers receive termination too.
No provider `close()` or SDK lock is invoked in the parent request thread.

An ignored RPC/credential timeout, partial IPC message, worker crash, corrupt reply
or active cancellation permanently retires that host. It cannot restart, resubmit,
mint a replacement job ID or return a late success. A stop during final scope exit
also withholds the result and retires the touched host. New operations fail closed.
`close()` first signals shutdown and waits only within the cleanup limit. It returns
`False` if cleanup cannot yet be confirmed; `cleanup_complete` reports whether the
direct worker has been reaped. The retained process handle allows explicit cleanup
inspection; an incomplete cleanup never enables another operation.

## Evidence, access and recovery

IPC uses [bounded JSON frames](../src/application/operation_channel.py), not pickle.
Frames are limited to 64 KiB, reject duplicate keys/nonfinite values, use
nonblocking partial reads/writes and bind replies to a unique operation ID.
[Request/result projections](../src/application/sharing_host_contract.py) restrict
methods, headers, parameter shapes and typed minimal results. No full source rows,
key bytes, permission grants or unverified identity objects cross the response
boundary. Opaque assertions/tokens travel only over the owned local socket;
they are not command-line arguments, files or logs. Worker stdout/stderr are
discarded, and provider/unknown failures become safe codes.

These projections and process termination are not access grants or commit proof.
The existing executor still authenticates and rereads current authorization,
signing policy, eligibility controls and correlated native outcomes. SQL settings,
sealed windows, retry limits and original job IDs remain unchanged. A killed
submission may already have committed remotely; killing the worker does not cancel
the BigQuery job or prove rollback. Recovery requires explicit trusted composition
of a new host, the original sealed token, and fresh authoritative checkpoint/access
checks. Recovery never automatically submits another mutation. UNKNOWN stays UNKNOWN.

Floors are retained in the live serial worker, not exported as an authoritative
local checkpoint. A retired host cannot reuse that runtime. Explicit new
composition follows the existing cold-start BigQuery checkpoint rules; no local
file, diagnostic or test receipt substitutes for authoritative state.

## Verification

All **764 affected offline tests passed**, including **32 new tests** in
[test_sharing_operation_host.py](../tests/test_sharing_operation_host.py). Twenty-four
host/process tests run real local subprocesses; eight cover settings, framing,
deadline inheritance and result contracts. Process fixtures compose actual SDK
objects, runtime and executor with mocked cloud/IAP methods. They verify:

- Default denial and explicit prestart; production-worker handshake with an invalid
  ADC path still succeeds because startup performs no ADC work.
- Ignored timeouts, partial messages, late success, cancellation, bounded close,
  blocked cleanup, TERM-to-KILL escalation and ordinary helper termination.
- Cancelled waiter isolation, serial concurrent callers, late scope-exit denial,
  same-worker higher-floor retention and rejection of a later store rollback.
- One stable fixture submission, hung committed-response loss, terminal-host denial
  and explicit original-job read-only recovery without a second submission.
- Protocol corruption, private error redaction, size/shape checks and all seven
  output projection types through JSON.

```bash
.venv/bin/python -m unittest tests.test_sharing_operation_host tests.test_operation_transport tests.test_operation_budget tests.test_sharing_runtime_integration tests.test_sharing_runtime_executor tests.test_sharing_runtime tests.test_signing_bootstrap tests.test_signing_bootstrap_integration tests.test_signing_control_writer tests.test_signing_write_integration tests.test_signing_storage_io tests.test_signing_read_integration tests.test_sharing_publication_storage tests.test_sharing_publication tests.test_workflow_schema tests.test_setup_workflow_tables tests.test_sharing_secret_loader tests.test_sharing_signing tests.test_sharing_retry tests.test_sharing_retry_integration tests.test_sharing_retry_executor tests.test_sharing_integration tests.test_sharing_executor tests.test_sharing_receipt tests.test_sharing_query tests.test_sharing_coordinator_contract tests.test_access_reader tests.test_identity
```

Syntax, dependencies and documentation links were checked. No full suite, native
cloud probe, live ADC discovery, cloud/resource/data write, secret read, model call,
IAM/API change, UI change, deployment or default activation occurred. Temporary
fixture files record only synthetic process/job observations for test assertions;
they are not serving recovery data. An initial fixture clock mismatch was corrected
to use the same native monotonic clock. A sandbox-blocked OS process-list assertion
was replaced with a termination receipt from the owned helper.

## Limits and fixed next work

Enforcement covers admitted requests in a live prestarted POSIX supervisor, with
normal OS scheduling. It is not a real-time OS guarantee, process-creation deadline
or security sandbox. Kernel signal/reap failures remain explicit incomplete cleanup;
detached helper groups, supervisor crashes and deployed container teardown have not
been verified. The worker does not implement durable cancellation or audits. The
existing unwrapped runtime, diagnostics and legacy client cleanup remain cooperative;
they do not gain process isolation automatically. Shared serving must explicitly
compose this host and pass server-owned cancellation on disconnect/shutdown.

The subsequent [sharing HTTP slice](SHARING_API.md) now connects seven opt-in
ASGI sharing routes, strict projections, bounded admission and disconnect/teardown
handling, verified with 795 affected offline tests. It remains disabled; deployed
HTTP/IAP and durable audit producers/writers are not claimed.

The remaining 4.4 work recorded at this slice is now complete within the
[final runtime-gate implementation and verification](WORKFLOW_RUNTIME_GATES.md).
Continue with **4.5 — Critical / Must have — shared pilot**, fresh evidence/audit
producers; then **4.6 — Critical / Must have**, durable workflow/result/artifact
writers and owned model binding; then **4.7 — High / Must have**, protected
submission/history/UI reconnect. Company identity activation remains priority 12.
The [fixed IDs](IMPLEMENTATION_PLAN.md#fixed-remaining-priority-4-milestones) are unchanged.
