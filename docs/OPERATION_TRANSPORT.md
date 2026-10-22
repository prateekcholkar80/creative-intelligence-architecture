# Provider and ADC timeout propagation

Implemented under **4.4 — Critical / Must have — shared pilot**. This completes
the provider/ADC propagation slice after [cooperative budgets](OPERATION_BUDGET.md).
**4.4 remains in progress; Priority 4 remains 3/7 complete, 4/7 remaining.**

## Behavior and composition

[operation_budget.py](../src/application/operation_budget.py) now caps each RPC
timeout by the existing monotonic budget, checks late success and failure, and
checks before and after lazy iterator advancement. Read retries retain their
legacy configuration outside an operation scope; scoped access reads explicitly
disable SDK retries. Existing sharing calls already disable RPC and job retries.

Propagation covers signing metadata dataset/table preflight, metadata query and
results, workspace/membership/conversation access queries and results, executor
preflight, sharing submission, original/retry job reconciliation, child-job
commit/rollback reads, receipt queries/results, pinned secret reads and IAP
certificate requests. Secret loading also retains its shorter local total/RPC
limits. Checks reject late empty pages as well as late rows. Safe stop exceptions
survive generic identity, access, metadata, secret and recovery error handlers.

A response arriving after expiry is rejected before it can become a validated
signing observation. A higher revision floor already validated before a later
stop remains retained. Stopped loads cannot install partial key material.

Host timeout caps do not change SQL, BigQuery `jobTimeoutMs`, query profiles,
fingerprints, sealed windows, retry limits or stable job IDs. A stop after
submission may conceal a committed mutation. It does not cancel the remote job,
establish rollback or permit another attempt. Fresh authorized recovery reads
the original job; uncertain evidence remains UNKNOWN.

[operation_transport.py](../src/application/operation_transport.py) adds the
explicit server-only `create_runtime_cloud_clients()` factory. It uses ADC with
cloud-platform scopes, the fixed project and asia-south1, and the exact regional
Secret Manager endpoint. The ADC-selected project cannot redirect serving scope.
Returned clients remain caller-owned and must be closed. No serving composition,
default client factory, execution setting, endpoint or UI is activated by this
addition. Both existing runtime/executor execution controls remain disabled.

The opt-in factory provides owned HTTP sessions for credential discovery and
refresh, BigQuery and regional Secret Manager REST. Remaining time is recomputed
at `Session.send`, after credentials have been refreshed, and on SDK polling and
lazy page requests. Socket timeouts have a 20-second upper cap and also respect
shorter supplied provider caps. IAP certificate requests have a five-second cap
and close their owned session after verification. HTTP adapters have zero
transport retries; automatic 401 refresh/replay and redirects are disabled.
Service failures require explicit recovery rather than hidden request replay.

The legacy Secret Manager gRPC factory remains unchanged. Its loader RPC arguments
are capped inside a budget, but its ADC plugin does not gain the HTTP-context
guarantee. Shared composition must explicitly use the new factory for that
guarantee. No guarantee is inferred for arbitrary injected clients.

The generated Secret Manager REST SDK has no public session injection argument.
The opt-in transport subclass replaces its owned `_session` and regenerates
wrapped methods bound to that session. Actual SDK REST calls and loader composition
are tested offline. The loader accepts only the SDK's exact regional gRPC host or
REST HTTPS URL form. Changes to these SDK internals require compatibility checks;
an incompatible factory fails without credential or client fallback.

## Verification

All **732 affected offline tests passed**, including **47 new tests** in this slice:
23 transport tests, 11 budget-helper tests, three added runtime tests, five composed
executor tests, three access tests and two identity tests. One earlier runtime
expectation was corrected to distinguish expired provider responses from already
validated observations. Existing post-dispatch recovery/cancellation tests pass.

Transport tests run actual Google Auth, requests, BigQuery polling/page/insert and
Secret Manager REST code with HTTP adapters mocked. They verify cumulative ADC
time, no service request after expired refresh, no 401/redirect replay, decreasing
poll/page caps, one stable lost-response submission, unchanged native job settings,
regional secret routing, complete loader composition and partial-construction
cleanup. Composed tests also verify access/preflight/secret caps, receipt reads,
child-job outcomes and refusal to issue retry proof after a late child page.

```bash
.venv/bin/python -m unittest tests.test_operation_transport tests.test_operation_budget tests.test_sharing_runtime_integration tests.test_sharing_runtime_executor tests.test_sharing_runtime tests.test_signing_bootstrap tests.test_signing_bootstrap_integration tests.test_signing_control_writer tests.test_signing_write_integration tests.test_signing_storage_io tests.test_signing_read_integration tests.test_sharing_publication_storage tests.test_sharing_publication tests.test_workflow_schema tests.test_setup_workflow_tables tests.test_sharing_secret_loader tests.test_sharing_signing tests.test_sharing_retry tests.test_sharing_retry_integration tests.test_sharing_retry_executor tests.test_sharing_integration tests.test_sharing_executor tests.test_sharing_receipt tests.test_sharing_query tests.test_sharing_coordinator_contract tests.test_access_reader tests.test_identity
```

Syntax, dependencies and local documentation links were checked. No full-suite,
native probe, live ADC discovery, cloud call, data/resource write, secret read,
model invocation, IAM/API change, deployment, UI change or default activation ran.
Prior [native runtime evidence](SHARING_RUNTIME_INTEGRATION.md) remains separate
and does not verify this new transport composition against deployed services.

## Remaining within 4.4

These are cooperative transport limits. Socket timeouts are not total wall-clock
deadlines: DNS, credential-provider local work, provider-internal retry loops,
background refresh or a call ignoring its timeout can outlive the budget. The
context does not cross worker threads automatically. Diagnostic state, client
close and transport cleanup are not bounded request paths. No detached worker,
hard host termination or durable cancellation/audit record is implemented here.

The subsequent [host/worker slice](SHARING_OPERATION_HOST.md) now contains ignored
timeouts and bounds cleanup for admitted requests in an opt-in prestarted POSIX
worker. All 764 affected offline tests pass. Its startup/deployment limits remain
explicit; unwrapped clients do not acquire this guarantee automatically.

1. **Critical / Must have — shared pilot:** Connect protected response/audit and
   disconnect/cancellation handling through the verified server boundary.
2. **Critical / Must have — shared pilot:** Complete
   management gates, controlled bootstrap/synchronization, last-admin and lifecycle
   safeguards within the existing milestone.
3. **Critical / Must have — shared pilot:** Add generation admission/dispatch guards
   and verify duplicate intents/lost responses before billable calls. Company
   identity activation stays deferred.

Then continue the unchanged **4.5–4.7** evidence/audit producers, durable writers and
protected UI reconnect in the [fixed plan](IMPLEMENTATION_PLAN.md#fixed-remaining-priority-4-milestones).
