# Native runtime verification

Implemented 2026-10-11 under **4.3 — Critical / Must have — shared pilot** in
[sharing_runtime_integration.py](../src/validation/sharing_runtime_integration.py).
The opt-in probe passed its isolated native acceptance cases. **4.3 is complete
within its implementation and isolated verification scope; Priority 4 is 3/7
complete, 4/7 remaining. Next: 4.4 — Critical / Must have — shared pilot.**

## Probe scope

The probe uses ADC to read the fixed project's numeric identity from Resource
Manager, verifies the serving BigQuery dataset region read-only, and then creates
one uniquely named fixture dataset with two-hour table expiration and one regional
Secret Manager secret with two-hour expiration. It creates two random signing-key
versions in that secret. No key bytes, identity assertions or sealed tokens enter
the report or repository. It requires explicit `--run` and an output directory.
Secret ownership requires the exact name/labels, a creation time after probe start,
no aliases or delayed destruction, and a lifetime between 7,190 and 7,200 seconds.
The ten-second lower tolerance matches the bounded creation RPC: the provider
computes TTL expiration slightly before stamping creation time.

Eight fixture tables use explicit destination schemas: four signing controls and
four workspace/membership/conversation/audit tables. All SQL references are limited
to these exact owned tables. Actual SDK clients, runtime sealers, authoritative
reader, secret loader and protected executor are used. Installed SQL table
identifiers and schema preflights are relocated to the fixture; the installed
profile fingerprint remains unchanged. Authentication supplies an explicit fixture
IAP identity. This is not deployed HTTP/IAP authentication or runtime service-account
IAM verification.

The native cases passed cold start and restart, durable checkpoint
absence, a real scheduled key cutover, retained original/retry seal roundtrips,
discarded native submission responses and recovery of the exact original job,
disabled-version failure without cached keys, explicit retained-key revocation,
new-cohort issuance, access removal during recovery, policy stop without key
access, and preservation of unrelated fixture rows. The retry roundtrip uses a
supplied rollback observation and never dispatches a retry; it does not replace
the existing [native retry proof](SHARING_RETRY_INTEGRATION.md).

Fixture publications/checkpoints are loaded by the harness, rather than the
deployed publisher. [4.2 publication/bootstrap verification](SHARING_SIGNING_BOOTSTRAP.md)
remains separate. Scope/labels/region/expiration checks are required before deletion;
uncertain resource creation is reconciled before cleanup. Both resource cleanups
are attempted independently and absence must be confirmed. The probe does not
enable APIs, change IAM, create serving control rows, call models, grant application
roles, modify product/approval data or activate shared execution.

## Verification and cloud changes

All **639 affected offline tests pass**, including **26 new probe guard tests** in
[test_sharing_runtime_integration.py](../tests/test_sharing_runtime_integration.py).
They verify scope containment, exact numeric secret pins, bounded ADC binding,
safe provider diagnostics, no resource creation on failed preflight, explicit
schema/returned-table refusal, bounded server TTL stamp differences, independent cleanup, ownership collision refusal and confirmed
deletion. These tests perform no live RPCs and do not prove the native case results.

```bash
.venv/bin/python -m unittest tests.test_sharing_runtime_integration tests.test_sharing_runtime_executor tests.test_sharing_runtime tests.test_signing_bootstrap tests.test_signing_bootstrap_integration tests.test_signing_control_writer tests.test_signing_write_integration tests.test_signing_storage_io tests.test_signing_read_integration tests.test_sharing_publication_storage tests.test_sharing_publication tests.test_workflow_schema tests.test_setup_workflow_tables tests.test_sharing_secret_loader tests.test_sharing_signing tests.test_sharing_retry tests.test_sharing_retry_integration tests.test_sharing_retry_executor tests.test_sharing_integration tests.test_sharing_executor tests.test_sharing_receipt tests.test_sharing_query tests.test_sharing_coordinator_contract tests.test_access_reader tests.test_identity
```

The read-only CLI project lookup confirmed project number **1042059373096** and
ACTIVE lifecycle. Initially, the stricter ADC Resource Manager probe returned **HTTP 403,
`SERVICE_DISABLED`** before any fixture creation. The latest
[report](../data/generated/sharing_runtime_verification/1f0293d4504b42869b7c19edea11ec85.json)
records `NOT_CREATED` for both resources and zero native acceptance cases. A
read-only `gcloud services list` check found neither
`cloudresourcemanager.googleapis.com` nor `secretmanager.googleapis.com` enabled
in the target project. API enablement is a persistent project configuration change
and is separate from the disposable fixture resources. Both APIs were subsequently
approved and enabled; they remain enabled. The probe itself never enables services.

All **14 isolated native cases passed** using actual ADC, BigQuery and regional
Secret Manager, with **34 secret-version accesses**. The
[successful report](../data/generated/sharing_runtime_verification/fa0fc6d36c6442dbb3bf24728d75fa8f.json)
confirms numeric project binding, scoped local ADC permissions, a real scheduled
cutover, fail-closed disabled/revoked keys, current-access denial, STOPPED handling
and stable committed original-job recovery after a deliberately discarded submit
response. The harness verified exactly one native mutation submission. Both owned
resources were deleted and their absence confirmed.

The first API-enabled run stopped because the original exact-TTL guard rejected a
23,987-microsecond server creation-stamp difference. Its dataset was deleted; its
secret was initially refused by that same guard. After adding the bounded tolerance
and regression test, cleanup matched the exact name/labels, captured creation time,
TTL and etag, deleted only that fixture secret and confirmed absence. Its
[failed report with cleanup recovery](../data/generated/sharing_runtime_verification/0857264102ae4515bf52721de64644b8.json)
remains FAILED; no failed run was relabeled as acceptance evidence.

Read-only `src.ingestion.setup_signing_tables --check` verified all four serving
tables with the expected schemas/region, each containing **zero rows**. Python
syntax, local documentation links and `pip check` passed. The full suite was not
rerun. No serving rows, product data, approvals, IAM, application users, model calls,
UI routes or default activation changed. The native report's `service_enablement=0`
describes the harness; the separately approved API changes are recorded above.
Recorded billed bytes cover selected harness queries and the native mutation,
not a complete runtime cost ledger or billing reconciliation.

## Repeating the probe

The approved prerequisite and opt-in command are:

```bash
gcloud services enable cloudresourcemanager.googleapis.com secretmanager.googleapis.com --project=project-661d501a-e709-4276-a26
.venv/bin/python -m src.validation.sharing_runtime_integration --run --output-dir data/generated/sharing_runtime_verification
```

No IAM permissions were granted. Native verification under another principal may
identify missing permissions, which must not be silently granted or replaced with
local authoritative data. Deployed service-account IAM, real IAP/HTTP invocation,
serving owner/consumer/resource pins, publication rollout and shared activation
remain release checks. These sequential fresh checks do not prove instantaneous
cross-replica revocation or cancel already submitted jobs.

1. **4.4 — Critical / Must have — shared pilot:** First implement a cumulative
   operation deadline and cancellation contract covering lock waits, ADC, queries,
   secret loading, reconciliation and final access/result checks. Expiry after
   dispatch must withhold results and preserve original-job/UNKNOWN recovery.
2. **4.4 — Critical / Must have — shared pilot:** Connect protected response/audit
   and management gates, controlled bootstrap/synchronization, last-admin and
   lifecycle protections; then generation admission/dispatch guards. Verify
   duplicate intents and lost responses before billable model dispatch. Actual
   company identity activation remains deferred.
3. **4.5–4.7:** Keep evidence/audit producers, durable writers and protected UI
   reconnect under those fixed IDs after their dependencies are ready.

Official references: [project lookup](https://docs.cloud.google.com/resource-manager/reference/rest/v1/projects/get),
[regional secret creation](https://docs.cloud.google.com/secret-manager/regional-secrets/create-regional-secret),
[regional secret expiration](https://docs.cloud.google.com/secret-manager/regional-secrets/set-expiration-regional-secrets).
The [fixed plan](IMPLEMENTATION_PLAN.md#fixed-remaining-priority-4-milestones) retains
the same seven IDs.
