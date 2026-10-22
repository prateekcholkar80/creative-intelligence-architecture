# Pinned sharing-secret loader

Priority 4 slice implemented on 2026-10-10 in
[sharing_secret_loader.py](../src/application/sharing_secret_loader.py).
This is an opt-in loader/composer for the existing
[retained-key sealer](SHARING_SIGNING_RETENTION.md). It does not provision keys,
publish configuration, replace running instances or activate sharing/UI execution.

## Provider contract verified

Google's [regional access sample](https://docs.cloud.google.com/secret-manager/docs/samples/secretmanager-access-regional-secret-version)
uses ADC, a regional endpoint and CRC32C payload verification.
[Location documentation](https://docs.cloud.google.com/secret-manager/docs/locations)
lists regional Secret Manager support in asia-south1. The loader pins
`secretmanager.asia-south1.rep.googleapis.com` and rejects other client endpoints.
The [SDK reference](https://docs.cloud.google.com/python/docs/reference/secretmanager/latest/google.cloud.secretmanager_v1.services.secret_manager_service.SecretManagerServiceClient)
exposes retry and timeout arguments. Calls explicitly use `retry=None`.

The new direct dependencies are `google-cloud-secret-manager>=2.31,<3` and
`google-crc32c>=1.9,<2`. Verification used SDK 2.31.0 and CRC32C 1.9.0. Installing
the SDK added that package only; existing dependencies satisfied its requirements.

## Trusted configuration and pins

`SharingSecretConfig` binds an immutable signing policy to a canonical project
number and one `SharingSecretVersion` per policy key. Project ID and location
must equal the project's established constants. The numeric project number is
trusted deployment metadata: deployment setup must verify its association with
`project-661d501a-e709-4276-a26` before publication. The loader does not perform a
Resource Manager lookup or prove that association. Unit tests use a clearly
marked fixture number, never a deployed binding.

References are built as
`projects/<number>/locations/asia-south1/secrets/<secret-id>/versions/<number>`.
Version numbers must be positive integers; aliases such as `latest`, numeric
strings, arbitrary URIs/paths, duplicate resources and incomplete maps are
rejected. Revoked keys retain their version metadata but their bytes are never
fetched. This preserves history for transition validation.

`validate_secret_config_transition` applies the signing-policy transition checks
and rejects changing the project number or an existing key's secret/version pin,
including revoked keys. It validates supplied snapshots; it does not publish
them, select the current snapshot or prevent a running replica from using stale
configuration. Publication and rollback/revocation propagation remain next.

## All-or-nothing composition

`create_regional_secret_client` uses normal SDK ADC discovery with the fixed
endpoint, without explicit key files or manual credential fallback. The caller
owns the client/transport lifecycle. `SharingSecretLoader.load` preflights the
active issuance window and every required installed profile before any RPC.

Each nonrevoked key receives one access RPC for its exact pinned version. The
returned SDK message must have the exact resource name, raw binary material
of 32–64 bytes, an explicitly present CRC32C field and a matching checksum.
CRC presence is checked separately from its value; a zero checksum is not
treated as missing. No text decoding, trimming or inferred key conversion occurs.
The [integrity documentation](https://docs.cloud.google.com/secret-manager/docs/data-integrity)
describes the returned checksum as transport/data integrity evidence; it does
not establish current application authorization or correct key provenance.

Only after all material and profiles pass does composition return a
`RetainedSharingSealer`. Issuance/profile checks are repeated after composition.
Missing/disabled versions, denied access, unavailable RPCs, wrong names,
corruption, short/long payloads and reused bytes across IDs fail closed. Provider
exception text is suppressed from rendered errors/tracebacks. The staging map
is cleared on both success and failure; Python memory zeroization is not claimed.
There is no saved payload, local registry or successful-load cache to fall back
to. A failed new composition cannot automatically return an old sealer.

Defaults are five seconds per RPC and a thirty-second monotonic admission budget.
Remaining budget clips each RPC timeout; exhausted or late composition is
rejected. Settings have explicit upper bounds. These are RPC/admission checks,
not a hard process deadline: ADC discovery/refresh, native transport behavior and
host cancellation still need overall runtime protection.

Successful loading is a snapshot. Later managed-version disabling or policy
revocation does not erase bytes from an existing sealer; the future runtime must
refresh/replace or stop it under controlled publication. The loader has no native
ENABLED-state field to independently verify in the access response; disabled
access failures are handled conservatively. Retired keys still sign bounded
retries for their original cohort under the existing retention contract.

BigQuery remains authoritative for business/application records and vectors;
Secret Manager supplies signing-key bytes only. No local serving fallback or
product/claim/approval changes are introduced.

## Verification

All **335 affected offline tests passed**, including **31 new loader tests** in
[test_sharing_secret_loader.py](../tests/test_sharing_secret_loader.py):

```bash
.venv/bin/python -m unittest tests.test_sharing_secret_loader tests.test_sharing_signing tests.test_sharing_retry_integration tests.test_sharing_integration tests.test_sharing_retry_executor tests.test_sharing_retry tests.test_sharing_receipt tests.test_sharing_executor tests.test_sharing_query tests.test_sharing_coordinator_contract tests.test_access_reader tests.test_identity
```

Tests use real SDK clients/messages with anonymous fixture credentials and mocked
access RPCs. They cover fresh repeated loads/restarts, old-token and retry
compatibility, changed active profiles, aliases/scope/binding rejection, revoked
key skipping, missing/disabled/denied/unavailable access, partial failures, exact
response identity, CRC presence/corruption, raw binary boundaries, admission
deadlines, profile/issuance drift and safe credential/provider errors. Factory
tests verify ADC constructor arguments; they do not authenticate to GCP.
`pip check` passed with no broken requirements. The full suite/native probes were
not rerun: SQL, legacy sealers, executors and UI defaults were unchanged.

No live Secret Manager/BigQuery/GCS/model calls, secret provisioning, IAM/user
changes, deployments or approvals occurred. Dependency installation and official
documentation retrieval were the only network operations. Live resource access,
least-privilege runtime configuration and managed rotation remain unverified.

## Next small steps

1. The [publication/refresh contract](SHARING_PUBLICATION_REFRESH.md) is complete
   offline with 379 affected tests. [4.1 storage/read design](SHARING_PUBLICATION_STORAGE.md) is complete offline with 443 affected tests. 4.2 (Critical / Must have) is complete within implementation/native fixture scope: [resource setup, codecs and authoritative reads](SHARING_PUBLICATION_READER.md), the [fenced writer](SHARING_PUBLICATION_WRITER.md) and [controlled bootstrap](SHARING_SIGNING_BOOTSTRAP.md) are implemented and verified; 546 affected offline tests and 13 native bootstrap cases pass, and real serving bootstrap/shared activation remain deferred.
   Serialized runtime refresh remains in 4.3. Keep metadata separate from secret bytes and
   preserve BigQuery authority; typed observations do not prove a current head.
2. Connect publication to trusted runtime composition, verify actual project
   number/resources/runtime access, and run an isolated managed rotation/restart/
   revocation probe before activation. Define provisioning scope and permissions
   as a separate reviewable slice; no deployment secrets are assumed here.
3. Add overall host deadlines, conservative cancellation/outcome recovery and
   protected response/audit endpoint contracts.
4. Connect fresh evidence/audit producers and fenced durable workflow writers,
   then protected campaign submission/history/reconnect. Follow the
   [ordered remaining work](WORKFLOW_DATA_DESIGN.md#next-small-steps).
