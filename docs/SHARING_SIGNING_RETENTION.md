# Sharing signing-key and version retention contract

Priority 4 slice implemented on 2026-10-10. The pure
[policy contract](../src/application/sharing_signing_contract.py) and opt-in
[retained sealer](../src/application/sharing_retained_sealer.py) use trusted,
injected metadata and key bytes. This slice loads no secrets, makes no cloud
calls, publishes no policy, and changes no execution defaults or UI wiring.

## Key windows and rotation

`SharingSigningPolicy` is an immutable snapshot with one explicitly active key,
at most eight keys and eight profiles. Each key has an opaque ID, profile ID,
UTC issuance interval, retention deadline and explicit revocation flag. Issuance
intervals are half-open and cannot overlap. Retention must extend at least seven
days past the end of initial issuance; it never changes an intent's original
seven-day, half-open window.

New initial intents use the active key and must have acceptance times in that
key's interval. An inactive or expired issuance interval cannot issue initial
intents. Existing intents and bounded retries use their original acceptance
cohort's key and profile. Retired keys therefore remain available for **retry
signing as well as verification** within the original intent window. They are
not verification-only keys. Both layers of a retry seal use that same original
key; mixed-key nested tokens are rejected.

Example: key A stops initial issuance at T, key B starts at T. An intent accepted
just before T can still resolve and retry under A for its original seven days.
A new intent accepted at T uses B. Rotating does not change native job IDs,
business pins, predecessor checks, attempt limits, receipt identity or deadline.

`validate_signing_policy_transition` rejects early removal, rebinding of known
key schedules/profiles, reversal of recorded revocation, backdated new key
cohorts and activation outside the proposed active interval. At the retention
boundary, an old key can be removed. Plan schedules in advance: changing an
existing issuance interval is deliberately rejected. A revoked key remains in
the policy until normal retirement, with its material omitted. Revoking the
active key makes configuration invalid; the future publisher/runtime must stop
issuance rather than fall back to an old snapshot. This contract does not
implement an unscheduled overlapping emergency replacement cohort.

## Version and handler selection

Profiles bind the installed v1 SQL renderer, project/dataset/region, four control
schemas, query limits, identity issuer, policy/command/retry versions and lifetime.
The existing per-attempt fingerprint also checks exact SDK settings/parameters.
Profiles are checked at construction and use. Different retained settings can
coexist under different profile IDs, and the executor dispatches using the
resolved original settings, even after active settings change.

Only the currently installed `sharing-seals-v1` handler is supported. A changed
SQL/schema/policy renderer or unsupported handler fails closed; the contract
does **not** store or execute old arbitrary SQL, dynamically import a handler,
or claim support for future versions. A future incompatible release must first
retain a reviewed compatible implementation or drain old intents before release.
`SharingSealProfile.from_runtime` is a trusted authoring helper; regenerating a
profile is not a migration protocol and cannot repair an old attempt fingerprint.

The token's unverified key hint selects one allowlisted in-memory handler only.
It cannot trigger secret fetching, select a URL/path or alter settings. Existing
canonical encoding, HMAC domain separation, exact payload fields, principal,
fresh identity, expiry and reconstructed attempt checks remain enforced.
The `share1` and `share-retry1` formats are unchanged and legacy tokens work when
their key ID, bytes, cohort and profile are explicitly retained. Imported older
keys must have schedules covering their original acceptance times.

## Runtime trust boundary

`RetainedSharingSealer` is an optional `SharingRetrySealer` subclass that can be
injected into the existing protected executor. The single-key sealers are
unchanged. Every nonrevoked configured key needs exact immutable bytes of
32–64 bytes; missing/extra material and reuse of identical bytes across IDs are
rejected. Input mappings are copied; representations and validation errors do
not include key bytes. Revoked material is not retained by a new instance.

The policy itself is not an access grant or current commit proof. Fresh IAP,
BigQuery access/receipt checks and native predecessor verification remain the
executor's responsibility. Equal trusted snapshots/material produce compatible
tokens across offline replica/restart fixtures. This does not prove deployed
replica consistency, current managed-secret status, synchronized clocks,
revocation propagation or rollback-resistant publication. A stale running
instance keeps its injected snapshot until replaced. Retired keys still held in
memory require operational removal; Python memory zeroization is not provided.

No secret resource is chosen by this pure contract. The separate
[pinned loader](SHARING_SECRET_LOADER.md) now binds each key ID to an explicit
regional numeric secret version with offline SDK verification. Publication,
live access and deployed policy/key consistency remain pending. BigQuery remains authoritative for
application business/vector data; no local serving fallback is introduced.

## Verification

All **304 affected tests passed**, including **38 new tests** in
[test_sharing_signing.py](../tests/test_sharing_signing.py):

```bash
.venv/bin/python -m unittest tests.test_sharing_signing tests.test_sharing_retry_integration tests.test_sharing_integration tests.test_sharing_retry_executor tests.test_sharing_retry tests.test_sharing_receipt tests.test_sharing_executor tests.test_sharing_query tests.test_sharing_coordinator_contract tests.test_access_reader tests.test_identity
```

Coverage includes planned rotation, issuance/retention boundaries, explicit
revocation, no deadline extension, all three nested retries, retained settings,
replica/restart compatibility, policy transition conflicts, unavailable handlers,
SQL drift, wrong/missing/reused material, mixed-key seals, tampering and identity.
Five new executor tests cover new-intent selection, retired-key retry issuance
and recovery, dispatch with retained settings, revocation before BigQuery I/O,
and the disabled default.

The full suite was not rerun. Existing
[native retry evidence](SHARING_RETRY_INTEGRATION.md) remains evidence for its
original SQL/job paths; it is not managed-key integration evidence. No live
queries, model calls, provisioning, deployments, IAM/user or approval changes
occurred in this slice.

## Next small steps

1. The [pinned-secret loader](SHARING_SECRET_LOADER.md) is implemented with 335
   affected offline tests and current official API/SDK verification. Live
   provisioning/access and policy/runtime consistency remain unverified.
2. The [publication/refresh contract](SHARING_PUBLICATION_REFRESH.md) is complete
   offline with 379 affected tests. [4.1 storage/read design](SHARING_PUBLICATION_STORAGE.md) is complete offline with 443 affected tests. 4.2 (Critical / Must have) is complete within implementation/native fixture scope: [resource setup, codecs and authoritative reads](SHARING_PUBLICATION_READER.md), the [fenced writer](SHARING_PUBLICATION_WRITER.md) and [controlled bootstrap](SHARING_SIGNING_BOOTSTRAP.md) are implemented and verified; 546 affected offline tests and 13 native bootstrap cases pass, and real serving bootstrap/shared activation remain deferred. Serialized runtime refresh remains in 4.3; verify
   operational revocation/rotation before activation. Typed state is not authority.
3. Add overall host deadlines, conservative cancellation/outcome recovery and
   protected response/audit endpoint contracts in separate slices.
4. Connect fresh evidence/audit producers and fenced durable workflow writers,
   then protected campaign submission/history/reconnect. See the
   [ordered plan](WORKFLOW_DATA_DESIGN.md#next-small-steps).
