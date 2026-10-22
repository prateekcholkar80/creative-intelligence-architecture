# Controlled sharing retry contract

Priority 4 contract slice, implemented on 2026-10-10 in
[sharing_retry.py](../src/application/sharing_retry.py). This is an offline
proposal and integrity envelope. It performs no cloud calls, proves no native
rollback and does not grant access or enable execution. The
[trusted executor](SHARING_RETRY_EXECUTOR.md) now connects retry issuance,
submission and recovery to native inspection/current access with offline tests;
the separate [native fixture retry checks](SHARING_RETRY_INTEGRATION.md) now pass
ten cases. Deployed IAP/HTTP retry execution remains pending.

## Preserved intent and bounded attempts

`prepare_retry` accepts a typed initial or retry attempt, a supplied
`NOT_COMMITTED` observation, an explicit workspace fence and observation time.
The observation must match the immediate predecessor's exact native job ID and
the original business-intent audit ID. UNKNOWN, COMMITTED, NOT_SUBMITTED,
missing/untyped observations and unrelated jobs are rejected.

The immutable original command retains workspace/conversation/principal,
visibility, membership/conversation revisions, retry-key digest, accepted time,
trace, policy, receipt identity and seven-day deadline. Query settings remain
pinned. Only the operational workspace fence can refresh: it cannot regress or
reach the exhausted maximum revision. Equal fences are allowed because a rolled
back transaction need not have advanced the fence. This does not prove that a
supplied fence is current; the future executor must read it authoritatively.

Version `sharing-retry-v1` allows at most three retries after the original
attempt. The full immutable chain binds every predecessor observation and fence.
Observation times must be monotonic and inside the original half-open window.
This is a bounded attempt policy, not backoff, a cost estimate or budget admission.
After exhaustion, recovery may still resolve the original intent within its
window; automatic creation of a replacement business intent is not implemented.

## Stable native job identity

Each retry's `share_retry_v1_<full SHA-256>` job ID binds the retry version,
original query fingerprint, immediate predecessor ID, ordinal and exact new
SQL/parameters/settings/scope fingerprint. The same predecessor and fence produce
the same ID across replicas, even when verification timestamps differ. Different
fences or predecessors produce different IDs. A retry at the original fence still
has a distinct ID from the original job.

The new query starts as `NOT_SUBMITTED`. The predecessor's rollback observation
is retained separately; it must not be interpreted as the new job's commit
status. Consequently the existing transaction/result contracts can return REUSED
when a competing attempt has already committed the original receipt, preserving
later visibility changes. This is verified offline, not a new native race proof.

## Runtime seals and trust boundary

`SharingRetrySealer` inherits the original trusted runtime key/settings
configuration and unchanged `share1` methods. Its separate `share-retry1` format
uses a domain-separated HMAC, embeds the original sealed token and records the
bounded chain. Exact fields, canonical encoding, signature/key/version, principal,
fresh identity, original expiry and reconstructed fingerprints are checked.
Seals are bounded to 8192 characters and reject future observation timestamps.

Tokens are signed, not encrypted. They contain hashes/scoped command metadata,
not product facts, raw JWTs/subjects, raw retry keys or signing keys. There is no
local registry or BigQuery fallback. Current SQL/settings changes invalidate old
tokens safely in the single-key sealer. The opt-in [retention contract](SHARING_SIGNING_RETENTION.md)
now preserves compatible settings/key cohorts. The [pinned loader](SHARING_SECRET_LOADER.md)
is implemented offline; managed policy publication/rollout and future
incompatible handler migrations remain pending.

A caller can construct a typed `NOT_COMMITTED` observation in Python; neither the
type nor a valid seal proves provider truth. The trusted retry executor now
requires fresh correlated terminal native predecessor/rollback child evidence,
current controls/permission, schema and original receipt checks before issuing
or newly dispatching. These connections are verified offline; native retry checks
are complete in isolated fixtures. Deployed IAP/HTTP invocation remains pending.
Browser-supplied job status or fence is never sufficient.
Receipt presence proves the business intent, not which native job committed it;
receipt absence, timeout and missing job metadata cannot authorize a retry.

## Verification

All 211 affected tests passed, including 30 new tests in
[test_sharing_retry.py](../tests/test_sharing_retry.py):

```bash
.venv/bin/python -m unittest tests.test_sharing_retry tests.test_sharing_receipt tests.test_sharing_executor tests.test_sharing_query tests.test_sharing_coordinator_contract tests.test_sharing_integration tests.test_access_reader tests.test_identity
```

Coverage includes deterministic competing proposals, full scope/predecessor
binding, immutable envelope/settings, chain/fence/time bounds, uncertainty denial,
historical receipt reuse, cross-instance sealing, identity/expiry/tampering,
SQL/settings drift and initial-token compatibility. Native probes were not rerun:
this slice changes neither transaction SQL nor the execution adapter. Previous
[native transaction](SHARING_INTEGRATION.md) and
[receipt recovery](SHARING_RECEIPT_RECOVERY.md) evidence remain historical evidence
for those paths. The previous full-suite baseline is 944; only affected suites
were run here. No cloud data, IAM, users, defaults or UI behavior changed.

## Next small steps

1. Trusted executor retry issuance/submission/recovery is complete with 41 new
   adapter tests and all 252 affected tests passing. See the
   [adapter trust boundary](SHARING_RETRY_EXECUTOR.md); fixture verification does
   not establish native retry races or enable execution.
2. The [focused native retry checks](SHARING_RETRY_INTEGRATION.md) now pass ten
   cases with owned cleanup and 266 affected offline tests. The
   [signing-key/version retention contract](SHARING_SIGNING_RETENTION.md) is now
   complete offline with 304 affected tests; serving execution stays disabled.
3. The [pinned loader](SHARING_SECRET_LOADER.md) passes 335 affected offline tests.
   The [publication/refresh contract](SHARING_PUBLICATION_REFRESH.md) now passes
   379 affected tests. [4.1 storage/read design](SHARING_PUBLICATION_STORAGE.md) is complete offline with 443 affected tests. 4.2 (Critical / Must have) is complete within implementation/native fixture scope: [resource setup, codecs and authoritative reads](SHARING_PUBLICATION_READER.md), the [fenced writer](SHARING_PUBLICATION_WRITER.md) and [controlled bootstrap](SHARING_SIGNING_BOOTSTRAP.md) are implemented and verified; 546 affected offline tests and 13 native bootstrap cases pass, and real serving bootstrap/shared activation remain deferred; runtime refresh, deployed replica/restart consistency,
   overall deadlines and protected response/audit contracts in separate slices.
4. Continue fresh evidence/audit producers, durable workflow writers and protected
   UI submission/history/reconnect. Workspace activation waits for real identity
   configuration. See the [ordered plan](WORKFLOW_DATA_DESIGN.md#next-small-steps).
