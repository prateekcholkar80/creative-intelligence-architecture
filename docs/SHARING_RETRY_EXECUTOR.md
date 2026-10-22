# Trusted sharing retry executor

Priority 4 adapter slice, implemented on 2026-10-10 in
[sharing_executor.py](../src/application/sharing_executor.py). It connects the
[retry contract](SHARING_RETRY_CONTRACT.md) to signed identity, fresh BigQuery
controls and correlated native job inspection. All 252 affected offline tests
passed, including 41 new adapter tests. No cloud calls occurred in this slice.
The separate [native retry probe](SHARING_RETRY_INTEGRATION.md) now passes ten
isolated cases with owned cleanup. Deployed recovery, HTTP/UI integration and
managed secret provisioning remain pending. The [retention contract](SHARING_SIGNING_RETENTION.md) is now complete offline. Execution stays disabled by default; the local chat does not
call these methods.

## Trusted composition

Retry methods require an explicitly injected `SharingRetrySealer` with trusted
runtime key/settings. Initial-token methods remain compatible with
`SharingIntentSealer`. There is no default/stored key, local registry, development
identity or browser status/fence grant. Composition still requires the fixed
project, typed IAP/access adapters, the same reader client and no default query
overrides. Every public call authenticates actual IAP headers, resolves its seal,
checks the original window and current creator/owner access. Permission is
reread before releasing results/tokens and after dispatch. Signed tokens protect
integrity; they do not establish current access or native rollback.

## Methods and outcomes

| Method | Behavior |
| --- | --- |
| `issue_retry(headers, predecessor_token)` | Accepts initial/retry tokens; returns READY with a sealed retry, REUSED with the original receipt result, or RECOVERY_REQUIRED with neither. Never submits DML. |
| `submit_retry(headers, retry_token)` | Reconciles that sealed retry job first. Known commit/noncommit returns without resubmission. New dispatch requires verified predecessor rollback and current controls/pins/fence; one insert uses only the sealed ID/configuration. |
| `recover_retry(headers, retry_token)` | Reads only the sealed retry job, with current access checks. Never submits queries or inspects ancestors. Missing metadata stays UNKNOWN. |
| `recover_receipt(headers, token)` | Accepts initial/retry tokens; reads one bounded authoritative receipt/control snapshot. Requires no mutation-job metadata and cannot classify a native mutation job. |

`SharingRetryIssuance` is an immutable minimal projection: READY has a token and
no result; REUSED has a validated historical result and no token; RECOVERY_REQUIRED
has neither. Invalid combinations are rejected. These are service contracts,
not implemented HTTP responses.

## Issuance and dispatch gates

Issuance preflights all four deployed control/receipt tables and dataset layout,
then freshly reconciles every ancestor through the requested predecessor. Exact
project/region/job identity, SQL, parameters and settings must match. Each must be
a failed terminal correlated root job with positive successful rollback child
evidence and no conflicting successful commit. Error strings, incomplete/foreign
children, pending/successful parents, timeouts and missing metadata are
insufficient. The bounded child inspector is reused. Identity/window freshness is
checked before each ancestor lookup.

A single correlated SELECT captures full workspace, membership, conversation and
full-prefix receipt records with explicit projections, LIMIT 2 per scope,
original partition bounds, byte/time limits and no cache/session. Native metadata
and bounded normalized SDK rows are checked. Full records stay server-internal;
no local exports are read. Access is reread after the snapshot.

A matching receipt returns REUSED before planning, even if job inspection is
uncertain or another attempt committed. Later visibility stays intact. Absent
receipt plus incomplete rollback proof returns RECOVERY_REQUIRED. Only positive
rollback plus absence and valid current controls permit planning: the fence comes
from the snapshot, original membership/conversation pins must match and the
original envelope/window/settings and three-retry cap stay fixed. Receipt absence
alone never grants a retry; exhaustion never automatically replaces the intent.

Submission reconciles its own job first. Known outcomes return without dispatch;
recovery remains possible when ancestor metadata has disappeared. For an
unresolved job, every predecessor is checked again, followed by the authoritative
snapshot and current access. A signed supplied rollback observation cannot pass
these gates alone. If a receipt exists while the retry job is unresolved, the
outcome contains REUSED for the business result and UNKNOWN for that exact job;
the SELECT does not attribute the mutation to it.

Otherwise the snapshot must match both original business pins and the sealed
fence. A stale fence raises a conflict; submission never substitutes a new fence
or ID. Explicit issuance against the same positively aborted predecessor can
refresh the fence while preserving the original intent. Dispatch performs one
`jobs.insert`, with fixed scope/sealed ID and disabled RPC/job retries. Lost
responses/collisions reconcile that ID. An explicit repeat submission may insert
the same ID after fresh gates, but cannot remint it. UNKNOWN for a retry plus no
receipt cannot issue its next retry. No model calls occur in this path.

## Verification and limits

```bash
.venv/bin/python -m unittest tests.test_sharing_retry_executor tests.test_sharing_retry tests.test_sharing_receipt tests.test_sharing_executor tests.test_sharing_query tests.test_sharing_coordinator_contract tests.test_sharing_integration tests.test_access_reader tests.test_identity
```

All 252 tests passed. The 41 new
[adapter tests](../tests/test_sharing_retry_executor.py) use real SDK job metadata,
supplied cloud fixtures and a real ES256 IAP signature fixture. Coverage includes
default disablement/configuration, seal/auth denial, no initial-method bypass,
server fence/immutable pins, replica ID convergence, uncertain/conflicting native
metadata, receipt reuse, schema/scope/duplicate checks, revocation/expiry, forged
supplied observations, exact dispatch, duplicate/lost responses, whole-chain
revalidation, exhaustion and read-only recovery. Fixtures neither execute SQL nor
prove native transaction races.

The previous full-suite baseline remains 944; unrelated suites were not rerun.
No native probe ran here. Earlier [transaction](SHARING_INTEGRATION.md) and
[receipt](SHARING_RECEIPT_RECOVERY.md) probes are evidence for their earlier
paths, not native retry validation. No cloud data, IAM, approvals, serving users
or UI behavior changed. RPC/result limits are per call, not an overall host
deadline. Live managed-secret access/rollout, future incompatible handler migration, crash/cancellation recovery,
deployment ingress, protected response/audit contracts and production throughput
remain open. Fixture verification does not enable execution or company SSO.

## Next small steps

1. The [focused native retry probe](SHARING_RETRY_INTEGRATION.md) is complete with
   ten passing cases and 266 affected offline tests. Its owned dataset was deleted;
   serving execution remains disabled. The [key/version retention contract](SHARING_SIGNING_RETENTION.md)
   passes 304 affected tests; the [pinned loader](SHARING_SECRET_LOADER.md) now passes
   335 affected tests. The [publication/refresh contract](SHARING_PUBLICATION_REFRESH.md)
   now passes 379 affected tests. [4.1 storage/read design](SHARING_PUBLICATION_STORAGE.md) is complete offline with 443 affected tests; 4.2 (Critical / Must have) is complete within implementation/native fixture scope: [resource setup, codecs and authoritative reads](SHARING_PUBLICATION_READER.md), the [fenced writer](SHARING_PUBLICATION_WRITER.md) and [controlled bootstrap](SHARING_SIGNING_BOOTSTRAP.md) are implemented and verified; 546 affected offline tests and 13 native bootstrap cases pass, and real serving bootstrap/shared activation remain deferred.
2. Deliver runtime protection in smaller slices: publication persistence/authoritative
   reads and serialized runtime refresh; deployed rotation/replica/restart compatibility; overall deadlines
   and conservative cancellation recovery; protected responses/audits and ingress.
3. Connect fresh evidence/audit producers and durable workflow writers, then
   protected campaign submission/history/reconnect in the UI. Preserve model
   profile/usage/cost pins and avoid repeating uncertain billable calls. Company
   bootstrap/deployment waits for real identity configuration. See the
   [ordered remaining plan](WORKFLOW_DATA_DESIGN.md#next-small-steps).
