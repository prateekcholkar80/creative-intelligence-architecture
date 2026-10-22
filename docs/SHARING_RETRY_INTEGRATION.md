# Native sharing retry verification

Priority 4 focused probe milestone, verified on 2026-10-10. The `--retry-only` mode in
[sharing_integration.py](../src/validation/sharing_integration.py) exercises
sealed attempt IDs, native transaction SQL, correlated job/child outcomes and
authoritative receipt snapshots. It uses ADC and the existing owned temporary
dataset guards. Serving execution stays disabled; no business data, serving
users, IAM, approvals, product assets or UI behavior is changed.

## Run and isolation

```bash
.venv/bin/python -m src.validation.sharing_integration --run --retry-only --output-dir data/generated/sharing_integration
```

`--retry-only` and `--receipt-only` are mutually exclusive. Mode validation occurs
before resource creation. This run verifies ADC access to the configured project's
`ci_application` dataset and its region before creating resources. It creates one
fresh UUID-owned `ci_sharing_it_*` dataset in `asia-south1`, four tables with
explicit destination schemas/layouts and two-hour expiry, and nine synthetic
fixture workspaces. It writes no serving tables and performs no model calls.

Every SQL statement is restricted to those four owned tables. Cleanup rereads
project/region/labels/expiry, rejects unrelated tables and requires every allocated
query/load/attempt ID to be terminal before deleting the exact owned dataset.
Concurrent attempts and duplicate IDs share that tracking. Uncertain ownership or
unresolved jobs cause cleanup refusal, not broad deletion.

## Sealed IDs and native evidence boundary

Initial/retry contracts are sealed and resolved with an ephemeral in-memory HMAC
key and synthetic typed identity fixtures. They are not actual IAP authentication
and confer no serving access. No key, token or raw identity subject is retained
in reports. Accepted envelopes, business pins, settings and original windows stay
fixed. Reverification matches terminal native job scope/SQL/parameters/settings
before accepting positive correlated rollback/commit child evidence.

The actual native job ID is the contract's deterministic sealed ID. SQL identifiers
are relocated only to owned fixture tables; the explicit post-commit fault is a
test-only variant. Reports retain both the original contract fingerprint and the
executed isolated SQL digest/fault. A repeated ID cannot be rebound to another
SQL variant. Production job correlation rejects these relocated jobs; that
rejection is verified. This is native proof for the isolated SQL/metadata path,
not an invocation of the deployed protected executor, IAP or HTTP service. The
trusted executor is separately covered by [adapter tests](SHARING_RETRY_EXECUTOR.md).

## Focused acceptance cases

1. Missing predecessor metadata plus no receipt remains recovery-only; no new
   attempt is sealed or mutation submitted.
2. An original unmodified sharing script rolls back on a stale operational fence.
   A positively verified retry uses the authoritative refreshed fence with the
   original business pins and inserts one applicable audit/mutation.
3. Concurrent submissions of the same sealed retry ID produce one native job and
   one applicable mutation. Discarded responses recover through that same ID.
4. Different sealed fence proposals for the same accepted business intent overlap
   in native transaction intervals, with one APPLIED mutation and one audit.
5. The receipt committed by another attempt returns the original result without
   issuing a new retry or changing state.
6. A stale sealed fence rolls back without mutation. A separate explicitly issued
   next retry refreshes the fence while preserving the original envelope.
7. Atomic fixture revocation advances the shared fence; subsequent SQL dispatch
   and context-based retry reissuance are denied without mutation.
8. An injected failure after retry commit is classified COMMITTED through native
   children; its receipt recovers the result without reminting.
9. Original receipt reuse preserves a later PRIVATE visibility/revision change.
10. An unrelated sentinel workspace remains unchanged.

Competing transactions are verified using correlated native child timestamps,
not thread launch times. Revocation is a sequential fenced check, not a verified
overlapping revocation race. Response loss is deliberate local response discard,
not injected network failure. Snapshots compare rows ordered by logical keys.

## Verification evidence

All 266 affected offline tests passed, including 14 new mode/isolation/proof/
cleanup tests in [test_sharing_retry_integration.py](../tests/test_sharing_retry_integration.py):

```bash
.venv/bin/python -m unittest tests.test_sharing_retry_integration tests.test_sharing_integration tests.test_sharing_retry_executor tests.test_sharing_retry tests.test_sharing_receipt tests.test_sharing_executor tests.test_sharing_query tests.test_sharing_coordinator_contract tests.test_access_reader tests.test_identity
```

All ten focused native cases passed in `asia-south1` with BigQuery SDK 3.46.1.
The [successful report](../data/generated/sharing_integration/37f7d5fa85a742359b45bcbfe32b70a8.json)
records PASS, DELETED_OWNED_DATASET, zero serving writes and execution activation
false. The run took about 6 minutes 23 seconds. Seventeen sealed attempt IDs were
tracked. All 169 recorded queries have byte metrics: 329,251 bytes processed and
2,495,610,880 bytes billed as reported by native parent jobs. These are diagnostic
figures, not a cost ledger: load jobs, metadata/API calls and actual billing
reconciliation are outside the totals. Small scans incur minimum billed bytes.

The initial sandbox attempt could not resolve the OAuth endpoint and created no
cloud resources; its [diagnostic report](../data/generated/sharing_integration/bef5f18cdaf54a97b6eb82d230d3a7d4.json)
records NOT_CREATED. The network-enabled run performed the actual ADC/native
verification. Reports are audit evidence, never a database/retrieval fallback.

The previous full-suite baseline is 944. Unrelated suites and earlier transaction
and receipt probe modes are not rerun for this small slice. Deployment ingress,
real user sign-in, host cancellation/process-crash recovery, production throughput
and end-to-end protected HTTP/UI behavior remain unverified.

## Next small steps

1. The [key/version retention contract](SHARING_SIGNING_RETENTION.md) is now
   complete offline with 304 affected tests. The [pinned secret loader](SHARING_SECRET_LOADER.md)
   passes 335 affected tests. The [publication/refresh contract](SHARING_PUBLICATION_REFRESH.md)
   now passes 379 affected tests. [4.1 storage/read design](SHARING_PUBLICATION_STORAGE.md) is complete offline with 443 affected tests. 4.2 (Critical / Must have) is complete within implementation/native fixture scope: [resource setup, codecs and authoritative reads](SHARING_PUBLICATION_READER.md), the [fenced writer](SHARING_PUBLICATION_WRITER.md) and [controlled bootstrap](SHARING_SIGNING_BOOTSTRAP.md) are implemented and verified; 546 affected offline tests and 13 native bootstrap cases pass, and real serving bootstrap/shared activation remain deferred.
   Runtime integration and deployed managed-key verification remain in 4.3.
2. Overall runtime deadlines and conservative cancellation recovery, followed by
   protected response/audit contracts and endpoint ingress checks. Preserve
   uncertain native/model outcomes and keep expensive calls outside retries.
3. Fresh evidence/audit producers and durable workflow writers, then protected
   campaign submission, history and reconnect in the UI. Workspace activation
   and bootstrap wait for real company identity configuration. See the
   [ordered remaining work](WORKFLOW_DATA_DESIGN.md#next-small-steps).
