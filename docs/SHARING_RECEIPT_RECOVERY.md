# Authoritative sharing receipt recovery

Priority 4 small milestone, implemented on 2026-10-10. The separate server-only
`ProtectedSharingExecutor.recover_receipt(headers, token)` reads BigQuery when
original mutation-job results are unavailable. It is not wired to HTTP or the
local chat, and the executor remains disabled by default.

## Behavior and trust boundary

[sharing_receipt.py](../src/application/sharing_receipt.py) constructs one
parameterized SELECT containing complete explicit projections of workspace,
membership, conversation and audit records. The four subqueries share one native
query snapshot and server observation timestamp. Each returns at most two rows
to detect duplicates. The audit lookup uses the complete scoped retry prefix and
the original accepted/deadline partition interval; equality on only the full
audit ID would miss changed-envelope conflicts.

The adapter in [sharing_executor.py](../src/application/sharing_executor.py):

1. Authenticates observed IAP headers, resolves the original sealed intent and
   checks fresh creator/owner access and the original intent window.
2. Checks deployed destination schemas/layouts and identity/window freshness
   again before dispatch.
3. Submits one SELECT using a new read-job ID, fixed project/region, explicit
   parameters, byte/time limits and disabled cache/session/RPC/job retries.
4. Requires a terminal successful correlated native job with the expected SQL,
   parameters and settings. It normalizes bounded nested SDK Rows and validates
   the complete result through the existing coordinator contracts with UNKNOWN
   commit input, which cannot produce a write proposal.
5. Rechecks current access, identity freshness and the original window before
   returning the minimal immutable result.

The result contains only `state`, `audit_id`, `observed_at` and
`resulting_revision`:

| Condition | Result |
| --- | --- |
| Matching valid committed audit with current permission | REUSED and the original conversation revision |
| No receipt with valid current controls/access | UNKNOWN with no resulting revision, including when original revision pins have become stale |
| Changed envelope, duplicate/malformed rows or inconsistent receipt/state | Validation failure; no success or retry grant |
| Current revocation, ownership/lifecycle denial or expired identity/intent | Denied |
| Unavailable result, failed/nonterminal/mismatched native job | Safe `SHARING_RECEIPT_UNAVAILABLE` failure without automatic resubmission |

A later legitimate visibility change remains intact. REUSED describes historical
receipt recovery, not the conversation's current visibility and not a new write.
The receipt proves that the accepted business intent committed; it does not
identify which native mutation job committed. This method therefore returns a
`SharingQueryResult`, not a job-specific `SharingCommitObservation`.

Original-job `recover` remains a separate metadata reconciliation method. Receipt
recovery does not read original job metadata, replace its ID, issue a new intent,
rebase business pins, move the seven-day window, write an audit or change rows.
The newly allocated native job is only a SELECT. A caller may explicitly request
another read after a read failure, but missing evidence cannot authorize a new
mutation attempt. Managed key/version retention and endpoint response policies
remain pending.

BigQuery remains authoritative. No cache, local JSON fallback, role bootstrap,
model invocation, product regeneration, schema migration or UI change is added.

## Focused verification

[23 new receipt tests](../tests/test_sharing_receipt.py) cover the read contract,
full prefix/window, explicit query bounds, no mutation, historical state,
malformed/duplicate records, causal audits, stale pins, permission changes,
expiry, schema drift, safe transport failures, nested SDK Rows and native job
correlation. The [probe isolation tests](../tests/test_sharing_integration.py)
include receipt SELECT relocation without changing the original intent fingerprint.

The 181 impacted tests passed: receipt, sharing executor/query/coordinator,
probe isolation, access reader and identity flows. Unrelated generators/UI tests
were not rerun for this small slice. The previous complete-suite baseline was
944 passing tests; this milestone does not claim a new full-suite run.

Focused native SELECT verification uses the existing owned-fixture harness:

```sh
.venv/bin/python -m src.validation.sharing_integration --run --receipt-only --output-dir data/generated/sharing_integration
```

All six focused native cases passed in `asia-south1` on 2026-10-10. The
[verification report](../data/generated/sharing_integration/eec2502da91d4e0eb4e3ed48e1979f30.json)
records `PASS`, mode `RECEIPT_ONLY`, `DELETED_OWNED_DATASET`, zero serving writes
and default-disabled execution. It verifies absence/UNKNOWN, matching receipts,
changed-envelope conflicts, preservation of later visibility, recovery after a
committed script loses its final result, and an unchanged unrelated fixture.
Fixture setup uses synthetic nonserving rows; cleanup retains the ownership and
terminal-job guards. The prior transaction/race suite is not rerun in this mode.
The report records 52 fixture execution/snapshot queries, 102,765 processed bytes
and 786,432,000 billed bytes as returned by native parent jobs. These observations
exclude load jobs and do not constitute a reconciled monetary cost ledger. No LLM
or embedding calls were made.

## Next small step

The [typed retry and sealed-attempt contract](SHARING_RETRY_CONTRACT.md) is now
complete offline. [Trusted retry executor issuance/submission/recovery](SHARING_RETRY_EXECUTOR.md)
is now implemented with fresh predecessor noncommit/access/control checks and
252 passing affected tests at that milestone. The separate
[native retry probe](SHARING_RETRY_INTEGRATION.md) now passes ten cases and 266
affected offline tests with owned cleanup. The [key/version retention contract](SHARING_SIGNING_RETENTION.md) now passes 304 affected tests. The [pinned loader](SHARING_SECRET_LOADER.md) now passes 335 affected offline tests; the [publication/refresh contract](SHARING_PUBLICATION_REFRESH.md) now passes 379 affected tests. [4.1 storage/read design](SHARING_PUBLICATION_STORAGE.md) is complete offline with 443 affected tests. 4.2 (Critical / Must have) is complete within implementation/native fixture scope: [resource setup, codecs and authoritative reads](SHARING_PUBLICATION_READER.md), the [fenced writer](SHARING_PUBLICATION_WRITER.md) and [controlled bootstrap](SHARING_SIGNING_BOOTSTRAP.md) are implemented and verified; 546 affected offline tests and 13 native bootstrap cases pass, and real serving bootstrap/shared activation remain deferred; deployed IAP/HTTP mutation verification remains pending.
Keep the original accepted envelope, business pins and deadline unchanged,
refresh only the operational workspace fence explicitly, and seal/pin the new
attempt's stable mutation-job ID. UNKNOWN, a missing receipt, a timeout or an
unavailable store must never remint a mutation job. Add competing-retry and
revocation tests before exposing this path. See the
[ordered remaining work](WORKFLOW_DATA_DESIGN.md#next-small-steps).
