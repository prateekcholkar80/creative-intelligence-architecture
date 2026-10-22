# Isolated native sharing verification

Implemented on 2026-10-10 as a small Priority 4 milestone. The opt-in
[probe](../src/validation/sharing_integration.py) executes the existing sharing
transaction against synthetic, nonserving BigQuery fixtures. The
[native job helpers](../src/application/sharing_jobs.py) are shared with the
disabled-by-default production executor, so SDK correlation and conservative
commit classification use the same implementation.

## Repeatable command

Use the repository virtual environment and existing Application Default
Credentials:

```sh
.venv/bin/python -m src.validation.sharing_integration --run --output-dir data/generated/sharing_integration
```

This command creates and deletes its own fresh fixture dataset and incurs
BigQuery query costs. It performs no model invocation. Each query retains an
explicit byte/time bound; each submission uses a preallocated native job ID with
RPC and job retries disabled. A lost submission response triggers lookup of that
original ID, never an automatic replacement query.

## Isolation and cleanup

- Project and region remain fixed to the configured project and `asia-south1`.
- Every run creates a new `ci_sharing_it_<uuid>` dataset with exact ownership
  labels and a two-hour default table expiry. Existing datasets are never adopted.
- Only `workspaces`, `memberships`, `conversations` and `audit_events` are created,
  using explicit destination schemas, partition requirements and clustering from
  the workflow manifest. Nine isolated fixture workspaces are loaded.
- Only the four exact table references in the production SQL template are
  redirected. Other quoted table references are rejected before submission.
  Production SQL scope and execution configuration remain unchanged.
- Fixture identities are synthetic test subjects, not signed-in application
  users. The report records `SYNTHETIC_POC` provenance. No serving membership,
  product, campaign, claim, vector, GCS object or IAM record is changed.
- Cleanup rereads ownership labels, project, region and expiry, rejects unexpected
  tables and requires every preallocated job to be terminal before deleting the
  exact owned dataset. Uncertain ownership or unresolved jobs cause cleanup to
  refuse deletion and report failure; table expiry is an additional safeguard.

## Native acceptance cases

The probe verifies exact scoped state and records native job IDs, terminal
status, reported bytes and safe failure reasons:

1. Apply consumes one workspace fence and one conversation revision and inserts
   one audit; replay returns REUSED without another mutation.
2. Changing the accepted envelope under the same retry scope conflicts and
   rolls back.
3. Reusing an older receipt preserves a later legitimate visibility change.
4. UNKNOWN with no receipt performs no mutation.
5. A stale membership expectation fails without mutation.
6. Injected failure after the fence update rolls back the fence and other rows;
   native child evidence establishes NOT_COMMITTED.
7. Injected failure after commit retains the applied rows and audit; native child
   evidence establishes COMMITTED even though final result retrieval failed.
8. Concurrent submissions of the same native job ID produce one mutation;
   deliberately discarded submission responses are recovered using that ID.
9. Distinct intents with the same revision pins have genuinely overlapping native
   transaction intervals, one successful mutation and one audit. Child timestamps
   and parent/project/region correlation are checked, rather than inferring overlap
   from local worker start times.
10. An atomic test-only membership revocation participates in the workspace fence;
    subsequent sharing is denied without mutation.
11. An unrelated fixture workspace is unchanged.

State comparison queries order rows by their manifest logical keys. Native
BigQuery result order without an ORDER BY is not a mutation signal.

## Verification evidence

All 11 native cases passed in `asia-south1` on 2026-10-10 with
`google-cloud-bigquery` 3.46.1. The
[successful report](../data/generated/sharing_integration/7bc8187476d94111bcbfde800e9f1c60.json)
records `PASS`, `DELETED_OWNED_DATASET`, zero serving data writes and execution
activation false. The race includes correlated child transaction intervals,
one successful mutation and one audit. The duplicate case observed one accepted
submission and one reconciliation of the same native ID.

The report contains metrics for 85 execution/snapshot queries: 160,870 bytes
processed and 1,174,405,120 bytes billed as returned by native parent jobs. Small
fixture scans still incur minimum billed bytes. These figures are diagnostic
observations, not a complete cost ledger: fixture load jobs and the separately
handled duplicate-submission job are outside this metrics list, and no price or
billing reconciliation is inferred.

The first runs exposed local fixture/comparison defects that were corrected;
their owned datasets were deleted. A sandbox-only authentication attempt failed
before creating any resource. Reports preserve these failed attempts; they are
verification exports and never a database/retrieval fallback.

The [15 offline probe tests](../tests/test_sharing_integration.py) cover isolation,
fixture validity, fault locations, unsafe scope rejection, ownership/terminal-job
cleanup guards, safe reports and correlated native transaction intervals. The
existing 33 executor tests remain intact. All 120 sharing tests (coordinator,
query, executor and probe) and the full 944-test suite pass.

## Limits and next milestone

This is fixture verification, not activation of sharing or company SSO. It does
not verify a deployed Google-issued assertion, host cancellation, process death,
populated production throughput, billing reconciliation or uncontrolled external
writes. Response loss is tested by discarding actual submission responses, not by
injecting a network outage. Revocation is committed before the sharing request;
concurrent administrative revocation and a protected management endpoint remain
future work. Table expiry does not automatically delete an empty dataset left by
an ownership/cleanup failure.

The [authoritative read-only receipt path](SHARING_RECEIPT_RECOVERY.md) is now
implemented and verified separately with six focused native cases. Use the
`--receipt-only` command documented there for that flow. The
[trusted controlled-retry adapter](SHARING_RETRY_EXECUTOR.md) is now implemented
with 252 affected offline tests passing at that milestone. The separate
[retry-only native mode](SHARING_RETRY_INTEGRATION.md) now passes ten focused cases
and 266 affected offline tests, with owned cleanup. It preserves the accepted
envelope, business pins/window and sealed workspace fence. UNKNOWN must not
remint an attempt. Managed key/version retention and protected endpoint policies
must be completed before HTTP/UI exposure. See the
[ordered workflow steps](WORKFLOW_DATA_DESIGN.md#next-small-steps).
