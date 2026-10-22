# Priority 4.6 — native workflow writer verification

Verified on 2026-10-11. This is **Critical / Must have — shared pilot** evidence
for the final Priority 4.6 checkpoint.

## Scope

[`workflow_writer_integration.py`](../src/validation/workflow_writer_integration.py)
is an opt-in ADC probe. It creates one uniquely named, two-hour-expiring BigQuery
dataset in `asia-south1`, reproduces all eleven destination schemas, and relocates
every SQL table reference into that owned dataset. A containment check rejects any
foreign reference. Cleanup verifies the dataset is absent before the run can pass.

The probe uses the production terminal-failure plan and the default-disabled
[`WorkflowWriterExecutor`](../src/application/workflow_writer_executor.py). It
does not authenticate a serving user, call a model, read GCS, enable serving
writes, or represent generated data as historical business data.

## Native cases

All six cases passed:

1. Atomic failure publication committed the job, attempt, remaining stage events
   and five audits together.
2. A repeated submission recovered the same deterministic native job without a
   second `jobs.insert`.
3. A stale revision and changed event identity rolled back; the original four
   events and five audits remained unchanged.
4. An injected pre-commit failure produced a correlated rollback and left the job
   running with only its initial event and no audits.
5. An injected post-commit failure produced a failed parent with a correlated
   successful COMMIT and was recovered as committed.
6. A deliberately discarded successful response was recovered from the original
   native job without resubmission.

The first failed-parent lookup may conservatively return `UNKNOWN` while BigQuery
child-job metadata becomes visible. The probe performs one read-only recovery of
that same job ID. It never turns `UNKNOWN` into permission for a new submission.

## Budget finding

BigQuery applies a 10 MiB minimum to each table-scanning script statement. The
workflow transaction has many independent assertions, so its cumulative script
budget must exceed the generic 64 MiB default even when fixture tables are tiny.
The controlled writer configuration now permits a bounded 512 MiB cumulative
statement budget; per-operation timeouts and deterministic job correlation remain.
The earlier lower-budget diagnostic attempts rolled back or stopped safely and
their owned datasets were deleted.

## Evidence

The passing report is
[`159840e0e18b49c0b761aca9e1366fcf.json`](../data/generated/workflow_writer_integration/159840e0e18b49c0b761aca9e1366fcf.json).
It records `PASSED`, six cases, `DELETED_AND_ABSENCE_VERIFIED`, zero serving row
writes, zero model calls and zero GCS reads.

Offline verification after the implementation changes:

- 139 workflow tests passed.
- 68 focused workflow-writer, failure, management and admission tests passed.
- Python compilation and `git diff --check` passed for the changed code.

The successful run changed no serving row metadata. No product, claim, approval,
campaign, sales, vector, artifact, IAM or runtime activation state changed.

## Fixed next step

Priority 4 is **6/7 complete**. Continue at **4.7 — High / Must have — shared
pilot**: protected campaign submission, bounded history, reconnect restoration,
safe status/output projections and protected downloads connected to the chatbot
UI. Company Google Workspace activation remains deferred until the enterprise
account, domain, groups and deployed IAP settings are available.
