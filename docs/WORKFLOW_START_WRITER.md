# Workflow start transaction foundation

Updated: 2026-10-11.

**Priority 4.6 — Critical / Must have — shared pilot** is in progress. The first
five small batches define the atomic submission boundary for one validated
`GENERATE` request and its initial `QUEUED` job, followed by a separate atomic
worker claim, complete evidence-snapshot publication and creation of the planned
planner invocation, plus guarded post-dispatch result/accounting publication.
They do not submit queries, call a model or persist rows by themselves.

## Implemented boundary

[workflow_start_query.py](../src/application/workflow_start_query.py) validates:

- a complete ACTIVE workspace, ACTIVE CREATOR membership and ACTIVE owned
  conversation from fresh server reads;
- a revision-one READY generation request and matching revision-one QUEUED job;
- one user access-decision audit with no invented mutation outcome;
- separate service `CREATE_REQUEST` and `CREATE_JOB` mutation audits;
- the seven-day retry window, exact request fingerprint, canonical SKU and all
  existing parent/scope contracts.

It then creates a deterministic native BigQuery job ID and a bounded stateless
query configuration with query caching and session creation disabled. Candidate
rows use the exact destination schemas through the existing workflow JSON codec.

[create_workflow_start.sql](../sql/create_workflow_start.sql) performs one
transaction that:

1. checks a 30-second plan deadline;
2. rereads the exact workspace, membership and conversation revisions/timestamps;
3. rejects inactive, revoked, changed-owner or non-CREATOR access;
4. rejects duplicate logical rows and retry-key/generation-intent conflicts;
5. accepts only an all-absent new write or an exact complete replay;
6. increments the ACTIVE workspace fence before inserting anything;
7. inserts the request, queued job and three audit rows together;
8. rolls back and rethrows every failed assertion;
9. returns only `APPLIED` or `REUSED` with exact stored-row projections.

The result validator compares all returned rows to the sealed plan. Provider
errors, missing results and ambiguous native job outcomes are intentionally not
classified here; the later executor must recover the original deterministic job
and must never infer noncommit from a transport error.

[workflow_attempt_query.py](../src/application/workflow_attempt_query.py) and
[claim_workflow_attempt.sql](../sql/claim_workflow_attempt.sql) add the distinct
worker-claim boundary. The plan revalidates the complete workspace, membership,
conversation, request and queued job, then proposes an ACTIVE workspace fence,
a revision-two `RUNNING` job, revision-one attempt and sequence-one
`RETRIEVING_EVIDENCE/STARTED` stage event. It creates separate service audits for
the job update, attempt claim and stage append. The transaction repeats the fresh
access and parent pins, accepts only an all-absent write or exact complete replay,
and rolls back partial/conflicting state. It contains no provider invocation or
model-invocation row.

[workflow_evidence_query.py](../src/application/workflow_evidence_query.py) and
[append_workflow_evidence.sql](../sql/append_workflow_evidence.sql) add the
evidence-publication boundary. The planner accepts only the complete validated
planning bundle from the 4.5 assembler, canonicalizes its explicit-schema rows,
binds them to the current RUNNING job and active worker attempt, limits capture
age and creates one `APPEND_EVIDENCE` service audit. The transaction rechecks
fresh access and exact request/job/attempt pins, fences the workspace and inserts
the entire snapshot and audit together. It accepts only an all-absent write or
an exact full replay; extra, missing or changed rows fail and roll back. Source
retrieval and GCS verification remain outside transaction retry paths.

[workflow_invocation_query.py](../src/application/workflow_invocation_query.py)
and [create_model_invocation.sql](../sql/create_model_invocation.sql) add the
pre-dispatch invocation boundary. The planner derives one revision-one `PLANNED`
PLANNER call from the request's exact pinned profile and binds it to the current
RUNNING job, active worker attempt and the exact stored evidence rows plus their
validated append audit. The transaction repeats fresh access and parent checks,
rejects another invocation ID or another PLANNER call for the request, fences the
workspace and inserts the invocation with a `CREATE_MODEL_INVOCATION` audit. It
accepts only all-absent creation or exact replay. The row has no dispatch receipt,
usage, response or cost. The existing protected 4.4 admission path must acquire
its separate durable dispatch claim before the installed model adapter may run.

[workflow_invocation_result_query.py](../src/application/workflow_invocation_result_query.py)
and [record_model_invocation_result.sql](../sql/record_model_invocation_result.sql)
add the post-dispatch boundary. A typed provider observation proposes one
revision-three `SUCCEEDED`, `FAILED`, `BLOCKED` or `UNKNOWN` invocation while
preserving the exact receipt, worker, profile and response identity. Usage fields
remain nullable; supplied estimates require Python `Decimal`, exact currency and
pricing version. Separate result and accounting audits bind the same revision.
The transaction repeats fresh access and exact parent/dispatch checks, fences the
workspace and accepts only the DISPATCHED-to-candidate update or exact replay.
`UNKNOWN` also moves the job and active attempt to `RECOVERY_REQUIRED` with two
additional mutation audits. A late provider response may still be recorded for
cost/accounting after the generation deadline; it does not authorize output.

[workflow_output_query.py](../src/application/workflow_output_query.py) and
[publish_workflow_output.sql](../sql/publish_workflow_output.sql) add successful
output publication. The plan consumes the exact successful planner invocation,
complete stored evidence bundle/audit and full eight-event history. Every embedded
artifact manifest must have a matching fresh trusted verification observation for
its GCS URI, generation, SHA-256, byte size and MIME type. Their canonical digest is
bound to the deterministic native job and returned result. The transaction fences
fresh access, inserts the original POC output and seven remaining events, completes
the active attempt and job, and inserts output/job/attempt/event audits atomically.
Only all-absent creation or exact replay is accepted. The planner constructs this
contract without reading GCS; the runtime executor must obtain these observations
from bounded generation-pinned verification before submission.

[workflow_failure_query.py](../src/application/workflow_failure_query.py) and
[finalize_workflow_failure.sql](../sql/finalize_workflow_failure.sql) add terminal
non-success publication. The plan accepts only an exact recorded `FAILED` or
`BLOCKED` planner invocation and a complete supplied stage prefix whose last event
has the same state, safe error and finalization time. It appends every event after
the pinned initial retrieval event, closes the active attempt and job without an
output, and writes job/attempt/event audits atomically. Exact replay is accepted;
partial histories, partial audits, existing outputs and mismatched terminal states
are rejected. It can record a late provider result but cannot infer failure from a
deadline or initiate another model call.

[workflow_writer_executor.py](../src/application/workflow_writer_executor.py)
connects all seven typed writer plans to a disabled-by-default native BigQuery
boundary. Submission first validates the configured project/region and all eleven
destination schemas, then looks up the deterministic job ID. An existing correlated
job is recovered; only authoritative `NotFound` permits one `jobs.insert` call.
Lost responses read that same original job without resubmission. SQL, parameters,
timeouts, location, project and parent-job identity must all match before a result
is interpreted, and exactly one row must pass the corresponding plan validator.
Failed scripts require correlated child COMMIT/ROLLBACK evidence; every other
uncertain condition remains `UNKNOWN`. The executor does not authenticate users,
create plans, call models or access GCS and is not connected to HTTP/UI serving.

## Verification

The focused query-contract tests cover deterministic plans, bounded SDK settings,
parameterization, access/parent/audit mismatch denial, exact `APPLIED`/`REUSED`
results, input immutability and absence of client/cloud calls. The affected
workflow, execution, access, workspace, membership, conversation, audit, codec
schema-preflight, native-job correlation and sharing-query suites pass:
**603 tests**.

The final isolated native probe is documented in
[WORKFLOW_WRITER_INTEGRATION.md](WORKFLOW_WRITER_INTEGRATION.md). Six native cases
passed against an owned expiring dataset: atomic terminal publication, stable
same-job duplicate recovery, stale conflict rollback, explicit rollback,
post-commit failure recovery and discarded-response original-job recovery. The
dataset was deleted and absence verified; serving row metadata remained unchanged.
The probe made no GCS or model call. A 512 MiB cumulative statement budget is used
for the assertion-heavy native script while each plan remains deadline bounded.

## Milestone status

**4.6 — Critical / Must have — shared pilot is complete** within the implemented
writer and isolated native verification scope. Serving activation remains disabled.
Continue at fixed milestone **4.7 — High / Must have — shared pilot** for protected
submission, bounded history, reconnect restoration and UI integration.
