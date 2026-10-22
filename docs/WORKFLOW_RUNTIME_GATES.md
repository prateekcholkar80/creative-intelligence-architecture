# Priority 4.4 — runtime recovery and protected API gates

**Critical / Must have — shared pilot.** Implemented on 2026-10-11. Final
verification is recorded below. Serving execution remains disabled; this
implementation does not activate company identity, deploy HTTP, or connect the UI.

## Protected reads and audit correlation

[WorkflowReadApi](../src/application/workflow_read_api.py) uses the existing
bounded ASGI admission, origin/header/body checks, cancellation and joined worker
supervision. Its explicitly prestarted
[WorkflowReadHost](../src/application/workflow_read_host.py) invokes a fixed
installed worker; ADC, IAP, BigQuery and GCS work stays inside that process.

| POST route | Required JSON body | Successful result |
| --- | --- | --- |
| `/v1/workflow/get_job_status` | `workspace_id`, `job_id` | Minimal current job state |
| `/v1/workflow/get_output_summary` | `workspace_id`, `job_id` | Minimal POC output metadata |
| `/v1/workflow/download_artifact` | Above plus `artifact_id` | Verified bytes with safe filename/MIME/hash headers |

Reads authenticate the observed IAP assertion, resolve current parent-conversation
access in BigQuery, and recheck access before release. Downloads retain the
existing full parent/output validation and pinned GCS generation/hash/size/format
checks, then recheck the manifest after GCS. Bytes cross owned IPC in bounded
16 KiB chunks; the host verifies total size and SHA-256 before HTTP delivery.
Responses are `no-store`; no GCS URI, JWT, email, prompt or complete evidence is
included in these projections.

[AccessCapture](../src/application/access_capture.py) validates the captured
access decision, resolved target and exact audit schema. The server-created HTTP
request ID crosses the supervisor/worker boundary and equals the captured audit
trace. Invalid assertions produce anonymous denials without resource metadata;
authorization denials retain only the validated private observation. Captures are
kept in a bounded server-only handoff, not used as authoritative serving state.
Delivery diagnostics are separate from business commits. General durable read
audits remain under 4.5–4.6; no successful HTTP delivery is treated as proof that
such an audit has been published.

## Controlled management

[ProtectedManagementService](../src/application/management_service.py) adds these
disabled-by-default, fixed-project operations:

- ADMIN member creation/change and explicit mapping-based synchronization.
- ADMIN workspace disable/reactivate.
- CREATOR private conversation creation with actor-derived ownership.
- CREATOR/owner archival, blocked while related jobs are QUEUED, RUNNING or
  RECOVERY_REQUIRED. Archived conversations cannot restore through this path.
- Explicit operator bootstrap of one ACTIVE workspace and its initial member.

The new `MANAGE_WORKSPACE` and `ARCHIVE_CONVERSATION` actions are additive to the
existing access policy. ADMIN does not gain content access. Membership states stay
ACTIVE/SUSPENDED/REVOKED; workspace states stay ACTIVE/DISABLED.

Bootstrap has no default owner or role assignment. A trusted installed manifest
pins the IAP subject, workspace, explicit roles including ADMIN, command and a
maximum five-minute window. Native global workspace slots prevent competing
owners from creating independent initial transactions. Slots 2–3 require positive
correlated rollback for every predecessor. Missing/UNKNOWN evidence cannot permit
a retry. Existing or partial workspace records deny creation; there is no reset
or repair operation. Retained read-only bootstrap recovery requires the installed
operator manifest; removing it revokes that recovery authority. This operator
grant does not grant access to campaign content.

Synchronization uses an explicit installed subject/roles/status mapping with a
fresh verification window. Caller role/mapping overrides are rejected. It is the
controlled mapping application boundary, **not a Google Workspace Directory
connector or activated domain/group synchronization**. Those deployment inputs
remain deferred under priority 12.

Every normal mutation captures a bounded consistent native snapshot, validates
complete destination rows, checks current signed identity/permission, pins the
entire membership set and relevant conversation, and updates the workspace fence.
The SQL rechecks actor, controls, time window and exact before-images in the same
transaction. At least one ACTIVE ADMIN must remain. Candidate and APPLIED audit
are inserted/updated atomically using explicitly typed destination fields; no
MERGE, unrelated deletion, inferred schema or local serving fallback is used.

[WorkflowControlApi](../src/application/workflow_control_api.py) exposes only
`/v1/control/manage` and `/v1/control/recover_management`, through its own fixed
owned worker. Bodies use closed fields per command; a browser cannot supply a
plan, actor, owner, SQL, timeout or native job configuration. Responses contain
only scope, original native job ID/status and verified mutation summary. Cold
recovery reconstructs the original server plan from its exact native job, checks
current authorization and never registers it for submission. A failed script
without correlated commit/rollback children stays UNKNOWN. A committed script
with no verified result reports commit status without inventing a resulting row.

## Generation admission and dispatch

[ProtectedGenerationAdmission](../src/application/generation_admission.py)
operates on **existing BigQuery PLANNED invocation and parent rows** for
INTERPRETER and PLANNER. Missing/duplicate records deny admission. It validates
full request/job/attempt contracts, current ACTIVE workspace/conversation,
CREATOR/owner access, assigned worker, remaining job deadline and the installed
allowlist of complete profile hashes. It does not create workflow rows.

The claim transaction pins native before-images, checks uniqueness of retry key/
generation intent and stage calls, updates the workspace fence, assigns the
PLANNER attempt receipt, changes PLANNED to DISPATCHED and appends the validated
service audit atomically. A global invocation job slot and private service
incarnation receipt prevent another worker from inheriting a winner's envelope.
An inherited service in a different OS process cannot reuse that incarnation for
write/dispatch; it must be composed again in its owned worker.

Only a fresh first local submission with a correlated COMMITTED/APPLIED result,
unchanged authoritative claim and fresh access may invoke the installed trusted
adapter once. Local admission is consumed **before** uncertain native I/O. Known
jobs, UNKNOWN, missing result rows, read-only recovery and repeated entries cannot
call the adapter. There is no model call inside a transaction/retry loop. Recovery
can outlive the dispatch deadline or profile retirement while retaining current
user access checks; it never becomes dispatch permission.

Billable dispatch requires the trusted owned-worker operation context. This is a
composition check, not authentication or independent proof of OS supervision.
The actual adapter must be composed in the owned process, use the pinned limits
and disable provider mutation replay when 4.6 connects model execution/results.
No adapter is installed by default and no generation HTTP route is enabled here.
The existing local POC generation path is unchanged and is not protected by these
separate shared-service gates. General creation, results, UNKNOWN reconciliation,
accounting and grounded evidence publication remain 4.5–4.6.

There is an unavoidable interval between committing a claim and sending a model
request. A process stop there may leave DISPATCHED without a confirmed provider
call. Recovery withholds another call rather than claiming exactly-once model
execution. Usage/cost stay NOT_REPORTED/NOT_ESTIMATED with null values, not zero.

## Query bounds and verification

[ControlIO](../src/application/control_io.py) checks fresh exact destination
schemas before write proposals/submission. SQL/SDK settings cap the whole script
at 512 MiB, each statement at 128 MiB, each statement at 20 seconds and the native
job at 60 seconds. Cache/session mode and mutation retries are disabled. Root
operation deadlines still cap every call; a pending original job may be awaited
for at most 45 seconds. Native-job classification checks project, region, ID,
exact SQL and typed parameters/configuration before examining outcomes.

The native probe uses only an expiring, uniquely named owned fixture dataset.
It checks ownership and schemas before writes and verifies deletion/absence.
Its explicit synthetic actors do not prove deployed IAP/IAM. Non-billable callback
fixtures, where used, are not Gemini calls.

```bash
.venv/bin/python -m unittest discover -s tests
.venv/bin/python -m src.validation.workflow_control_integration --run --output-dir data/generated/workflow_control_integration
```

Full regression checkpoint: **1,631 tests passed** using the command above.
This broad run is justified by changes spanning policy, schema decoding, IAP,
audits, transports, process supervision and HTTP. Earlier focused checks included
133 read/control/contract/transport tests and 95 sharing/read/control/worker tests.
The full run includes correlation/recovery tightening. A final process-incarnation
guard was subsequently added and is covered by the focused verification below;
the full suite was not repeated for that isolated admission change.
The final affected-flow run passed **62 tests** in 7.345 seconds:

```bash
.venv/bin/python -m unittest tests.test_generation_admission tests.test_workflow_read_api tests.test_workflow_control_api tests.test_management_service
```

Syntax parsing and dependency consistency pass.

All **23 native checks passed** across these four isolated fixture groups:

| Group | Passed checks | Report |
| --- | --- | --- |
| Management | 8 | [9376ae084475466b8e86cb48cb2181e7](../data/generated/workflow_control_integration/9376ae084475466b8e86cb48cb2181e7.json) |
| Dispatch/recovery | 6 | [57cf5a238f8c4244a5e56aec0d4e3095](../data/generated/workflow_control_integration/57cf5a238f8c4244a5e56aec0d4e3095.json) |
| Adversarial transactions/cold recovery | 7 | [67d2a706ddba42c8bebf718ccea34fc5](../data/generated/workflow_control_integration/67d2a706ddba42c8bebf718ccea34fc5.json) |
| Non-billable installed callback | 2 | [6abb2832e7374b2ba97f997f2e632798](../data/generated/workflow_control_integration/6abb2832e7374b2ba97f997f2e632798.json) |

The two base groups both include unrelated-workspace preservation. These are
23 executed checks, not 23 distinct business scenarios. The admin race verified
overlapping native transaction intervals and one surviving ACTIVE ADMIN. Duplicate
intent/stage checks rejected claims atomically. The callback group invoked one
local fixture adapter once after actual native claim verification; recovery and
another entry could not invoke it again. Every owned dataset was deleted and
absence verified. Serving writes, model calls, secret accesses, IAM changes,
product/approval changes and default activation were zero.

The groups used `--management-only`, `--dispatch-only`, `--adversarial-only` and
`--callback-only` respectively; omitting these flags runs every group. Reports
are audit exports only, never serving state.

Native failure reports remain retained: initial sandbox DNS failure created no
resource; early script/read byte allowances rejected transactions. Their owned
datasets were deleted and absence verified. These failures led to the final
bounded settings above; no failure was reclassified as success.

**4.4 is complete within implementation/isolated verification scope. Priority 4
is 4/7 complete, 3/7 remaining.** Defaults stay disabled.

Deployment IAP/ingress, real service-account permissions, populated-table latency,
actual provider invocation, ASGI server behavior and deployed process teardown are
not verified by these fixtures. Company identity activation remains priority 12.
BigQuery transactions provide the native atomic boundary; see
[Google's transaction documentation](https://docs.cloud.google.com/bigquery/docs/transactions).

## Fixed next milestone

1. **4.5 — Critical / Must have — shared pilot:** Complete fresh BigQuery/GCS
   evidence projections and audit producers for the allowed planning context;
   recheck eligibility, provenance, image pins, observation identity and replay.
2. **4.6 — Critical / Must have — shared pilot:** Connect explicit-schema durable
   workflow/evidence/result/artifact writers and the owned model adapter. Verify
   fencing, duplicate workers and uncertain outcomes without model replay.
3. **4.7 — High / Must have — shared pilot:** Connect protected submission,
   paginated history, job/conversation restoration and downloads to the chat UI.

These are the existing fixed IDs. No additional Priority 4 milestones are added.
