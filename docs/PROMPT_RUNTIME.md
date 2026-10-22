# Exact registered prompt runtime

Priority 5, Batch 16 — **Critical / Must have / shared pilot**.
Complete within implementation and isolated native verification scope on
2026-10-14. All 19 planned native cases and 145 focused offline tests pass.
Serving execution remains disabled; the registered planner remains DRAFT.

## Resolution and dispatch

`prompt_runtime_reader.py` performs exact schema preflight and one fresh bounded
BigQuery transaction. It requires an ACTIVE workspace, current IAP-bound CREATOR
membership and an ACTIVE conversation owned by that principal. Runtime use does
not grant ADMIN or MANAGE_MODELS. It selects the unique global ACTIVE prompt key,
its complete registry history, shared lifecycle control/history, and this
workspace's current evaluation controls, original report anchor and complete cases.
Missing, duplicate, partial, corrupt, expired or cross-task state denies.

`prompt_runtime_contract.py` requires a PASSED evaluation for the same immutable
snapshot and historical REGISTERED revision, retained evaluation identity, valid
chronology and unchanged current benchmark/policy. The report's original control
anchor remains pinned even when identical policy content has a newer control
revision. Explicit installed full-profile hashes approve the entire requested
profile. Exact Vertex prompt ID/version, model, region, output/safety versions,
generation/thinking/tool settings, variables, output shape and safety settings
must match the installed application contract. This is an explicit server
allowlist, not the still-planned user-selectable M2 catalog.

`prompt_runtime_service.py` is disabled by default. It verifies IAP and fresh
CREATOR/owner access, captures BigQuery eligibility, reads only the selected exact
Vertex version with `vertex_prompt_reader.py`, then rereads access and BigQuery
state. Canonical content must match the complete stored snapshot. Edits at the
same Vertex address, changed selection/policy, unavailable resources or stale
observations deny. No latest-version, local file, cached export or repository
prompt can replace a failed authoritative read. Prompt rendering uses the
registered text and exact declared variables with bounded single-pass
substitution; braces inside evidence/user data cannot introduce substitutions.
This substitution check does not itself establish injection resistance or quality.

## Immutable invocation records

One separate explicit-schema append-only `ci_application.invocation_prompt_bindings`
table retains the full canonical snapshot, exact profile and eligibility pins:

- Vertex project/location/prompt/version and snapshot/template hashes.
- Current registry and shared lifecycle revision/digest.
- Evaluation ID/digest, current evaluation-control revision/digest and original
  evaluation-control anchor revision/digest.
- Benchmark/policy hashes and eligibility expiry.
- Requested model/settings/schema/safety versions and workspace, conversation,
  request, invocation, verified principal and binding time.

Its logical key is `(workspace_id, invocation_id)`. Canonical metadata and scoped
binding time produce an immutable SHA-256 binding. The original eleven workflow
table schemas remain unchanged. No product, evidence or creative rows are deleted.
The CLAIM_MODEL_DISPATCH audit records the atomic claim containing this binding;
the binding is joined by invocation identity rather than copied into audit logs.

`prompt_runtime_query.py` extends the existing dispatch transaction with complete
native source-set comparisons, native policy/report expiry checks, a write to the
shared lifecycle control row and explicit binding insertion. The shared write
preserves lifecycle content while providing a transaction conflict against
concurrent cross-workspace promotion. Duplicate bindings reject. Existing exact
invocation/request/attempt pins, workspace fence, dispatch state and audit commit
in the same transaction. Stable native job identity, original-job lookup/recovery
and uncertain-outcome handling remain those of the existing admission path.
There is no independent binding writer or second mutation job to recover.
Owned native verification now covers binding atomicity, original-job recovery,
denials and promotion-versus-claim overlap. The observed race conflict is at the
unpartitioned `workspaces` table; isolated lifecycle-control-only contention is
not claimed. See [native evidence and limits](#native-verification--2026-10-14).

Registered dispatch uses a 768 MiB cumulative byte cap, the existing 512 MiB
statement cap and 20-second statement timeout, and a 120-second native script
timeout. Its pending result wait is bounded at 90 seconds and remains capped by
the existing server operation deadline/cancellation signal. Legacy admission
retains its 512 MiB/60-second script/45-second result limits. All ten source
comparisons retain typed projections, symmetric set differences and exact counts;
one combined assertion and inline parameter-only expectations remove 19 native
statements from the original script and keep recovery below the 64-child bound.
No receipt, model dispatch permission or usage is inferred from a timeout.

## Trusted composition and limits

`ProtectedGenerationAdmission` supports explicit
`AdmissionSettings(require_registered_prompts=True)` together with an installed
`PromptRuntimeService`. Missing composition denies; HTTP/browser input cannot
install it. Existing full-profile admission remains mandatory. Preparation creates
the binding, preclaim resolution rechecks its exact pins, and successful first
dispatch rereads the committed binding plus exact Vertex/current eligibility.
Only an installed owned-worker adapter receives `(profile, resolved_prompt)` once.
`resolved_prompt.render(variables)` provides its verified registered instruction
and request text. The adapter remains responsible for verified evidence/images,
using those settings and the existing constrained output/finish validators.

Eligibility, stored binding and access are checked again before releasing the
adapter result. A late retirement/revocation/failure withholds release without
repeating the model call or inventing usage. UNKNOWN, lost/historical claims and
read-only recovery never invoke the model. Stops and deadlines retain the existing
operation budget and original IDs. These are fresh observation gates, not instant
distributed revocation or a proof that an installed adapter behaves correctly.

The legacy local POC and existing unregistered admission fixtures retain their
previous behavior. Shared registry-backed activation must explicitly install the
registered composition; it cannot switch back to the legacy path on an error.
No runtime adapter, worker, UI setting or service was activated by this batch.

Metadata-only resource commands, **subsequently run against GCP on 2026-10-12**:

```bash
.venv/bin/python -m src.ingestion.setup_prompt_runtime_tables --check
.venv/bin/python -m src.ingestion.setup_prompt_runtime_tables --apply
```

Setup preflights resources, creates only missing compatible tables, checks racing
creation and writes no data rows. Automatic migration/reset is unsupported.
The binding table's exact schema/layout passed and it contains zero rows.
Repeated `--apply` created nothing. All seven Batch 14–16 physical tables are now
verified; see [resource evidence](PROMPT_PHYSICAL_TABLES.md).

## Initial offline checkpoint — 2026-10-12

**530 distinct affected tests passed**, including **37 focused Batch 16 tests**:
8 contract, 6 reader, 11 service, 4 query/setup and 8 admission tests. Coverage
includes real offline ES256 signatures/tampering before BigQuery, exact native-job
matching, complete histories, policy/expiry/profile drift, original evaluation
anchors, exact Vertex mismatch, revocation, stored binding integrity, bounded
variables, atomic SQL construction, duplicate/lost/UNKNOWN claims and one-shot
provider failure. Compilation, dependency and changed-file whitespace checks pass.
SDK-shaped and mocked transactions do not prove native SQL or concurrent races.
During the initial offline implementation, no ADC, GCP API, billable model, GCS,
IAM, physical table or serving write occurred. Subsequent metadata-only physical
setup is documented separately above.

## Native verification — 2026-10-14

**Critical / Must have — shared pilot — COMPLETE within the isolated scope.**
The probe and synthetic fixtures were added in small implementation groups, then
the race helper and registered-dispatch fixes were verified with focused tests.
Three final PASSED reports close the 19-case manifest:

| Group | Cases | Evidence |
| --- | --- | --- |
| Core | 9 | [atomic binding and recovery](../data/generated/prompt_runtime_diagnostics/batch16_core_20261014/f42f09f3e1b245d489d2407c4cfe6ae0.json) |
| Denials | 8 | [expiry, policy, revocation and partial state](../data/generated/prompt_runtime_diagnostics/batch16_denials_20261014/48e8d331af3b4d8a80c957a18352252e.json) |
| Race | 2 | [promotion versus claim](../data/generated/prompt_runtime_diagnostics/batch16_race_20261014/10e56ecbdf70430aa6d5b0ce38e02468.json) |

[Combined verification](../data/generated/prompt_runtime_diagnostics/batch16_completed_20261014/verification.json)
records report/source SHA-256 values, manifests, nine correlated native fault/
denial observations and retained failures. Core checks full authoritative chains,
one atomic binding/invocation/attempt receipt/audit/workspace fence, exact stored
binding readback, duplicate and discarded-response original-job recovery,
rollback after binding insertion, failure after commit without an invented
receipt, absent-original UNKNOWN and duplicate-binding denial. Denials cover
native report expiry, a historical binding deadline, changed policy, membership
revocation, missing initial history, missing/corrupt stored bindings and a stale
claim after another workspace promotes the replacement. Exact sorted snapshots
of all 20 fixture tables match after each denied mutation. Policy publication
advances the workspace fence, so that stale policy claim is correctly rejected
by `DISPATCH_CONFLICT` before the runtime source comparison.

The final race proves **41.691 seconds** of actual native overlap, one committed
dispatch winner with complete binding state and positive promotion rollback.
All rows outside the winner's permitted scope are preserved. The conflict is at
the shared unpartitioned workspace table; this does not separately isolate the
lifecycle-control write fence. No adapter/model is invoked by the native probe.

All **145 focused tests** pass across runtime contract/reader/service/query/
admission/probe, generation admission, management/ControlIO and operation-budget
dependencies, including **24 probe guards**. Compilation and dependency checks
pass. No full suite was repeated. Deadline/cancellation tests verify that the
longer registered wait cannot extend the caller budget or release stopped results.

Native runs exposed the original 512 MiB script budget failure and 60-second
timeouts, including a timeout after commit with no receipt. The corrected bounded
configuration and shorter script are verified by the final reports. Thirteen
intermediate FAILED reports retain verified cleanup: fixture partition filters,
missing timeout pins, CLI class identity, incorrect policy-denial expectations,
expiring-table metadata adaptation, budget and timeout failures. One earlier
PASSED race remains a separate checkpoint, not a substitute for final-source
verification. All failed-run datasets were also deleted and absence verified.
The stored-binding fixture adapter verifies real owned metadata, then adapts only
the deliberate resource-reference/expiry differences for serving preflight;
physical expiry and genuine native job metadata are unchanged.

Each final owned expiring 20-table dataset was deleted and absence verified.
All 20 serving table row-count metadata values match before/after; this is not a
full serving-row checksum. Serving writes, Vertex/model calls, GCS reads, IAM/
deployment changes and activation were zero. Typed identities, perfect evaluation
observations and policies are synthetic. Native preparation uses a fixture
runtime resolver with production canonical binding checks; exact Vertex/provider,
one-shot adapter and result-release gates remain focused offline evidence, not
deployed IAP/HTTP or end-to-end chatbot verification. The deadline fixture tests
the immutable binding's native expiry guard, not live-approved policy expiry.

Priority 5 stays fixed at **16/16 implementation milestones complete**, with
Batches 14–16's planned isolated native gates now complete. Real Batch 13
registration remains DRAFT. Next is **Priority 6 — High / Must have**: measured
benchmark/evaluation and reviewed policy/thresholds before authorized promotion.
Shared UI/deployment and Gmail/Workspace activation retain their existing
Priority 9/12 gates. Personal ADC/sign-in does not grant application roles.
No seventeenth batch is introduced. See [implementation plan](IMPLEMENTATION_PLAN.md),
[lifecycle management](PROMPT_LIFECYCLE.md) and
[evaluation storage](PROMPT_EVALUATION.md).
