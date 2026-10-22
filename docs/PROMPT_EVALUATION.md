# Prompt evaluation storage and promotion gates

Priority 5, Batch 14 — **Critical / Must have / shared pilot**.
Implementation is complete and verified offline on 2026-10-12. The pure contracts,
explicit BigQuery schemas/codecs, resource setup, fenced write plans, original-job
recovery, authoritative reader and authenticated service are implemented. On
2026-10-13 the native storage/recovery slice passed 12 isolated cases and the eight
planned overlap/partial-state cases passed. **Batch 14 is complete within its
implementation and isolated native verification scope.** Measured quality remains
in Priority 6; serving activation has separate prerequisites. All four physical
tables were created and verified empty on 2026-10-12; see
[physical resource verification](PROMPT_PHYSICAL_TABLES.md).
Serving execution is disabled by default.

## Records

`src/application/prompt_evaluation_contract.py` defines immutable supplied records:

- A versioned task-specific benchmark pins sorted unique case IDs, input hashes,
  expected-result hashes, evidence hashes and expected ALLOW/BLOCK/NEEDS_REVIEW
  outcomes. Mandatory categories cover normal/ambiguous requests, missing evidence,
  pending/rejected claims, styling-only sources, synthetic performance, injection,
  cross-SKU evidence and stale approvals.
- A policy binds that benchmark, explicit Decimal thresholds, allowed returned
  models, evaluator version and expiry. No threshold defaults or approved live
  policy are supplied. Claims, evidence scope, injection resistance and safe finish
  retain their zero-failure boundaries; schema/grounding targets are policy inputs.
- Case observations preserve requested and returned model identity, invocation ID,
  response hash, observed outcome and strict boolean/unknown check values.
- An evaluation binds one REGISTERED registry record, complete policy/benchmark
  identities, evaluator/recorder, timestamps and case observations. Its canonical
  digest includes the exact snapshot/template hashes and historical registry
  revision/digest. Derived PASSED/FAILED/INCOMPLETE cannot be supplied by a caller.

All checks are required for each case. A runner must explicitly judge whether
negative cases safely block or require review, including their output/finish
contract; it cannot omit checks or count unknown observations as passes.
Observed outcomes must match the labeled outcome. Unapproved returned models
fail; unknown returned models remain incomplete. The model allowlist is explicit
policy, not an inferred alias mapping. Case inputs/evidence/expectations and
requested models cannot be rebound to another evaluation.

Rates are computed from observations, with exact integer comparisons against the
Decimal threshold's rational value. Low process Decimal precision cannot turn a
near miss into a pass. Duplicate cases/invocations, extra cases, unsupported types,
time regressions and invalid hashes are rejected. An omitted case or an unknown
observation cannot pass; a known safety/outcome/model failure remains failed even
when the rest of the run is incomplete. Canonical payloads retain the shared
64 KiB bound; case counts are capped at 128.

## Promotion gate

`validate_promotion_evaluation` checks a supplied full registry chain and supplied
evaluation/current policy/current benchmark before returning a gate for:

1. REGISTERED → EVALUATED.
2. EVALUATED → ACTIVE.
3. RETIRED → ACTIVE, retaining the original evaluation identity for rollback.

The exact evaluated REGISTERED record must occur in the chain; snapshot and
policy/benchmark must match. Evaluation must pass and remain unexpired at the
supplied check time. Activation/rollback require the existing evaluation ID and
reject evaluation-linked history before completion. The gate pins the current
registry revision/digest and target transition.

These pure records and gates do not authenticate, run a benchmark, verify model
dispatch or evidence bytes, prove scores, read authoritative state, persist results,
enforce one-active uniqueness or authorize activation. Unit fixtures are supplied
observations, not measured RAG quality. Later readers must fetch the authoritative
BigQuery records; lifecycle writers must recompute and transactionally fence the
gate. Local exports never supply an authoritative fallback.

## Explicit BigQuery storage

`src/application/prompt_evaluation_storage.py` defines four destination schemas in
`ci_application`, with explicit types and scoped clustering:

| Table | Purpose | Mutation boundary |
| --- | --- | --- |
| `prompt_evaluation_controls` | Current workspace/prompt benchmark and policy selection, canonical bindings, revision and digest | Compare-and-set only for the selected workspace/prompt |
| `prompt_evaluation_control_history` | Complete immutable control revision/hash chain | Append only |
| `prompt_evaluations` | Exact registered-version anchor, policy/benchmark/control pins, derived status and canonical full report | Append only by workspace/evaluation ID |
| `prompt_evaluation_cases` | Per-case observations, invocation/model/response pins and hashes | Append only with the complete report |

Strict codecs round-trip canonical payloads, recompute hashes and derive status
again from case observations. They reject noncanonical or duplicate-key JSON,
changed redundant columns, missing/extra/duplicate cases, incomplete control chains
and rebinding an existing benchmark/policy ID and version to different content.
Control histories are capped at 64 revisions; reports retain the 128-case and
64 KiB canonical-payload bounds. Historical evaluations retain their original
control binding even after the current policy changes.

`src/ingestion/setup_prompt_evaluation_tables.py` provides an opt-in resource check
or missing-table setup. It preflights all four existing tables before creation,
refuses incompatible schemas/layouts and never writes data rows:

```bash
.venv/bin/python -m src.ingestion.setup_prompt_evaluation_tables --check
.venv/bin/python -m src.ingestion.setup_prompt_evaluation_tables --apply
```

These commands were not run during the initial offline batch. They were
subsequently applied against GCP on 2026-10-12: all four schemas/layouts passed,
all tables were empty, and repeated setup created nothing. The
[resource evidence](PROMPT_PHYSICAL_TABLES.md) distinguishes those live checks
from the initial mocked tests and the still-pending native transaction probes.

## Authorized writes and uncertain outcomes

`prompt_evaluation_query.py` constructs explicit-schema, parameterized transactions
for control publication and complete report/case insertion. Native assertions
recheck ACTIVE workspace, exact verified IAP issuer/subject/principal, current
ACTIVE ADMIN membership revision and assertion expiry. New writes fence the
workspace, compare the complete captured control/history and, for evaluations,
the exact REGISTERED registry head/history. Expired or stale pins deny writes.
Transactions atomically append complete reports and cases, reject partial/conflicting
state and accept exact replay. Replaying historical controls preserves later head
changes. Only the selected control row and workspace fence may update; evaluation
records, cases and histories never update or delete unrelated rows.

`prompt_evaluation_store.py` is default-disabled. It preflights all four evaluation
schemas plus the registry and workspace/membership schemas. A deterministic native
job ID reserves one logical control revision or workspace/evaluation ID, including
competing content or actors. The store looks up that exact job before submission;
only authoritative NotFound permits one submission. A lost response reconciles the
original job without a second insert. SQL, parameters, project, region, settings
and native identity must match before accepting the business receipt. Failed
scripts require correlated child COMMIT/ROLLBACK evidence; unavailable, mismatched
or malformed outcomes remain UNKNOWN. A correlated COMMIT without a valid receipt
does not authorize result release.

Queries disable cache/session mode, cap cumulative billed bytes at 512 MiB and use
bounded RPC, result, script and statement settings. These limits do not substitute
for an owned-process HTTP deadline or prove native transaction races.

`prompt_evaluation_service.py` requires explicit enablement, verified IAP identity
and fresh BigQuery MANAGE_MODELS authorization before writes and after commit/read
before release. Revocation withholds the result even if the write committed. Its
read-only recovery method accepts a retained original typed plan, authenticates a
fresh assertion for the same principal/issuer/subject/audience and preserves all
original parameters, assertion expiry and native job ID. It never submits a new
job. Reconstructing a plan with a refreshed assertion can change the native query
configuration; use retained-plan recovery instead. Durable cold-restart plan
storage and automatic retries after known rollback are not supplied here.

## Fresh authoritative reads

The store reads current access, selected controls, complete control history,
report/cases and exact registry head/history in one bounded BigQuery transaction.
Counts and bounded arrays reject overflow, duplicates, orphan cases or partial
results; registry history includes its required partition filter. Native read-job
identity/configuration is checked before and after result consumption. No local
export or cache may supply missing authoritative state.

The authenticated service applies the promotion contract to this snapshot and
the current policy/benchmark, then rechecks access before returning an
`EvaluationPromotionCapture`. It pins workspace/prompt scope, current control
revision/digest, benchmark/policy hashes and the registry revision/digest in the
inner gate. This capture is not a write grant or a durable lock. The completed
offline [Batch 15 lifecycle writer](PROMPT_LIFECYCLE.md) revalidates and atomically
fences these control/registry pins, expiry and access, and enforces one active
version. Its native verification and activation remain deferred.

## Verification and limits

On 2026-10-12, **401 affected tests passed**, covering prompt, identity/access,
generation and UI/workflow flows. **63 focused Batch 14 tests passed**: 20 contract,
9 storage/setup, 6 write-plan, 13 store and 15 service tests. The completed storage/
service slice adds 43 tests beyond the earlier contract slice. Coverage includes
real ES256 signed-IAP offline fixtures, tampering/revocation denial, exact replay,
lost-response original-job recovery, native-job/config mismatch, partial records,
policy changes, expiry and FAILED/INCOMPLETE promotion denial. Provider RPCs and
native results are mocked; this is not deployed IAP or native concurrency evidence.
Repository Python compilation, dependency and changed-file whitespace checks pass.

During the initial offline implementation, no ADC/cloud/model calls, physical
table creation, serving writes, IAM changes, deployment or activation occurred.
Subsequent physical setup is recorded separately above. No reviewed live benchmark/acceptance policy,
measured quality scores or benchmark runner is invented. The broader RAG E1/E2/E4
work remains open. Batch 14's planned native checks are now complete. Live use
still requires reviewed measured evaluation and the exact prompt's lifecycle
prerequisites. Operator IAP/ADMIN setup and DRAFT registration are already verified;
they do not activate this evaluation service or campaign generation.

## Fixed next steps

Priority 5 stays fixed at **16 batches, 16/16 implementation milestones complete
within documented scope**. Batch 13's real IAP/ADMIN DRAFT registration is verified.
This probe does not complete all native or quality activation gates.

Batch 14's planned native work is complete. No extra Batch 14 work item is added.

1. **Batches 15–16 native gates — Critical / Must have — shared pilot:** lifecycle
   promotion, active replacement, blocking/retirement and rollback; then dispatch
   binding, promotion versus claim, stale pins and original-job recovery. Use
   synthetic owned fixtures without model dispatch.
2. **Priority 6 — High / Must have — shared pilot:** labeled benchmark and measured
   retrieval/grounding/schema/citation/banner/adversarial evaluation, authoritative
   BigQuery results and reviewed acceptance policies/thresholds before activation.

Keep implementation groups at 2–3 files. Company Workspace/shared marketing UI SSO
remains in the existing plan; personal Gmail operator sign-in does not activate
shared campaign generation. See [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md).

## Native storage/recovery slice — 2026-10-13

[prompt_evaluation_integration.py](../src/validation/prompt_evaluation_integration.py)
uses production write plans/store and read SQL/codecs with allowlisted relocation.
Eight exact tables reside in one labeled owned dataset with a two-hour default
table expiry. Setup/preflight checks ownership, schema, partitioning and clustering;
cleanup rechecks ownership and verifies dataset absence. Uploads use destination
schemas and zero upload retries. Explicit ADC opt-in:

```bash
.venv/bin/python -m src.validation.prompt_evaluation_integration \
  --run --output-dir data/generated/prompt_evaluation_integration
```

The **12 native cases passed**:

- Atomic control/history publication and authoritative read.
- Same-control-job recovery without submission; new-job exact reuse.
- Atomic report plus ten case rows and full codec decoding.
- Same-report-job recovery and new-job exact reuse.
- Conflicting report rollback; explicit rollback without partial rows.
- Discarded-response recovery of the original job.
- Post-commit failure: correlated COMMIT/stored report preserved, no invented
  business receipt and no recovery submission.
- Expired report/assertion denial.
- Control replacement, stale-control denial and historical replay without head
  regression or rebinding historical evaluation.
- Revocation blocks replay; unrelated fixture workspace/membership rows stay exact.

Seven expected native assertion codes were independently validated against the
recorded commit classifications; future probe runs enforce this automatically.
A rollback from incidental SQL/SDK failure cannot pass a denial case. This final
report-validator addition was checked against recorded native evidence without
another cloud call.

**Verification:** 85 distinct affected offline tests pass: 63 Batch 14 tests,
14 new probe guards and eight registration-probe dependency guards. Compilation
and dependencies pass. Dataset deletion/absence is verified; all eight serving
table row counts match before/after metadata. This is not a full serving-row
checksum. Serving writes, Vertex/model calls, GCS reads, IAM/deployment changes
and activation were zero.

Evidence:

- [Successful native report](../data/generated/prompt_evaluation_integration/7b7656bb65ad45f98707aad3857600d3.json).
- [Native assertion verification](../data/generated/prompt_evaluation_integration/7b7656bb65ad45f98707aad3857600d3_assertion_verification.json).
- [Retained failed SDK-load run](../data/generated/prompt_evaluation_integration/f2498ed931a04083aaad4cf09cecfa85.json)
  and [failed empty-parameter run](../data/generated/prompt_evaluation_integration/35384dab7e2e47c2a413feac8bb21b5a.json).
  Both stopped before evaluation transactions, remain FAILED and verified cleanup.
  The two fixture-only SDK/config defects have regression tests.

Typed identities and benchmark/policy/observations are synthetic fixtures. Their
derived PASSED status tests storage semantics; it is not a measured model score,
approved live policy or business approval. This slice does not verify deployed
IAP/service execution, concurrency, partial/orphan rows, lifecycle transactions,
dispatch binding or marketing UI activation.

## Planned Batch 14 completion — overlap and partial state — 2026-10-13

The separate [remaining-gates module](../src/validation/prompt_evaluation_remaining.py)
is selected explicitly by the existing probe:

```bash
.venv/bin/python -m src.validation.prompt_evaluation_integration \
  --run --remaining-only --output-dir data/generated/prompt_evaluation_remaining
```

The code group contains that module, nine focused guard tests and the probe's
opt-in CLI routing. Production evaluation queries, stores, services and schemas
did not change. One owned labeled dataset holds eight exact expiring tables;
independent fixture scopes are seeded once, without resets or row deletion.
Fixture identity contexts are renewed between cases only; no JWT is minted and
these supplied identities do not authenticate a user.

**All eight native cases passed:**

| Case | Required observed behavior |
| --- | --- |
| Competing control updates | Actual transaction overlap, one complete revision-two head/history winner, one positively rolled-back loser |
| Conflicting reports for one evaluation ID | Actual overlap, one exact report with all ten case rows, one positively rolled-back loser |
| Control head without history | Reader and control/report writers deny; exact stored rows stay unchanged |
| Control history without head | Reader and control/report writers deny; no implicit head repair |
| Control history missing revision one | Reader and control/report writers deny; no chain repair or new publication |
| Report without case rows | Reader and replay writer deny; no implicit case completion |
| Report missing one case | Reader and replay writer deny; no added or overwritten case |
| Orphan case without report | Reader and report writer deny; no report created or orphan removed |

Race proof uses bounded, correlated native child jobs and intervals from successful
BEGIN completion to successful COMMIT/ROLLBACK start. Both races genuinely overlap;
each winner has APPLIED/COMMITTED and each loser has correlated NOT_COMMITTED with
the expected concurrent-update failure. Parent SQL/config and project/region/job
identity remain pinned through the production store. Distinct fixture native slots
deliberately exercise SQL fencing; these races do not change production job IDs.
Unknown classifications, incidental SQL errors, missing/truncated child evidence
or merely simultaneous submissions cannot pass.

Winning controls/reports are fully decoded and compared to the winning typed
proposal. Exactly one workspace-fence increment is required. Every other fixture
row outside the permitted winner scope is compared exactly. Six corrupt-state
reads return the expected contract error; nine writes have the expected native
assertion plus correlated rollback. Sorted, bounded native JSON snapshots of all
eight fixture tables match before/after each corrupt-state case. No serving repair
or delete behavior is introduced.

**Verification:** all 94 affected offline tests pass (85 earlier affected tests plus
nine new guards), compilation/dependencies pass, and all eight new native cases
pass. The earlier 12 native cases were not repeated: together the two reports cover
the **20 planned Batch 14 native cases**. Dataset deletion and absence are verified.
All eight serving table row counts match before/after metadata; the four evaluation
tables remain empty. This serving check is metadata, not a full serving-row checksum.
Serving writes, Vertex/model calls, GCS reads, IAM/deployment changes and activation
were zero. No native failure or production defect was found in this final slice.

[Native completion report](../data/generated/prompt_evaluation_remaining/890e7e50bdf94a5daacbbd6d6d52e58d.json)
records both transaction intervals/child evidence, six read errors, nine verified
native assertions, serving counts and verified cleanup.

This completes only Batch 14's planned implementation/isolated native work.
Batches 15–16 native verification, measured quality/reviewed policy under Priority 6,
deployed service/marketing SSO and serving activation retain their existing gates.
Synthetic case observations are not measured quality or business approval.
