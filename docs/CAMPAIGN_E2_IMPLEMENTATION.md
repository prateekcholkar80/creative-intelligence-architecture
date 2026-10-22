# E2 deterministic checker and runner checkpoint

Latest checkpoint: 2026-10-16. E2 is in progress; E1 remains complete within its reviewed scope.

## Catalog, selection, transcript and provider authority — 2026-10-16

**Critical / Must have — execution binding/shared pilot.** Successive 2–3-file
code groups implement a separate opt-in full-authority diagnostic path:
`campaign_quality_authority.py`, `campaign_quality_authority_reader.py`,
`campaign_quality_authority_writer.py` and `campaign_quality_bound_execution.py`.
The entry point is `run_authoritative_quality`; legacy `run_current_quality`
remains the explicitly limited persisted-request diagnostic path described below.
Neither is installed in the chatbot or permits response/artifact release.

The new reader captures current access, requests, product catalog, selection
authority, invocations, jobs, attempts and invocation/prompt bindings in one
bounded BigQuery transaction. Request validation has an explicit supplied-catalog
parameter; legacy defaults remain the original 20 SKUs. Only the new reader opts
in using current ACTIVE product rows with exact source provenance: BEAUTYFEEDS
for CI-SKU-0001–0020 and NAKED_AND_THRIVING_OFFICIAL for CI-SKU-0021–0023.
Missing/conflicting/inactive catalog rows deny. IDs are never remapped. Generic
validators still do not prove catalog authority; only the fresh reader supplies it.
Legacy generation/admission/writer catalog behavior has not been activated or
widened. This removes the new diagnostic reader's catalog restriction, not every
shared-workflow integration gap.

One immutable `ci_application.campaign_request_authorities` row per request stores
the versioned accepted-input fingerprint, actor/scope, exact country/platform,
SKU/audience/formula selection, claim-use references and ordered transcript hash
manifest. It stores no raw user or assistant messages. Exact selections must match
the diagnostic request and compatible stored request/preferences. Unresolved
free-text-only SKU/audience/channel is not inferred; known explicit interpretation
preferences may be captured before dispatch. Refinement remains unsupported.

The transcript manifest reconciles every stored user message hash and successful
prior response hash through the selected request's ordered history prefix. It
rejects missing/changed/duplicate turns, ambiguous responses and prior uncertain
outcomes. A first structured-input request can have an explicitly empty manifest;
its input fingerprint is not relabelled as a chat utterance. This proves linkage to
stored hashes, not retrieval of unseen raw chat text, rendered-message fidelity,
or completeness of unpersisted local UI turns. Installed dispatch/UI adapters must
persist/hash every turn they actually use; they cannot treat this as permission
to add unrecorded context. Country/platform selection is not market/legal clearance.

The disabled server-only publisher requires explicit CREATOR workspace authority
and owner READ_DRAFT access, verified-identity metadata and authoritative source
capture before preparation. Its proposed row is validated on a separate copy;
the source reader never substitutes proposed data. The fenced transaction checks
current identity/roles/ownership and exact native source-row strings, reserves one
global native job ID per request, appends an immutable selection record and bumps
the workspace fence. It refuses post-dispatch capture. Exact existing records
have a REUSED SQL branch; conflicting selections cannot overwrite a row.
Submission/recovery uses the existing ControlIO original-job machinery and current
access checks. Plans are server-held internal objects, never browser payloads.
UNKNOWN/known rollback never creates another native slot. Recovery needs the exact
original plan; no durable intent store, cold plan reconstruction or automatic
retry is added. There is no separate audit_events action; the immutable selection
row records its principal and transaction timestamp. It does not grant permissions.

`run_authoritative_quality` requires initial/final identical authority captures
with distinct native read IDs. It reads current claim permissions and independently
reconciled evidence on both sides of checking, enforcing exact wording,
qualifications, formula/SKU, country/platform/audience and approval windows. Empty
claim selections do not certify that arbitrary candidate prose is claim-free;
remaining semantic checks and release holds still apply. Review metadata does not
prove original evidence/GCS bytes. Fresh source/rights/byte verification remains
required wherever actual reports or images are consumed.

The provider binding requires the selected durable successful invocation, validated
parents/profile/accounting, atomic dispatch-time prompt binding, current exact
eligibility pins, returned model, STOP/PASSED outcome and exact SHA-256 response
bytes. Selection capture must precede dispatch binding. An installed server
annotation adapter returns `(candidate, observed_response_text)`; it must not invoke
a model. Candidate text must equal the pinned response text. Structured planner
JSON-to-copy projection is unsupported until a deterministic versioned projector
is implemented. This binds recorded intent/dispatch/outcome, not independent proof
of every byte sent to the provider or a new model-dispatch path.

**Verification:** 439 affected offline tests pass, including 33 new tests across
catalog/authority/read/publish/compose/setup and permission-reader regressions.
Compilation/dependencies pass; a later targeted contract rerun also passes. An
initial test command named a nonexistent module; it was corrected to the existing
ControlIO test class and all selected tests passed. Native metadata setup created
the single new authority table in asia-south1; reapply created nothing, and both
responses reported metadata row count zero. No selection/claim/approval rows,
model calls, IAM/deployment changes or activation occurred. Positive source tests
are SDK-shaped/synthetic observations, not live stored-context or deployed-IAP
verification. New authority/publisher SQL transaction/race/recovery verification
is still pending; do not infer it from the existing ControlIO native probes.

The new path is implemented offline and its storage resource exists, but no live
request-authority capture or end-to-end campaign is claimed. Writers remain
disabled; the planner and acceptance policy remain DRAFT. E2 is incomplete,
41 quality checks remain unavailable and full benchmark execution remains 0/159.
Next within the existing binding step: isolated native authority/read/publication/
race/recovery verification and the required stage-specific structured-response
projector/dispatch input adapters, then existing remaining check, benchmark and
per-interaction release integration. Shared UI/admission installation must preserve
the explicit catalog/transcript/selection boundaries above.

Setup (schema only, never seed authority records):

```sh
.venv/bin/python -m src.ingestion.setup_campaign_quality_authority --check
.venv/bin/python -m src.ingestion.setup_campaign_quality_authority --apply
```

## Mandatory request binding in diagnostic execution — 2026-10-16

**Critical / Must have — execution binding/shared pilot.** A three-code-file
group connects `read_request_context` to `run_current_quality`, updates the prior
execution tests and adds ten focused composition tests. There is no optional
capture argument or supplied-history fallback. After initial access, the operation
reads the authoritative request capture before policy/prompt/product reads or
candidate annotation. Captured canonical JSON/hash, shape, scope, principal, history
kind, counts, revisions, native job ID and freshness are checked at the boundary.

Supplied input and conversation hashes must match the capture. GENERATE input is
the accepted structured-input fingerprint; INTERPRET input is the stored message
hash. Conversation hash means the complete current persisted-request history,
including the selected request, not raw user/assistant transcript text. Selected
SKU must match, operation/stage/status must be compatible, unresolved SKU/channel
denies, clarification requires NEEDS_CLARIFICATION and refinement stays unsupported.
Exact platform must be compatible with the stored coarse channel family (EMAIL
or PAID_SOCIAL); this does not prove exact-platform selection or country authority.

The checker runs before final policy/prompt and request rereads. The second request
capture must have a distinct native read ID, a nonregressing observation after
checking/context reread, and identical complete bindings. Request record/revision,
accepted input, history membership/order/content, access snapshot, status, SKU and
channel changes withhold the result. Final access, overall/prompt/product freshness
and both original/final request expiries are checked; refreshing the capture never
extends the original expiry. No atomic snapshot across independent readers or
instant concurrent revocation guarantee is claimed.

The versioned `campaign-quality-execution-v2` diagnostic context binds the capture
hash/content and both native job IDs. Coverage explicitly records ORIGINAL_20_SKUS,
PERSISTED_REQUEST_RECORDS, transcript_authoritative=false and
market_and_exact_platform_authoritative=false. Release remains false. The workflow
reader is unchanged: current real N&T requests cannot pass its original catalog
validation, and a legacy SKU cannot be relabelled as N&T. Closing that compatibility
gap needs explicit future integration; no catalog, schema or request records are
silently migrated. Transcript coverage and country/exact-platform authority remain
required for interactions relying on them. DRAFT policies still deny release.

All **265 affected offline tests passed**, including the ten new composition tests;
focused reruns cover subsequent freshness/stage guard changes. Syntax/dependencies
pass. Positive execution tests use explicitly synthetic reader-interface captures
to isolate composition; they are not valid stored N&T rows or live execution proof.
Separate real-decoder/SDK-shaped tests prove legacy/N&T denial. No cloud/IAP calls,
model calls, writes, schemas, approvals, chatbot wiring or activation occurred.
E2 remains open: 41 checks are unavailable and full benchmark execution is 0/159.

Next within the existing E2 binding step: resolve authoritative catalog/stage/
market/channel and transcript coverage without inference, then add applicable
claim/evidence and durable invocation/provider-outcome bindings. Native verification,
remaining quality checks, measured benchmark and actual release integration remain
separate required work; no new priority or milestone is introduced.

## Persisted-request authority reader — 2026-10-16

**Critical / Must have — execution binding/shared pilot.** The first two-file
code group adds `campaign_quality_request_reader.py` and 21 focused offline tests.
One fixed-project/region BigQuery transaction captures current workspace,
membership, conversation and all stored requests in that conversation. It requires
the selected exact request, fresh verified identity, ACTIVE owner-scoped READ_DRAFT
access, valid workflow-v1 records and matching stored request fingerprints.
It does not infer membership from ADC, an email address or a browser role.

An independent original-job fetch pins native SQL, parameters, requested billing/
timeout/session/cache settings and successful terminal status before decoding its
results; metadata is checked again afterwards. Provider mutation cannot redefine
expected settings. Automatic RPC/job retry and query caching are disabled. Limits
are 50 complete request records, 64 KiB per row, 1 MiB total capture, 256 MiB billed
bytes, 60-second script/result waits, 20-second statements/RPCs and a 60-second
freshness window measured from before submission. Late captures deny; these
freshness limits do not promise hard end-to-end process cancellation. Overflow
denies rather than silently truncating or paging an incomplete context.

The immutable server-only result binds accepted-input fingerprint, nullable stored
message hash, exact selected record/revision, access snapshot and the ordered full
stored request history. Each history record contributes its full canonical-row
hash. Native job ID and observation expiry accompany the binding. The compact
projection omits preference text, identity subject and model-profile contents.
History hashes change when another request appears or a stored outcome/revision
changes; timestamps and native read IDs do not silently redefine content hashes.

**Scope limits:** history kind is explicitly `PERSISTED_REQUEST_RECORDS`, including
the current request, not a chat transcript or reconstructed request-time prefix.
The existing tables store message hashes, not raw user/assistant text; GENERATE
uses a structured accepted-input fingerprint, not a fabricated message hash.
The unchanged workflow-v1 contracts still support the original 20-SKU catalog.
New N&T records, unsupported refinement inputs and incompatible rows deny; this
reader does not widen legacy writers/readers or authorize catalog migration.
Country and exact marketing-platform resolution are not established by this
capture. Repeated reads are independent snapshots, not cross-reader atomicity.

All **220 affected offline tests passed**, including **21 new reader tests**;
compilation and dependencies pass. Real SDK-shaped QueryJobs exercise correlation,
provider-config mutation, metadata drift, count/order/size/expiry and access
denials; this is not live BigQuery/IAP verification. No cloud reads/writes, model
calls, schemas, approvals, policy state, UI wiring or activation changed.
At this standalone-reader checkpoint, `run_current_quality` still accepted supplied
request/history pins; mandatory composition is completed in the later checkpoint
above. The reader itself grants no response release. E2 remains
open, 41 rules remain unavailable and full benchmark execution remains 0/159.

At that checkpoint, the next 2–3-file code group within E2 was to connect these
authoritative captures to diagnostic execution, compare scope/content/revisions before and
after checking, and reject supplied-pin drift. Explicit catalog/stage/channel
compatibility and missing transcript authority must deny rather than be inferred.
Claim/evidence and durable invocation/provider-outcome adapters follow in the
existing execution-binding sequence; live verification and release wiring remain
required separately.

## Implemented behavior

`campaign_quality_checks.py` checks actual candidate substrings against supplied
source fields. Six deterministic components cover A01 product identity/size,
G01 bounded literal support, G03 SKU scope, G07 supplied observation windows/access,
G10 citation source/field/hash pins, and U06 channel-field completeness. These are
components of the reviewed criteria, not a complete implementation of those rules
for arbitrary language, images or source eligibility.

Candidate text, structured sections, stage, request, SKU, country and channel bind
the candidate hash. Immutable sorted nonoverlapping spans identify exactly which
text each source field supports. Missing fields/sources, mismatched values, changed
source pins, cross-SKU scope, expired/future/stale observations and denied source
access cannot silently pass. Canonical JSON and explicit byte/count bounds reject
malformed or oversized inputs. No case labels or case IDs are used for prediction.

Only fully covered literal text with matching source fields can pass the bounded
support component. Extra prose and structured-section content stay inconclusive
unless actually checked; checking channel keys does not certify their content.
Absence of prohibited keywords is never treated as proof of safety. Forty-one
rules have no implementation in this profile and remain UNAVAILABLE. As a result,
this profile cannot produce global PASS or release a response.

`require_release` independently recomputes the report, verifies exact candidate/
evidence/report bindings and rechecks the supplied source window at release.
Changed candidates, forged PASS reports, stale sources and regressed release
windows withhold release. This is a pure primitive, not an installed chatbot gate.
It performs no authentication, source reads, model dispatch or artifact publication.
SourceFact values are supplied server observations, not proof of current BigQuery
access. Current authoritative read/admission integration remains required.

## Current N&T product-fact adapter — 2026-10-15

`campaign_quality_product_reader.py` adds a separate fresh exact-SKU BigQuery
read for CI-SKU-0021–0023. It projects only product name and size from the existing
product master; it does not widen legacy BEAUTYFEEDS or permission readers.
One ACTIVE official N&T row is required. Missing/duplicate rows, wrong provenance,
non-Decimal/nonpositive sizes and mismatched native SQL, parameters, configuration,
job identity or outcome deny. Reads have explicit billing/time/result bounds and
no query cache, retry, model calls or local fallback. Observation validity starts
before the provider call and expires after 60 seconds; slow reads do not renew it.

The opt-in `check_current_product` service uses an already server-verified identity
and the installed access reader to require current parent-conversation READ_DRAFT
access before reading. A trusted server annotation function receives the current
fact pins and must return a candidate for the same SKU. It must preserve actual
candidate text; mismatched text fails the checker. It is not an untrusted model
callback. Access is reread after annotation, and changed or denied decisions
withhold the result. The context digest binds candidate/report/native job and the
returned access decision. This is not an atomic access/product snapshot, a durable
audit, or the complete policy/prompt/model execution context. Another fresh access
check remains required at eventual release.

This adapter returns diagnostics and facts only, with release_allowed=false. The
60-second observation bound is a conservative engineering expiry, not a reviewed
quality/promotion threshold. Other rules remain unavailable. No benefit, formula,
clinical evidence or scientific claim eligibility is inferred from product metadata.
It is not installed in the chatbot. Policy/prompt/model pins and permission-source
adapters remain pending. Native BigQuery execution and deployed IAP behavior have
not been verified for this adapter.

**32 focused offline tests passed**, including **14 new adapter tests**; compilation
and dependency checks passed. This slice did not repeat the full suite or previous
unaffected E1 checks. No cloud calls/writes, model invocations, approval changes,
deployments or activation occurred. See
[adapter verification](../data/generated/campaign_e2_diagnostics/product_adapter_verification_20261015.json).

## Fresh policy/prompt execution context — 2026-10-15

`campaign_quality_execution.py` composes the existing authoritative DRAFT-policy
reader, approved-runtime prompt reader, N&T product-fact reader and current access
reader into an opt-in diagnostic operation. It requires installed server settings,
a verified identity, explicit request scope and a trusted annotation function.
The operation checks access first, reads policy and prompt eligibility, reads the
product and checks the candidate, then rereads policy/prompt and checks access
again before returning diagnostics. Candidate request ID, SKU, stage, country and
channel must match. Interpretation/clarification require an interpreter template;
generation/refinement require a planner template. Model profiles must pass the
existing exact prompt/model/settings/schema/safety binding and installed profile
allowlist. DRAFT/ineligible prompts deny before candidate construction.

The final context digest binds request/history hashes, candidate/evidence/report,
product native job ID, access decision, policy header/rules/snapshot and complete
runtime eligibility pins plus the model-profile hash. Changed authoritative policy
or prompt-control revisions withhold the result; they are never silently rebased.
Access is checked after final provider reads too. Capture freshness, source expiry,
prompt eligibility expiry and the overall observation window are checked at the
final clock. The 60-second bounds are engineering freshness limits, not promotion
thresholds or hard process/RPC cancellation guarantees.

This is **diagnostic composition only**. The only supported acceptance-policy
storage format remains DRAFT, so release_allowed is always false. Context digests
are integrity bindings, not signatures, durable receipts, access grants or proof
that supplied request/history digests came from authoritative conversation rows.
The operation does not invoke a model, establish a durable invocation binding,
verify a returned provider model/outcome, approve claims or check rendered assets.
Independent reads are not an atomic global snapshot. A deployment must install
trusted request/history capture and owned annotation code; these dependencies
must never come from browser/model-supplied callbacks or roles. No local/shared
chatbot route is wired to this operation yet. The current live planner remains
DRAFT and cannot pass its ACTIVE-template gate.

Review also corrected the earlier product reader: native job configuration includes
SQL and may contain server-generated destination/default metadata. Validation now
checks SQL separately and pins every requested configuration field, while allowing
that returned metadata. Expected settings are copied before submission so provider
mutation cannot redefine the check. Result reads explicitly disable RPC/job retry.
An actual BigQuery SDK QueryJob fixture exercises the native response shape. This
is offline SDK verification, not a live BigQuery integration result.

**77 focused offline tests passed**, including **14 new execution tests** and two
SDK/configuration regression tests, plus
compilation/dependency checks. These include the affected product/checker/runner,
policy reader and prompt-runtime contract/reader tests. No full suite or live
cloud/IAP/model checks, writes, policy changes or activation occurred. See
[execution verification](../data/generated/campaign_e2_diagnostics/execution_context_verification_20261015.json).

## Repeatable partial baseline

Run:

```sh
.venv/bin/python -m src.validation.campaign_quality_runner --output data/generated/campaign_e2_diagnostics/partial_baseline_20261015.json
```

The runner validates the reviewed E1 digest and artifact pins. Six existing actual
E1 candidate texts (identity and incorrect-size examples for the three N&T products)
are annotated with source-field spans in a separate executable fixture package.
Official source projections are pinned. Access/freshness and clock are explicitly
simulated for frozen replay; there is no live source authorization or model output.
Expected labels are consulted only after check execution; tests prove changing a
gold label does not change the checker prediction.

The partial baseline matches **6/6 A01 projections**, but only **3/6 global E1
decisions**, because the three identity responses still require unavailable checks.
These are not full-case successes: **0/159 full E1 cases have been executed**.
The report explicitly lists component coverage and unimplemented rules and states
PARTIAL_E2_BASELINE, e2_complete=false and release_allowed=false. There is no model
quality score or agreed promotion threshold. No complete E1 benchmark pass is claimed.

Precision/recall-at-K and no-match metrics are implemented with separate EXACT_SKU
and SEMANTIC identities. Duplicate IDs are rejected; empty denominators remain
undefined rather than invented as perfect/zero scores. These helpers do not run
retrieval. Actual ranked retrieval quality and live exact-SKU reader benchmarks
have not been measured; retrieval metrics in the baseline remain null.

## Verification and remaining work

114 affected offline tests passed, including 18 new checker/runner tests. Syntax
and dependency checks passed. No full suite, cloud reads/writes, model invocations,
policy approvals, deployments or activation occurred. An initial tuple-based hash
input failed the existing strict JSON serializer; source pins now use a canonical
list of dictionaries without changing the runtime serializer.

Next E2 work, within the existing milestone:

1. Mandatory initial/final persisted-request captures and supplied-pin rejection
   are implemented offline in diagnostic execution. Resolve authoritative
   catalog/stage/market/channel compatibility and missing transcript authority explicitly;
   no implicit legacy scope widening. Complete claim/evidence adapters, durable
   invocation/provider-outcome pins and native verification. Operational acceptance-
   policy eligibility and full execution binding remain pending.
2. Implement remaining claim/evidence, numeric-study, safety/injection, intent,
   usability and multimodal checks. Check failures/unavailability must retain
   BLOCK/CLARIFY/NEEDS_REVIEW behavior; no label-driven prediction or implicit PASS.
3. Annotate and execute the full reviewed benchmark with actual candidate outputs,
   and run exact-SKU retrieval separately from semantic ranking. Record coverage,
   false positives, missed failures and unavailable checks honestly.
4. Wire fresh checks into every enabled interpretation, clarification, generation,
   refinement and artifact-release path; verify denial before returning text/bytes.
   Existing local POC code and disabled shared serving were not rewired here.

E4 persistence/threshold work and E5 actual rendered-artifact checks remain separate
requirements. Existing DRAFT policies cannot authorize serving or benefit claims.

[Implementation plan](IMPLEMENTATION_PLAN.md) · [Reviewed E1 scope](CAMPAIGN_E1_SIGNOFF.md)
· [Partial baseline](../data/generated/campaign_e2_diagnostics/partial_baseline_20261015.json)
· [Checkpoint verification](../data/generated/campaign_e2_diagnostics/verification_20261015.json)

## Native authority verification and planner adapters — 2026-10-16

**Critical / Must have — shared pilot, existing P6 E2 scope.** Isolated native
verification uses 14 owned expiring tables, including a three-column product
projection fixture. It exercises the production authority SQL and ControlIO with
fixture-scoped references/job IDs. Exact source multiset checks now use grouped
comparisons and canonical scalar JSON parameters, avoiding BigQuery's unsupported
FULL JOIN array scan and ambiguous empty-array job metadata. Partitioned sources
have explicit complete-range filters; exact request/conversation predicates remain.
The original failed report is retained, with verified cleanup.

`campaign_planner_projection.py` implements `campaign-planner-copy-v1`: duplicate
JSON keys, extra/missing fields and values outside the supplied enum options deny.
It preserves the raw response hash and projects headline, optional body and CTA in
fixed order. Only permitted null body maps to the explicit empty option.
`campaign_planner_inputs.py` prepares immutable rendered prompt/input pins from an
exact ResolvedPrompt and fresh authority capture. Ordered supplied raw turns must
match every stored transcript hash. It pins evidence/options, invocation binding,
rendered text and image asset/generation/MIME/size/byte hashes. Its diagnostic
outcome bridge validates the durable successful provider response and projects copy
against the prepared options. No raw transcript is stored in the manifest.

These are server-owned diagnostic adapters, not authenticated HTTP input types.
Evidence/image provenance still depends on the trusted source reader; supplied
bytes are not independent GCS verification. The prepared manifest is not persisted
atomically with dispatch and does not prove receipt of inputs by the provider.
`run_authoritative_quality` still accepts exact text only: installing the structured
bridge into that runner requires durable prepared-input authority first. No model
was dispatched, no chatbot was installed, and unpersisted/local raw transcript
coverage remains incomplete. Original workflow catalog defaults remain unchanged.
Policies/planner remain DRAFT; 41 checks remain unavailable and full benchmark
execution remains 0/159. No release permission follows from this verification.

Next, within existing E2: persist and reread exact dispatch-input manifests with
invocation admission; compose the structured bridge with initial/final authority,
access and permission checks; then verify stored raw transcript ingestion and
per-interaction release integration before chatbot installation. Remaining checks,
benchmark/retrieval measurements and E4/E5 remain their existing requirements.

Native result: **12/12 PASSED**, 18.799 seconds transaction overlap, verified
fixture deletion/absence and unchanged serving metadata counts. **191 affected
offline tests pass**, compilation/dependencies pass. [Native report](../data/generated/campaign_authority_diagnostics/b84ca2830cfb4d6c8eb07c01c3359c4f.json)
and [combined verification](../data/generated/campaign_e2_diagnostics/native_planner_adapters_verification_20261016.json).

## Durable planner inputs and retained transcript bodies — 2026-10-16

**Critical / Must have — existing P6 E2 execution binding.** The opt-in
`ProtectedGenerationAdmission` composition accepts a server-installed
`planner_input_builder`. It inserts `invocation_input_manifests` within the same
transaction as prompt binding, invocation/attempt dispatch claim, audit and workspace
fence. Exact plan reconstruction rejects a removed/changed input parameter or
changed server-held envelope. SQL checks current immutable selection, binding and
a 60-second preparation window. Fresh stored-manifest verification surrounds the
single provider callback; recovery never dispatches. The legacy callback and catalog
defaults remain unchanged. The new callback receives `(profile, resolved, prepared)`;
installation is explicit and default serving remains disabled.

`run_authoritative_quality(..., prepared_inputs=...)` now supports the versioned
structured projector. It rereads the exact stored manifest before/after checking,
compares selected authority/catalog/transcript pins, verifies the durable successful
response bytes and constrains candidate text AND sections to the projected copy.
Current access, policy, prompt, product and independent claim/evidence checks still
surround execution. Input/projection digests enter the context record. These guards
do not implement the remaining semantic, visual or channel deliverable checks and
never grant release on unavailable checks.

The user selected **30-day raw chat retention**. The separate
`campaign_transcript_bodies` table partitions on recorded_at with 30-day partition
expiration and mandatory partition filters. Readers enforce the exact rolling
30-day cutoff as well. Opt-in authority publication accepts `transcript_texts` and
writes the exact hash-matched ordered batch atomically with the authority row;
replays require the same complete retained batch. The existing global job slot is
preserved: a prior hash-only publication cannot silently acquire raw bodies through
an alternate slot. Retention expiry withholds transcript use, rather than fabricating
missing messages. Native partition expiration is asynchronous and partition-based;
application eligibility uses exact timestamps. No raw text enters diagnostic files.
The raw bodies remain private application records, not prompt instructions.

`prepare_from_stored_transcript` composes fresh owner-authorized authority reads,
stored-body verification and immutable planner-input preparation. It never fills
missing turns from local/browser history. Unpersisted chat remains unsupported.
The original server-held prepared envelope is still needed for dispatch/outcome
verification; cold reconstruction is not added. Source evidence/image authorization
still belongs to trusted source adapters; hashes alone do not authenticate provenance
or prove a provider consumed the bytes. Raw retained records require the protected
writer/reader paths; no public transcript endpoint is installed.

Both new BigQuery tables were created/verified with explicit schemas; no business
rows were seeded. Metadata setup is separate from the existing runtime-table command.
Chatbot installation, deployed IAP exercise of these adapters and serving activation
remain incomplete. Planner and acceptance policies stay DRAFT; release remains
withheld, with 41 unavailable checks and 0/159 full benchmark executions.

The original three follow-ups now have implementation paths in the disabled server
composition. Remaining verification/integration belongs to the existing E2/E4/E5 and
Priority 9/12 gates: verify the combined protected admission/capture path natively,
complete remaining quality checks and measured benchmarks, then install each enabled
interaction's guarded release path before any shared chatbot generation activation.

Verification: **220 focused offline tests pass**, compilation/dependencies pass.
**6/6 isolated native extension cases pass**; all 13 jobs were independently
verified terminal and fixture deletion/absence verified. Concurrent submissions
have one winner; interval overlap was not measured by this extension probe.
[Native evidence](../data/generated/campaign_planner_inputs_diagnostics/ce153f6baed04da583f74d332f03efac.json) ·
[Combined evidence](../data/generated/campaign_e2_diagnostics/durable_planner_inputs_verification_20261016.json).

## Full pipeline benchmark focus — 2026-10-18

User prioritizes full pipeline benchmark completion within P6 E2/E4 and the fixed
MVP. Intake browser checks remain acceptance gates; they are not the active
engineering focus. No access/release/approval gate is waived.

**High / Must have, shared pilot:** two local implementation groups add a 159-case
execution inventory with scoring labels separated from model inputs, source/case
hashes and preserved reviewed scopes; and an opt-in all-rule observation adapter
inside authoritative quality execution. Six deterministic checks remain unchanged;
41 semantic observations run in three bounded batches. Every payload is validated
before dispatch; no criteria/evidence truncation or context cap increase. Complete
transcript/candidate bindings precede the observer; existing final source/access/
context/deadline checks still gate return. Default serving remains unchanged.

51 affected offline tests, compilation and dependency checks pass. Tests include
all-rule coverage, candidate/evaluator drift, invented quotes, synthetic positive
composition and context change during semantic observation. An initial single
41-rule payload exceeded the existing budget; bounded batches correct that defect.
No provider calls, cloud writes, deployment, approvals or activation occurred.
A fresh read-only BigQuery preflight verifies exact planner/intent/questions v1
snapshots (all DRAFT) and both pinned policy sources. The current semantic evaluator
hash differs from the saved calibration, so those observations cannot qualify the
current evaluator. The wrapper has a separate composed evaluator hash.

This is integration preparation, not a qualifying run. Full qualifying execution
remains **0/159**. Only current product projections reach the new semantic hook;
missing study/claim/visual support is never invented. No live observer is installed;
durable judge outcomes/usage and real stage/provider/source/retrieval/artifact
observations remain required. Inventory: 94 isolated-rule, 49 scenario and 16
request-only cases. The latter 65 need explicit reviewed qualification-scope mapping;
no isolated label becomes a global release label. Text identity fixtures do not
receive blanket pixel requirements. Frozen candidate replay is not stage execution.

Next fixed work: implement the evaluation-only observer with exact prompt/profile
and durable judge outcome/usage; connect real stage/request/provider/source adapters;
complete source applicability and case-scope mapping; measure actual applicable
retrieval/rendered artifacts; execute/score the 159 cases and qualify through the
existing protected publisher/reader. Preserve unknown outcomes without automatic
retry. Present exact policy/prompt versions for authorized approval only after
qualifying results. DRAFT status never grants serving execution or release.

Evidence in `data/generated/campaign_e2_diagnostics/`:
`pipeline_benchmark_inventory_corrected_20261018.json`,
`pipeline_benchmark_pin_preflight_20261018.json`,
`pipeline_benchmark_integration_checkpoint_20261018.json`.

## Durable evaluator observer — 2026-10-18

**Critical / Must have, P6 E2/E4, shared pilot; local code and isolated native scope.**
The user asked to start the durable evaluation observer and connect one real stage.
Small implementation groups add an immutable BigQuery evaluator store, a default-
disabled owned-worker observer, a pinned one-attempt Vertex transport, and the
interpretation-stage connection in authoritative quality execution. The hook uses
actual captured stage invocation/response, binding, authority and candidate hashes;
it does not rewrite the unfinished live request or fabricate a completed intent.
Only INTERPRETATION is admitted by this observer version.

Each run/stage-invocation/rule batch has one stable native reservation job. Changing
candidate/input/evaluator/profile bytes conflicts in that same slot. A newly
verified reservation precedes transport; replay/uncertainty never permits another
model call. Lost reservation response gives no dispatch permission. Exact outcomes
store raw response/hash, returned model, safety/finish state and nullable token
usage; unknown does not become zero. Provider failure records UNKNOWN, mismatched
metadata records REJECTED, and neither returns a usable assessment. Original
outcome-publication plans are retained before writes for exact lost-response
recovery. Cold recovery reads original native jobs; no cold redispatch or new slot.
Transport disables tools/automatic function calls and SDK retries, caps provider
wait to the owned deadline and returns provider-observed metadata. No transport was
called against a live model in this slice.

The all-rule wrapper advances to campaign-full-quality-observation-v2, binds the
interpretation candidate/scope, includes three durable chunk receipts, and retains
all existing final context/access/policy/deadline checks. Default non-opted-in
serving behavior is unchanged; semantic observations never grant source authority,
evaluator qualification or release. The two new table definitions are
ci_application.campaign_evaluator_intents and campaign_evaluator_outcomes, clustered
by record_id with required partition filters and 30-day partition expiry. These
production tables and grants are NOT provisioned. Expiry metadata is not proof of
complete privacy enforcement, especially original native-job metadata retention.

**Verification:** 69 affected offline tests, compilation and dependencies pass.
Six owned native BigQuery storage cases pass: commit/exact replay/cold recovery,
changed intent denial/exact snapshot, lost reservation with cold UNKNOWN and no
new dispatch permission, lost outcome with cold recovery/exact replay, outcome
rollback with exact unrelated preservation, and competing same-native-job admission
with one new reservation permission. This is not a distinct-job transaction-race
claim. The owned dataset was deleted and absence verified. Native fixture records
are synthetic; no model calls, serving writes, IAM/deployment or approval changes.

**Still pending:** provision exact tables/narrow access; assemble the trusted
protected evaluation host/guard and exact current judge profile/template; implement
or connect evaluation-only stage admission. Existing serving execution deliberately
requires eligible ACTIVE/profile authority and must not be bypassed just because
registered stage prompts are DRAFT. No observer is installed and no live stage has
run. The connection tests are synthetic, not proof of a real interpretation run.
Full qualifying pipeline execution remains **0/159**.

**Next fixed batches:** (1) exact table/least-privilege setup and protected evaluation
composition, preserving disabled serving defaults; (2) complete evaluation-only
admission and run one genuine interpretation case, persist actual stage and judge
outcomes and prove reconnect/original-job recovery; (3) extend the verified adapter
to other stages and required evidence/retrieval/artifacts, resolve reviewed scope
mapping, then execute/score/qualify the 159 cases. Present exact prompt/policy
versions and qualifying results before authorized release approval.

Evidence: data/generated/campaign_e2_diagnostics/evaluator_storage_native_20261018.json
and durable_evaluator_checkpoint_20261018.json in the same directory.

## Protected interpretation evaluation checkpoint — 2026-10-18

**User checkpoint:** “I am not available now, keep this as a checkpoint.” Stop
before signed-in execution. No browser/model action may be inferred from this.

**Critical / Must have, P6 E2/E4, fixed MVP:** step 1 complete within configuration
scope: exactly campaign_evaluator_intents and campaign_evaluator_outcomes created/
verified in ci_application, with required partition filters, 30-day partition
expiry and registrar-only exact get/getData/updateData table grants. Existing
bindings preserved. Existing ciClarificationPolicyReader grants only get/getData
on the single product_master source table. A custom ciEvaluationModelInvoker
role has only aiplatform.endpoints.predict and a time-conditional registrar grant
now without a time condition, until explicit revocation (user-directed correction verified on 2026-10-18). No broader Vertex
role or reviewer assignment was made; effective runtime access awaits IAP action.

Step 2 deployed/server-verified: immutable digest
sha256:3b1cce9d4bf3af48be65853deec753a7d50238ae284ea029f9fabc3faa9129e8,
Cloud Build 7b1c5c0b-6c61-4cd2-9f44-6c1bb9e61199, registrar revision
ci-prompt-registrar-00013-b9g serving 100%. Exact image, two intended environment
changes, unchanged IAP/invoker policies, runtime/secrets/resources/concurrency/max
instances and anonymous HTTP 302 denial pass. Registration/bootstrap remain
false, measurement/approval publication adapters remain disabled, marketing
serving/generation/release are unchanged. Evaluation recording is a separate
explicit capability, not policy write permission or production activation.

The protected controller uses fresh IAP/explicit publisher and native workspace/
membership checks plus exact current registered snapshot/policy/product reads.
It admits one fixed request-only INTERPRETATION evaluation case through a separate
non-serving lane; DRAFT registration is allowed only for this explicitly configured
evaluation, never by relaxing existing ACTIVE/profile serving gates. Browser
parameters cannot choose scope, model, case, prompt, response or principal. The
case is e1-closure-missing-report, run interpretation-pilot-20261018-v1, intent
5933813566455939072/v1. It uses benchmark text and product metadata, not the saved
unfinished campaign request or uploaded unverified material. Exact hashes/model
settings are pinned in config/evaluation/interpretation_pilot_deployment.json.

Stage, three bounded judge chunks and combined 47-check assessment retain original
native slots. Combined assessment is durable, so recovery returns identical
assessment and provider receipt hashes. Recovery cannot reserve missing judge
chunks or dispatch providers. Current source/access drift withholds results.
The operator action returns bounded receipt/model/usage metadata, not campaign
copy or approved material. All qualification/release flags remain false.

116 affected offline tests, compilation and dependencies pass. Synthetic controller
run/reconnect tests verify identical records with one stage/three judge calls and
no added provider calls on recovery. These are not live model observations. Initial
transport tests used a wrong expected exception/module selection, corrected before
validation. Renderer tuple handling and native timestamp encoding defects were
caught and regression-tested before deployment. First build
613b07ad-13ed-4209-badb-b59591a5c61d is retained as REJECTED BEFORE DEPLOYMENT;
only the corrected build was deployed. Earlier six native store cases and owned
cleanup remain valid; no serving workflow rows were rewritten by setup.

**Step 3 not run:** Chrome control was denied; user is unavailable. No real
interpretation/judge call or signed-in reconnect was performed or verified.
Full qualifying benchmark remains **0/159**. No prompt/policy approval or campaign
release occurred. Complete raw retention, including native job metadata, is not
proved by table partition settings alone.

**Resume here:** reconcile current cloud/config/authority and exact pins; inspect
original evaluator records/jobs before any dispatch; verify the temporary prediction
grant is still effective (do not silently renew an expired window). In the signed-in
console choose Operator status, reconnect, verify measurement runtime/authority,
then run the configured interpretation evaluation. Reconnect again and recover
original records. Obtain the displayed receipts/error and independently verify
canonical stage/judge/assessment rows, exact native jobs/runtime creator and
provider usage plus absence of new model calls on recovery. Preserve UNKNOWN and
original slots. Only after the vertical slice is verified continue other stage/
evidence/artifact adapters and measured 159-case qualification; no approval bypass.

Combined evidence/checkpoint:
data/generated/campaign_e2_diagnostics/interpretation_evaluation_checkpoint_20261018.json.
Native setup, pins, build, rejected build and deployed checks are linked there.
Rollback preserves data/IAP and restores revision 00012-276's immutable image,
removes the evaluation config environment variable and restores the prior evaluator
hash; exact values are in the checkpoint. No rollback was performed.

Evaluation access correction — 2026-10-18 (Critical / Must have): user requested no time-based grant. The exact prior registrar member was migrated to an unconditional ciEvaluationModelInvoker binding, retaining only aiplatform.endpoints.predict and preserving unrelated IAM entries. Native readback passed; product reader remains verified. Three focused migration/idempotency tests pass. No model call, deployment, approval or release change. Signed-in run/recovery remains pending; qualifying benchmark 0/159. Evidence: data/generated/campaign_e2_diagnostics/evaluator_model_access_explicit_revocation_20261018.json.

Operator failure diagnostic correction — 2026-10-18 (Critical / Must have): user reports OPERATOR_OPERATION_UNAVAILABLE after Reconnect operator runtime. Root cause of that live failure remains UNRESOLVED: reconnect itself sends no operation, while this code is emitted only by an unexpected worker-operation exception. Recent registrar native authority job workflow_control_read_eed3b48d4aee47ad92905e213bb81163 is DONE without error; independent read-only source query confirms one ACTIVE workspace/member and native decoding, not IAP execution. Reading another creator's result was denied and was not bypassed. Worker now returns bounded exception-category/source-line codes without raw messages or values. Separate confirmed packaging defect corrected: benchmark loader requires both acceptance policy documents and the exact catalog import checkpoint metadata, absent from the preceding image. Isolated load first failed on missing v0.1 policy; corrected load and container build load all 159 cases. No local metadata substitutes for live source authority. 29 focused tests and compilation pass. Build 3fe25420-874b-4f64-986e-6408c947d173 succeeded; immutable image sha256:f9af446ca7cef1bc94b42ec8cdb893b37a8a2ddd6a8b74a2eea5784220b54b92 deployed as ci-prompt-registrar-00014-6kt, 100%. All 12 deployment checks pass, preserving environment, IAP/IAM/resources and disabled registration/bootstrap/release. No model call or approval. The build used an isolated explicit file context; the historical repository-root ignore file remains unsuitable for this nested evaluation package. Next: fresh signed-in reload/reconnect, verify measurement runtime/authority, capture any new safe error code; then configured original evaluation and recovery only after authority succeeds. Full qualifying benchmark remains 0/159. Evidence: operator_diagnostics_build_source_20261018.json and operator_diagnostics_deployed_checks_20261018.json under data/generated/campaign_e2_diagnostics/.

Real interpretation pilot stopped by invalid judge coverage — 2026-10-18 (Critical / Must have): user supplied signed-in measurement success (workspace revision 4, membership revision 2, publisher verified; all measurement/approval/release writes disabled). One actual interpretation plus two judge provider responses were durably written with returned gemini-3.5-flash and nullable usage. All six original intent/outcome jobs independently match exact canonical bytes, SQL/config, registrar creator and terminal success; outcome hashes bind intents/response bytes. First 16-rule judge validates. Second 16-rule judge returns only two checks (C11/U01) and a proposed PASS; authoritative SEMANTIC_RESULT_COVERAGE rejects it. That proposed PASS is invalid and grants nothing. Third judge and combined assessment do not exist. Worker previously masked ContractError as ACCESS_DENIED; fixed allowlist exposes evaluation validation codes only, retaining masked identity/authority denials. 32 focused tests pass. Build 842b6fbe-9f6a-46c3-955d-14b75a93a351 passed; image sha256:5a77b12a6e9e24034634db4dd6eb878245f3c18357f2c14057c07f2d368de14e deployed as ci-prompt-registrar-00015-c8s at 100%, all 12 preservation/IAP deployment checks passed. No additional model calls, original rewrites, approval or release. Next: fresh signed-in reconnect and Recover original records (expect SEMANTIC_RESULT_COVERAGE with no provider redispatch); independently verify unchanged originals. Then tighten and explicitly version the judge contract/new attempt while retaining this failed run and its recoverable version; complete all judge batches/assessment and signed-in recovery before expansion. No minItems/maxItems contract tightening or new evaluation version is implemented in this batch. Qualifying full pipeline benchmark remains 0/159. Evidence: interpretation_denial_verified_20261018.json, interpretation_original_records_denial_20261018.json and evaluation_coverage_deployed_checks_20261018.json in data/generated/campaign_e2_diagnostics/.

Signed-in recovery of failed pilot verified — 2026-10-18 (Critical / Must have): user reloaded/reconnected and reports Recover original evaluation records returned SEMANTIC_RESULT_COVERAGE. Independent current BigQuery comparison verifies all three original intent rows and all three original outcome rows unchanged byte-for-byte, including recorded timestamps; recent registrar jobs show only the six original write jobs, with recovery reads and no new reservations/outcomes. Combined with the recover-only path (which cannot dispatch providers), this verifies recovery of the failed result without redispatch within this scoped observation, not provider billing reconciliation or a successful complete evaluation. Failure remains second judge 2/16 checks; third batch/assessment absent. No model call by agent, mutation, deployment or approval in this verification. Qualifying full benchmark 0/159; release false. Next fixed MVP batch: version and tighten the judge response contract, preserve original failed run/recovery version, prepare a distinct explicit attempt and then verify all batches/assessment plus signed-in recovery before expansion. Evidence: data/generated/campaign_e2_diagnostics/interpretation_failed_recovery_verified_20261018.json.

## Versioned complete-coverage judge correction — 2026-10-18

Critical / Must have within fixed Package 1 / P6 E2-E4. Four small batches preserve the original failed pilot and avoid provider retries:

1. SemanticCatalog defaults to the original v3 hash/system/payload; opt-in campaign-semantic-checkers-v4 adds explicit required_rule_ids/order and exactly one check per supplied criterion. Pinned transport v2 applies minItems=maxItems for each requested batch (16/16/9). Existing application validation still rejects missing, duplicate, reordered, extra, unsupported-evidence or fabricated-quote results. Legacy v1 transport/schema is unchanged. 29 initial focused tests passed.
2. Interpretation controller/settings bind contract version and transport/system hashes; recovery-only configuration denies execution before capture/transport. Synthetic complete assessment and cold recovery pass for both contracts. 34 affected tests passed.
3. Server-held config now contains distinct corrected run interpretation-pilot-20261018-v2 and original v1 recovery-only settings. Fixed UI choices switch trusted worker/measurement settings; original has no Run action and server denies dispatch. Browser supplies no run/model/authority parameters. Lane-switch and wire tests pass.
4. Build allowlist includes nested configs, both exact acceptance policy documents and only the required catalog import checkpoint metadata. Container build validates all 159 cases and both evaluator hashes; no metadata becomes current source/claim authority.

Final 59 focused tests, compilation and dependency checks pass. Added SDK test initially treated dict-valued response_schema as a Schema object; corrected test validates it with the actual SDK Schema model and confirms exact bounds. No implementation weakening. Exact original judge system/content reconstruction passes for both stored judge intents. Fresh native preflight verifies both registered DRAFT prompt/policy pins and all six original rows unchanged. Existing narrow prediction/product-reader grants verified without mutations; prediction binding has no time condition.

Current v4 evaluator sha256 f8325305e75637668f509e0979dfb07099aa8a938f01e084b7f8845f7ab1cb54; original 7cff215eaf3d69ff8fe7291c249f9a6d945e236e4679969beb20d58972d0c5ec retained. Intent prompt 5933813566455939072/v1 snapshot 89a252d4d8e4c9a14080b5578a9eccf3019501b0f971c0f9db049047ccf82f58 and clarification draft-v1 policy hash 97497414d6aed432b013ed5565ef717b3705198334d2c2e68c3c5c92012620df unchanged. Judge system v4 hash 91cd26dd457151f49039ee6a37aac42a8e67ef7e05c16a82d1730fc0a4798782. No serving prompt/policy activation or approval.

Build b0a142c0-8616-43b6-9f76-123ef17397b7 succeeded from exact 403-file allowlist, source hash 54ed163f0abf4c7eb3d00efaabb599b38c06937b001dea69ff606ca6981afe85. Image sha256:c41b8dcad6f33732e62b62aef1342252b2174cfdedc769071e33e982ba0ccbfa deployed to ci-prompt-registrar-00016-xkp at 100%. Only image and current measurement evaluator pin changed; all 12 deployment/IAP/IAM/resource/environment preservation checks passed. Registration/bootstrap/approval/release remain disabled. No new model call; corrected live result is NOT RUN and schema completeness is not measured success. Full qualifying benchmark remains 0/159.

Next: signed-in reload -> Operator status -> Corrected v4 attempt -> reconnect -> verify measurement authority -> Run once. Then independently verify exact durable stage, all three judge batches, combined assessment, current source applicability/model/usage and preserved v1 rows. Reconnect/recover v2 without redispatch before expanding. Any failure stays a failed attempt; no automatic remint/retry or invented success. Original failed attempt remains separately selectable for recovery only. Further source-applicability and qualifying full-pipeline measurement plus exact-version authorized approval remain required. Evidence: interpretation_v4_checkpoint_20261018.json, interpretation_v4_pins_verified_20261018.json, interpretation_v4_build_source_20261018.json, interpretation_v4_access_rechecked_20261018.json and interpretation_v4_deployed_checks_20261018.json under data/generated/campaign_e2_diagnostics/.

## Corrected v4 real interpretation pilot verified — 2026-10-18

Critical / Must have within fixed Package 1 / P6 E2-E4. User supplied signed-in v2 result: stage aeae7b910d0cc250a649186e44949f09cf35d95a4023b20ad2199acbaf943d57, assessment 087f89a294342cf1ffcbf529ccf2aacee0cce3616cab10098a516a388bca06ea, observation 0c36d888d89b12bc66fbd9d17dd1a0e27055c6639ce040c027ac0b882c455368. Independent read-only BigQuery verification confirms all ten intent/outcome original write jobs match exact canonical bytes, SQL/config, terminal success and registrar creator; intent/response/outcome/assessment hashes and all judge receipts match. All three judge responses validate exact complete coverage 16/16/9, and combined assessment covers all 47 rule IDs. Four actual provider responses (one interpretation, three judges), returned gemini-3.5-flash with stored nullable usage. Referenced policy/product native source-job metadata is terminal without error; this is not complete source-applicability or original-byte review. All six rows of failed v1 are unchanged. No extra model call, mutation, approval or deployment during verification.

Execution completed; this is not a passed/qualified campaign result. Semantic batch decisions are PASS/PASS/BLOCK. U02 FAIL reason asserts missing concept/copy/CTA/visual direction in interpretation metadata. Reviewed U02 trigger is whenever producing a campaign concept deliverable, with revise-incomplete-draft handling. This raises a measured stage/trigger and decision-semantics applicability concern; do not overwrite the response, infer a PASS or tune solely to remove the hold. Review scoped applicability/calibration against the actual interpretation stage before qualifying. Other PASS observations are equally non-authoritative and do not clear campaign release or missing-source checks.

Signed-in current-v2 reconnect/recovery remains pending: retain Corrected v4 attempt, reconnect, verify measurement authority, then Recover original evaluation records. Expect identical assessment_record_id and observation_sha256 without Run or new dispatch; independently compare original snapshots/jobs afterward. Then resolve applicability and qualifying-evaluation requirements within the fixed MVP before expansion. Full qualifying pipeline benchmark remains 0/159; release_allowed=false, qualifying=false. Evidence: data/generated/campaign_e2_diagnostics/interpretation_v4_completed_records_20261018.json and interpretation_v4_completed_verification_20261018.json.

## Corrected v4 signed-in recovery verified — 2026-10-18

Critical / Must have, fixed Package 1 / P6 E2-E4. User supplied recovered v2 receipt matching original assessment ID 087f89a294342cf1ffcbf529ccf2aacee0cce3616cab10098a516a388bca06ea, observation hash 0c36d888d89b12bc66fbd9d17dd1a0e27055c6639ce040c027ac0b882c455368, stage response, three judge receipts and nullable usage. Independent current BigQuery comparison verifies all eight intent rows and eight outcome rows (v1 plus v2) unchanged byte-for-byte including timestamps. No evaluator write jobs after the original assessment; recover-only code cannot dispatch. This establishes scoped signed-in reconnect/recovery without new provider dispatch reservations/outcomes, not provider billing reconciliation. No model call, mutation, deployment or approval performed during verification.

The requested native-table/narrow-access/protected-evaluation/one-real-case/durable-record/recovery engineering gate is complete within this interpretation-pilot scope. Quality/qualification is not complete: original measured v2 decisions remain PASS/PASS/BLOCK; U02 stage/trigger and requested-content decision applicability must be resolved without modifying stored responses or forcing PASS. The real interpretation is not full multi-stage pipeline execution; qualifying full-pipeline benchmark remains 0/159. Release false. Next within existing E2/E4: inspect stage-specific rubric/trigger handling and genuine context/source applicability, implement bounded versioned fixes only where supported by reviewed criteria, then qualifying measured pipeline execution/scoring and exact prompt/policy approval before release. Preserve both original runs/jobs; no UI rerun needed. Evidence: data/generated/campaign_e2_diagnostics/interpretation_v4_recovery_verified_20261018.json.

## U02 and interpretation-source applicability correction — 2026-10-18

Critical / Must have within fixed Package 1 / P6 E2-E4; no new workstream. Small implementation/test/config batches delivered:

- Typed stage/output applicability identifies evidence-intent metadata only from the bound campaign-evidence-intent-v1 contract and complete valid response shape. U02's reviewed trigger (campaign concept deliverable) is not met by that metadata. Stage name or keywords cannot exempt actual deliverables; planner projections remain applicable, unknown contracts unresolved, malformed/extra fields deny. No stored judgment rewritten.
- Interpretation-source checks consume fresh native ProductFactRead/ClarificationPolicyRead captures, exact SKU/audience/formula scope and actual policy feature enums. Only product identity fields may be used; no product metadata as clinical, commercial or upload approval. APPROVED markers do not establish verified review. Triggered downstream requirements retain required fields and current-native-coverage/legacy-scope holds. Source checks never authorize generation, qualification or release.
- Opt-in campaign-semantic-checkers-v5 uses STAGE_OUTPUT_OBSERVATION instead of GLOBAL_CAMPAIGN_RELEASE for this output, binds stage/source reports to response/scope hashes and rejects contradictory U02 applicability judgments. No forced PASS; complete 16/16/9 coverage, exact quote/evidence validation and context budgets retained. V3/v4 hash/system/payload and recovery remain unchanged.
- Controller/transport v3 retain exact source-applicability bytes in the original stage outcome after fresh post-provider source/access checks. Capture failure preserves actual provider outcome but prevents judging. Cold recovery reuses original report bytes with fresh source guards. Synthetic complete flows/recovery and failure cases verified.
- Read-only native preflight verifies original stage/result job binding, reviewed U02 trigger, current exact registered DRAFT prompt/policy/product pins, permission catalog and structured stores. Initial audit had a local missing policy-pin lookup; FAILED report retained. Corrected audit recovers the pin from exact original rendered input, verifies snapshot/record parity, and passes. For CI-SKU-0022 all five structured kinds have zero records; scientific/offer projection is UNAVAILABLE under the legacy official-SKU adapter. Actual measured features are empty, so those seven downstream rules are not triggered for current intent metadata. These are not approvals or evidence availability for future assertions. ADC audit is inspection, not IAP authority.
- Fixed console lanes now offer Stage/source v5 attempt (run interpretation-pilot-20261018-v3), original failed v3 recovery only and previous completed v4 recovery only. Both prior runs/jobs/versions preserved. Fixed applicability error codes no longer masquerade as ACCESS_DENIED. Container build checks all three evaluator pins and one dispatch-capable lane.

Final 131 focused tests covering impacted behavior/direct dependencies, compilation and dependency checks pass. V5 evaluator b49cebb51b4d4af87f6285a2f5f75f4da19526f1c50d60ace3d16d5dc7740d29; original v3 7cff215eaf3d69ff8fe7291c249f9a6d945e236e4679969beb20d58972d0c5ec and v4 f8325305e75637668f509e0979dfb07099aa8a938f01e084b7f8845f7ab1cb54 preserved. Registered intent 5933813566455939072/v1 snapshot 89a252d4d8e4c9a14080b5578a9eccf3019501b0f971c0f9db049047ccf82f58, clarification draft-v1 hash 97497414d6aed432b013ed5565ef717b3705198334d2c2e68c3c5c92012620df and model/settings unchanged. Judge contract/template is separately versioned; no serving prompt/policy approval or activation.

Build e651ecfb-4638-4fc4-aafe-5dda6df507ad passed from exact 406-file allowlist; source hash accfb1d5c5b1cf813181ef6a9c963cebaccf83ed700b3971f71667361fa27868. Image sha256:264ceed9691d4391ec74e53a94e1d6781b4d9832b7f87ad6658265f3d53534b6 deployed as ci-prompt-registrar-00017-94n at 100%. All 12 preservation/IAP checks passed; only image/current evaluator pin changed. No IAM, registered-prompt/policy or approval changes. All eight original intents/eight outcomes unchanged including timestamps. No model calls or qualifying execution in this task. Full qualifying benchmark remains 0/159; release and qualifying-pipeline enablement false.

Next: reload signed-in console -> Operator status -> Stage/source v5 attempt -> reconnect -> verify measurement authority -> configured nonqualifying run once. Independently verify actual durable U02/source applicability, all stage/judge/assessment jobs/hashes, then signed-in reconnect/recovery without redispatch before qualifying runs. Failure remains measured failure, never an automatic remint or forced PASS. Applicable downstream permissions/source bytes/retrieval/rendered artifacts remain gates before full-pipeline qualification and exact-version approval. Evidence: stage_source_v5_checkpoint_20261018.json, stage_source_applicability_native_corrected_20261018.json, stage_source_v5_build_source_20261018.json, stage_source_v5_deployed_checks_20261018.json and stage_source_v5_originals_preserved_20261018.json under data/generated/campaign_e2_diagnostics/.

## V5 measured stage/source correction verified — 2026-10-18

Critical / Must have, fixed Package 1 / P6 E2-E4. User supplied the real v5 run v3 receipt: stage 6e5858e868cf380e393ae42e465ea70d6e37bf527a4f994dadf5844ce9de7034, assessment 1d39ba1f506de6b59963ede97b2d459afc6174e2dadcb8fa3721a045d91d836a and observation 08469dfc3756513fb3773360dd986f1ed6e10c48f8164d916efad5c7e2f7f649. Independent read-only verification confirms all ten original intent/outcome write jobs match exact canonical SQL/config/bytes, terminal success and registrar creator. Stage response/provider hash/model/nullable usage and all judge receipts/outcome/assessment hashes match. All three batches validate 16/16/9 checks and combined assessment covers all 47 rule IDs. Four real provider responses (one stage, three judges); no additional model call, write, deployment or approval by this verification.

U02 is actually observed NOT_APPLICABLE, with an empty quote/evidence list and a reason explicitly tied to campaign-evidence-intent-v1 metadata rather than a campaign concept deliverable. Semantic batch decisions PASS/PASS/PASS. This resolves the measured U02 misclassification for this bounded typed interpretation case, not a blanket stage exemption. Current-stage source-applicability report is stored identically in stage outcome and assessment; its hash/response/scope/parent product fields/policy record and post-response native job references match. Source captures are within original validity windows at stage completion; all four referenced before/after product/policy job metadata are terminal without error with registrar creator. Features remain empty; downstream requirements are not triggered for this output. Independent downstream authority remains false, policy review required, qualification_ready=false, generation_authorized=false and release=false. Missing downstream sources/legacy scope holds from native preflight are not waived. Prior sixteen original rows remain byte-for-byte unchanged.

V5 signed-in reconnect/recovery pending: keep Stage/source v5 attempt selected, reconnect, verify measurement authority, then Recover original records (do not Run). Expect identical assessment ID/observation hash/receipts and frozen source report; independently compare all 26 rows/native jobs afterward for no redispatch. Then proceed to applicable downstream source/retrieval/artifact evidence and actual qualifying full-pipeline execution/scoring. Full qualifying benchmark remains 0/159. Evidence: data/generated/campaign_e2_diagnostics/stage_source_v5_completed_records_20261018.json and stage_source_v5_completed_verification_20261018.json.

## V5 recovery denial and bounded action-session correction — 2026-10-18

Critical / Must have within fixed Package 1 / P6 E2-E4. User reports ACCESS_DENIED specifically from Recover original evaluation records. Recent registrar recovery/source queries are terminal without error; no native query failure identifies the exact hidden ContractError. Offline replay of exact saved v5 rows reconstructs the identical assessment/receipt, with reservation/publication/provider calls forbidden. Independent current BigQuery comparison confirms all thirteen intents and thirteen outcomes unchanged including timestamps. Exact live denial cause is not proven: job timing is consistent with the connection's five-minute operator manifest expiring during recovery. The expired-manifest behavior is independently reproduced with a fresh correct identity. Do not claim successful signed-in recovery yet.

Two-file worker/test correction: for measurement_current/evaluation_run/evaluation_recover only, owned worker reauthenticates IAP at action entry, validates exact configured audience/publisher and existing deployment assignment, requires disabled host/policy/measurement/approval writes, and issues a fresh action-bounded measurement read/evaluation manifest. Fresh native workspace/source guards and assertion expiry remain enforced during the operation. No renewal of IAM, reviewer authority or write manifests. Existing worker/evaluator/pending recovery objects are retained; no implicit restart, retry or provider dispatch. Fixed recovery integrity codes now distinguish original-record/source conflicts from access denials without exposing values. Wrong principal/audience, expired assertions and write-enabled configurations deny without changing the old manifest. Initial negative tests used invalid identity-constructor fixtures; corrected valid-shaped mismatched/expired fixtures now test the intended boundary.

56 impacted tests plus compilation pass. Container isolated probes validate all original/current evaluator pins. Build ab53535c-2ddb-43fc-9569-644e85e677ab passed from 406 allowlisted files, source hash 704ecdff0ac06b823b7cab170e5c0ae6b181f8bd333382515cb72fcb31f3b074. Image sha256:4f9ffdf9ec3314dcbbb4de0bfb902a23f912e8253a279eabe1ea93050144fb18 deployed as ci-prompt-registrar-00018-6sz serving 100%. All twelve preservation/IAP checks pass; environment, evaluator/prompt/policy pins, IAM and resources unchanged. No model call, business mutation, approval or release change. Full qualifying benchmark 0/159.

Next: fresh signed-in reload, retain Stage/source v5 attempt, reconnect, verify measurement authority, then Recover original evaluation records (not Run). Expect assessment 1d39ba1f506de6b59963ede97b2d459afc6174e2dadcb8fa3721a045d91d836a and observation 08469dfc3756513fb3773360dd986f1ed6e10c48f8164d916efad5c7e2f7f649. Independently verify unchanged original rows/jobs and no redispatch. Further denial remains a live blocker to diagnose from a safe exact code; no fallback identity or automatic model retry. Evidence: evaluation_action_session_checkpoint_20261018.json, stage_source_v5_recovery_offline_replay_20261018.json, evaluation_action_session_originals_preserved_20261018.json, evaluation_action_session_build_source_20261018.json and evaluation_action_session_deployed_checks_20261018.json under data/generated/campaign_e2_diagnostics/.

## V5 signed-in recovery complete — 2026-10-18

Critical / Must have, fixed Package 1 / P6 E2-E4. After action-session correction revision ci-prompt-registrar-00018-6sz, user supplied the recovered v5 receipt matching original assessment 1d39ba1f506de6b59963ede97b2d459afc6174e2dadcb8fa3721a045d91d836a, observation 08469dfc3756513fb3773360dd986f1ed6e10c48f8164d916efad5c7e2f7f649, stage hash, all judge/outcome hashes and nullable usage. Independent current BigQuery comparison verifies all thirteen intents and thirteen outcomes unchanged byte-for-byte including timestamps. No evaluator write jobs after original assessment. Combined with recover-only dispatch prohibitions, this verifies scoped signed-in recovery without new provider dispatch records; not provider billing reconciliation. No model calls, business mutations, deployment or approvals during this verification.

Measured U02/source-applicability correction and durable reconnect recovery are now complete for the bounded typed interpretation pilot. U02 remains NOT_APPLICABLE only to this typed metadata, semantic batches PASS/PASS/PASS; original v1/v2 observations retained unchanged. This is not a complete/qualifying multi-stage pipeline run or global campaign/source approval. Current-stage identity/policy captures are verified; applicable downstream claim/offer/report-byte/retrieval/artifact evidence and source-scope support remain gates. Full qualifying pipeline benchmark remains 0/159, release false. No further UI rerun needed. Next fixed MVP work: prepare and qualify one actual complete pipeline case with reviewed stage/case mapping, applicable native sources and real retrieval/artifact execution (or justified case-specific nonapplicability), measured scoring and durable recovery, before expanding across the reviewed benchmark. Evidence: data/generated/campaign_e2_diagnostics/stage_source_v5_recovery_verified_20261018.json.
