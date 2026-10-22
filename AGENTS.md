# Creative Intelligence Project

## Goal

Build a Google Cloud based multimodal RAG platform for skincare/beauty creative intelligence.

The end goal is:

1. Ground on real product metadata and ingredients
2. Ground on real product images
3. Ground on historical campaign creatives
4. Ground on campaign performance and SKU sales
5. Use RAG + analytics to generate campaign concepts
6. Pass grounded context + real product images to Gemini/image generation
7. Produce a high-quality campaign banner that:
   - accurately represents the product
   - uses approved claims only
   - follows brand guidelines
   - learns from historical campaign performance

## AI collaborator role

Read and apply [docs/AI_SOLUTION_ARCHITECT_ROLE.md](docs/AI_SOLUTION_ARCHITECT_ROLE.md)
alongside these project instructions. It defines the editable working role for
solution architecture, Python engineering, LLMOps, CI/CD, Google Cloud and
efficient token/context usage. It supplements the project rules and does not
grant permissions or imply that planned capabilities are implemented.

## Token-efficient development

Use [docs/PROJECT_MAP.md](docs/PROJECT_MAP.md) to locate modules and
[docs/TOKEN_EFFICIENCY.md](docs/TOKEN_EFFICIENCY.md) for effort defaults, targeted
searches, bounded tests and concise reporting. The reusable skill is
[token-efficient-development](.agents/skills/token-efficient-development/SKILL.md).
Preserve fixed MVP scope. Broaden inspection/testing when correctness or project
gates require it; savings never weaken authority, privacy, approval or release rules.

## Latest checkpoint — 2026-10-16

Package 1 native interpreter verification/question UI — 2026-10-16 (Critical / Must have):
six native cases passed: atomic outcome/request/accounting/fence, original-job recovery,
new-job exact replay, rollback, postcommit error without invented receipt, and genuine
competing outcome race. Native intervals overlap 3.821 seconds, one winner/positive
rolled-back loser; winner invocation/fence and unrelated snapshots are checked. Core
request/invocation/workspace use destination-decoded equality and audit counts; replay/
recovery/rollback preserve exact canonical snapshots. The first probe failed because
it compared differently serialized rows before destination decoding; both initial and
corrected owned eleven-table datasets were deleted and absence verified. FAILED report
retained. No serving writes, model calls, IAM or deployment changes.
Trusted campaign_runtime_bootstrap.py atomically installs an existing typed/prestarted
workflow runtime and registered intent/question worker, requiring matching clients/IAP
and known adapters; no enablement or fake approved profiles. Installation tests use
synthetic dependencies; no actual live runtime is installed by this slice.
QuestionTurn binds exact response/candidate/report/context/scope/expiry. Protected
service stages bounded private turns, rechecks access/readiness on reads; Streamlit
now calls the renderer. Text requires quality permission/PASS and recomputed fresh
release checks and uses plain text. Existing real quality holds still suppress it.
56 focused tests, compilation/dependencies pass. Positive layout test explicitly mocks
authority and is not approval. Step 1 is complete within native fixture scope; step 2
live dependency assembly/serving installation remains pending, step 3 code is connected
but live release remains gated. Next fixed package 1 work: assemble verified runtime
hosts/client/profile configuration and install in serving process, verify IAP/worker/
reconnect; complete prompt/policy/quality evaluation before any release. Full benchmark
0/159 and 41 unavailable checks unchanged. Evidence:
data/generated/campaign_e2_diagnostics/runtime_question_connection_20261016.json.

Package 1 interpreter outcome/worker coordination — 2026-10-16 (Critical / Must have):
interpreter_outcome.py adds a separate disabled-by-default server-issued result plan
and protected publisher using ControlIO original-job recovery. Transaction pins full
workspace/member/conversation/request/invocation state, uses the common workspace
fence, writes interpreter invocation/request plus result/accounting audits atomically,
and validates returned exact rows. One native slot per dispatch excludes observation
content; conflicting observations get no alternate slot. UNKNOWN preserves ACCEPTED
and unknown accounting; successful evidence-only responses close NEEDS_CLARIFICATION
without invented preferences. Planner jobs/attempts are untouched. Exact replay does
not increment the fence. Read-only recovery uses only the original retained plan.
interpreter_worker.py coordinates one registered admission dispatch and publication,
retains the exact result plan before writes for lost-response recovery, respects
owned-worker/deadline gates and exposes once-only trusted process installation.
104 focused tests, compilation/dependencies and BigQuery script dry run pass. Dry run
is NOT native transaction/race verification. No live worker was installed, no question
renderer connected, no model/provider or business writes, deployment or activation.
Recovery plans are process-local; cold-process reconstruction is not implemented.
Next fixed Package 1 work: isolated interpreter transaction/recovery verification,
then actual trusted host/runtime installation and checked question rendering, preserving
current policy/evaluation gates. Full evaluation/approval remains incomplete; no new
workstream. Evidence:
data/generated/campaign_e2_diagnostics/interpreter_worker_verification_20261016.json.

Package 1 admitted intent/question adapters — 2026-10-16 (Critical / Must have):
EvidenceIntentAdapter is a one-shot registered admission callback that reads current
persisted transcript/request/policy through ProtectedEvidenceChatService, requires
owned-worker DISPATCHED invocation and matching prompt/profile/principal/scope, renders
the exact registered intent definition and rechecks authority/policy after provider
response. DRAFT/unverified policy denies before transport. EvidenceQuestionDispatch
wraps the existing question adapter with exact raw-response hash, returned model ID
and nullable provider token counts. Both return server-held drafts with release false;
unknown usage/model/cost stays unknown. Factories connect both to the protected service.
prepare_evidence_from_result requires the exact stored prompt binding and SUCCEEDED
provider hash/model/safety/token fields before authoritative parent gap preparation,
then rereads context/access. Missing/uncertain durable outcomes never authorize gaps.
61 focused offline tests, compilation and dependency checks pass; an initial test-only
missing import was corrected. No cloud/model calls, writes, deployment or activation.
This is callback/outcome-binding integration, not a running worker, actual provider
execution or durable outcome publication. Existing result SQL is planner-specific;
do not misapply it to interpreter outcomes. Next within fixed Package 1: connect
interpreter-specific durable outcome publication/recovery and trusted worker composition,
then checked question rendering. Policy review authority/applicable quality/evaluation
remain required before activation; full benchmark 0/159 is unchanged. Evidence:
data/generated/campaign_e2_diagnostics/evidence_dispatch_connection_20261016.json.

Package 1 protected chatbot evidence connection — 2026-10-16 (Critical / Must have):
new ProtectedEvidenceChatService connects fresh IAP/access and stored policy/registered
intent/question definitions to the existing authoritative gap reader. WorkflowUiRuntime
accepts the typed service and forwards only IAP plus configured workspace/conversation;
server-observed response rebinds through read_bound_gaps. Durable chat now checks
readiness before catalog/local interpretation, preventing local-model fallback when
the service is absent, unavailable or ineligible. Policy/registry/access changes and
expiry deny; readiness explicitly never grants release. 45 focused tests, compilation
and dependencies pass. Partial E2 runner still has six component matches, 3/6 global
label matches, 41 unavailable checks, full benchmark 0/159 and no measured model quality.
This is offline code integration, not a deployed/installed chat service or completed
activation evaluation. No cloud/model calls, writes, deployment or activation. Next
within the same package: trusted runtime/worker installation and persisted interpretation/
intent/question dispatch/outcome integration; complete applicable policy/quality and
evaluation eligibility before release. Preference interpretation and evidence-feature
intent have different schemas; do not substitute them. Original workflow catalog/channel
constraints remain explicit. Evidence:
data/generated/campaign_e2_diagnostics/chat_evidence_connection_20261016.json.

Package 1 clarification DRAFT imports verified — 2026-10-16 (Critical / Must have):
user completed both signed console submissions. Independent full BigQuery current/
history decoding verifies campaign-evidence-intent 5933813566455939072/v1 and
campaign-evidence-questions 7384254121445949440/v1, both DRAFT revision 1 with exactly
one history row. Fresh exact Vertex/policy/settings parity previews show EXISTING.
Both original native registration jobs are DONE without error, have the expected
runtime creator and match exact production SQL/config derived from stored rows.
No verification mutation/replay, model call or activation occurred. Initial read-only
job verifier had a local dict-vs-SDK argument error; FAILED report retained, corrected
verification passed. Configuration-only revision ci-prompt-registrar-00010-jsp
serves 100% using the same image with IAP/bootstrap protections and writes disabled.
The temporary registration window is CLOSED. Evidence:
data/generated/campaign_clarification_policy_diagnostics/both_registrations_verified_corrected_20261016.json.
This completes Package 1's console deployment/IAP review/DRAFT registration slice,
not Package 1 or the MVP. Next fixed work: connect existing protected intent/question
and campaign services to the chatbot behind disabled release defaults, with prompt/
policy eligibility and scoped evaluation before activation. Do not expand admin
workflows or bypass current quality/approval gates.

Package 1 controlled DRAFT registration window — 2026-10-16: user supplied
both IAP review outputs; exact intent/question IDs, v1 snapshots and DRAFT proposals
match the verified targets, with mutation_performed=false and UNREGISTERED.
These are user-reported previews, not native registration receipts. Configuration-only
revision ci-prompt-registrar-00009-ckt now serves 100% with registration writes true;
comparison verifies this is the only environment change, with identical immutable
image, IAP, runtime identity/capacity and bootstrap false. This supersedes prior
writes-disabled deployment state while the coordinated import window is open.
Fresh browser reload/reviews and per-prompt hash confirmations are required; no
registration or activation is implied. Next: obtain both DRAFT receipts, independently
verify authoritative rows/original jobs, then close registration writes.

Package 1 existing-console deployment — 2026-10-16 (Critical / Must have):
Cloud Build ba75353f-dc8d-4d64-8efa-71ef6208a3c4 succeeded from 297 allowlisted
files. Immutable image sha256:6c13d1d0fee5bc806315bb3acfbb6fb31b2a33644d10ac9f0066fc831084c504
is deployed to the existing ci-prompt-registrar service, revision 00008-scc,
serving 100%. Exact configuration comparison verifies both clarification target
pins, unchanged prior environment/runtime/capacity, IAP enabled, only its service
agent invoker, registration writes false and bootstrap false. The first update
command was rejected locally for flags-file syntax; the corrected command deployed.
37 focused tests and compilation pass. No registrations/model calls or activation
occurred. Computer control was denied; no browser interaction occurred, so actual
signed IAP previews/runtime queries remain pending user verification. Next within
fixed package 1: verify both exact reviews in the existing signed-in console,
coordinate fresh ADMIN/hash-confirmed DRAFT imports, verify native receipts and
close writes. Do not replace IAP identity with ADC or ask for tokens in chat.
Evidence: data/generated/campaign_clarification_policy_diagnostics/console_deployment_verification_20261016.json.

Plan frozen — 2026-10-16: user requires the existing expanded six-package MVP
plan to remain fixed, with no intervening new workstreams. Only planned MVP work,
implementation bugs and existing bugs demonstrably blocking MVP may be worked.
Unrelated work stays Phase 2; record blockers under existing exit criteria.

Package 1 runtime-read prerequisite — 2026-10-16 (Critical / Must have):
setup_clarification_runtime_access.py implements a fixed two-table, two-permission
custom role/grant setup with default read-only mode, role parity, etag-preserving
binding updates, unrelated-binding checks and no automatic mutation retry. Native
role creation succeeded; the first table attempt was rejected while the new role
was not resolvable by BigQuery. A later grant committed but unconditional IAM
version normalization caused a local readback conflict. Authoritative reads
confirmed that grant existed, and the corrected run reused it and applied only
the missing revocations-table grant. Both failed reports remain retained. Six
focused tests and compilation pass. Current service image/revision, IAP, workspace,
capacity, disabled registration writes and disabled bootstrap were verified
unchanged around the completed setup. Effective runtime queries remain unproven;
role/table grants are not an IAP or successful query test. Next fixed package 1
step: immutable build and disabled-write console deployment, then real protected
runtime previews before coordinated DRAFT registration/activation gates.

Expanded MVP instruction — 2026-10-16: user includes hybrid search, generative
image rendering and wider formats/markets in MVP, superseding prior Phase 2 scope
for these capabilities. Existing six-package queue and priority IDs remain; R3/P14
hybrid retrieval/reranking and P15 source-grounded image rendering are High / Must
have with Critical fidelity/authority gates. Proposed landscape/square/vertical
formats and US/CA/GB/AU market matrix require specification/evidence/review before
enablement; they are not approvals. Existing ban on AI people/before-after remains.
Advanced admin UI/model selectors/real-outcome learning remain Phase 2. Planning
only; no implementation, tests, cloud calls or activation. See expanded MVP
acceptance and updated delivery contract in docs/IMPLEMENTATION_PLAN.md.

MVP-first delivery contract — 2026-10-16: user directs a workable integrated MVP
and moves remaining enhancements to Phase 2. docs/IMPLEMENTATION_PLAN.md now opens
with the controlling fixed six-package queue, existing priority mappings, product
boundary and measurable done criteria. Reuse completed components; prefer end-to-end
integration over further isolated admin foundations. Reranking and UI effectiveness
stay High / Must have. Grounding, brand/safety validation, exact evaluated prompt
activation, bounded correction, feedback, context/usage limits and RAG telemetry
remain MVP. Advanced admin UI, company onboarding, model selector, hybrid expansion,
image-model rendering and broad scaling/analytics move to Phase 2. Existing release,
review/access and 30-day privacy requirements remain; no checks or approvals are
bypassed. Keep 2–3-file batches and focused tests. Immediate package 1: narrow runtime
reader prerequisite, protected prompt operations and installed campaign-chat path.
Planning only; no cloud changes, deployment, activation or new implementation.

MVP scope update — 2026-10-16: user explicitly includes reranking and UI
effectiveness in MVP. Existing R3/priority 14 bounded second-stage reranking is
now High / Must have and pulled forward alongside R1/R2 and P6 evaluation; hybrid
expansion remains optional. U4/U5/U6 and E-stage quality include measured marketing
task usability, actionable clarification/recovery, evidence/output/feedback and
accessible responsive interaction. Numeric acceptance thresholds require baseline
evidence and agreement. Existing IDs and 2–3-file batches are preserved. This is
planning only, with no implementation, cloud changes, tests or activation. See
docs/IMPLEMENTATION_PLAN.md, MVP reranking and UI effectiveness.

P6 E2 clarification deployment preflight — 2026-10-16 (Critical / Must have):
a repeatable read-only package/service/IAM inspector verifies a 296-file allowlisted
context and preserves per-file/source hashes. Current revision 00007-gcc retains
IAP, exact runtime/audience/origin/workspace, concurrency 16, max one instance,
registration writes false and bootstrap false. Two clarification-policy tables
have no direct runtime reader binding, and ci_brand_memory has no direct runtime
dataset grant. Runtime impersonation is denied; effective read access remains
UNVERIFIED, including any inherited/group/deny policy effects. A concrete two-table,
two-permission reader proposal is recorded, not applied; no IAM broadening occurs.
The first inventory command failed on unsupported --ignore-file; corrected isolated
inventory passed and the FAILED report is retained. Packaging inspection is not a
container build or deployment. Next: exact narrow table-reader setup, immutable
build/disabled-write deployment, then runtime/IAP exact previews and coordinated
ADMIN DRAFT registration. Serving/policy approval remain disabled and E2 open.
See deploy/prompt-registration/README.md and
 data/generated/campaign_clarification_policy_diagnostics/deployment_preflight_corrected_20261016.json.

P6 E2 protected clarification registration console — 2026-10-16
(Critical / Must have): opt-in selection now supports exact intent/question versions
alongside the preserved planner route. Server-only configuration pins policy,
settings source and both targets; each prompt retains an isolated bounded issued
review store across reruns while clients are freshly composed/closed per action.
Fresh IAP/ADMIN, original command/hash, expiry, scope, current policy and full
settings parity remain enforced. Configuration changes deny retained reviews;
uncertain submissions retain the original command. 80 focused offline tests,
compilation and dependencies pass, including Streamlit rerun and enabled-submit
fixtures. No live cloud calls, registration writes, deployment, policy approval,
model calls or activation occurred. The code is not yet deployed; both new remote
versions remain UNREGISTERED at the last native observation. Next: package/deploy
with writes disabled and verify protected identity/target parity, then coordinate
fresh ADMIN exact-hash DRAFT imports and authoritative receipts. Policy review
and question-quality authority, unverified answers/recheck and chatbot installation
remain open under existing E2/9/12. No new milestone is added. See
 docs/CONVERSATIONAL_EVIDENCE_GAPS.md and
 data/generated/campaign_clarification_policy_diagnostics/console_verification_20261016.json.

P6 E2 clarification Vertex authoring — 2026-10-16 (High / Must have;
Critical / Must have registration boundaries): two 2-file implementation groups
add deterministic initial-version authoring/read-only recovery and a policy-bound
IAP/ADMIN exact-hash registration operator. Native creation and exact readback
passed for intent 5933813566455939072/v1 and questions 7384254121445949440/v1;
both native registry previews remain UNREGISTERED. Intent read-only recovery reused
the same version. 114 focused tests, compilation and dependencies pass. Initial
question metadata hash depth failure occurred before RPC, is retained FAILED and
has a regression-tested bounded wire-JSON fix. BigQuery prompt registrations and
model inference remain zero. Fresh authenticated ADMIN review/import is pending;
the new operator is not installed in the planner-only console. No policy approval,
activation, product/business changes, IAM or deployment changes. Single-operator
recovery does not establish concurrent creation or durable cross-process uncertainty
handling. E2 stays open; policy authority, answer capture/recheck and quality-gated
chatbot installation remain pending. See docs/CONVERSATIONAL_EVIDENCE_GAPS.md and
data/generated/campaign_clarification_policy_diagnostics/prompt_authoring_verification_20261016.json.

P6 E2 clarification prompt definitions and structured adapters — 2026-10-16
(High / Must have; Critical / Must have trust boundaries): v1 intent/question
schemas, exact-target snapshot/parity/RuntimeBehavior helpers and hash-matched
persisted-intent rendering are implemented. The intent projector requires the
matching stored intent-prompt binding. A real BigQuery-policy read produced both
review definitions, status DRAFT_DEFINITIONS_NOT_REGISTERED; no Vertex creation,
BigQuery prompt registration, promotion or model calls occurred.

New structured record/revocation stores cover labels, prices, availability,
shipping and same-offer exclusions in opt-in gap planning. Native setup created
both tables empty; five exact empty queries for CI-SKU-0021 passed; reapply created
nothing and both counts stay zero. Scope/hash/history-size/native/access/expiry/
revocation guards distinguish true empty results from failure. Approved markers
alone do not authorize fields: independent current hash-matched PRODUCT_FACT
reviews are required. Record/review expiry bounds shorten captures. Exclusions
must bind the same current offer and exact qualifications. Source discovery covers
23 IDs, while legacy permission/independent-review scopes remain unchanged; N&T
review incompatibility holds use, never grants approval. No product/business rows,
approvals, IAM/deployment or activation changed. 143 focused tests plus compilation/
dependencies pass. Native positive-review/race/revocation and deployed IAP/model
execution are not proven. Registry review/import/promotion, policy review authority,
N&T review compatibility, full study/source-byte validation, answer capture/recheck,
question-quality gates and chatbot installation remain open under existing E2/9/12.
No quality checker was closed. See docs/CONVERSATIONAL_EVIDENCE_GAPS.md and
data/generated/campaign_structured_evidence_diagnostics/verification_20261016.json.

P6 E2 exact clarification-policy source — 2026-10-16 (Critical / Must have):
fixed-schema immutable policy versions and a separate revocation namespace now
support exact ID/version/hash native-correlated reads. Conversation preparation
rejects supplied JSON and binds initial/final stored policy around evidence reads,
with owner-authorized request authority before/after. Missing/duplicate/corrupt/
revoked/future/expired captures deny. The reader never grants approval_verified;
a stored APPROVED marker cannot authorize question generation. DRAFT-only operator
ingestion uses one stable global native job per ID/version, with no uncertain slot
remint. Native creation added two tables and one DRAFT row in ci_brand_memory;
reapply created nothing and recovered the same COMMITTED publication job, with
identical independent policy/record readback (versions=1, revocations=0).
112 focused tests pass (nine new), compilation/dependencies pass. No model/Vertex,
IAM/deployment, business approval or activation. Native proof covers DRAFT write/
read/recovery, not concurrent revocation/races or deployed IAP. Protected review/
revocation publication, new intent/question prompt versions, missing structured
source adapters, unverified answer capture/recheck and chatbot wiring remain open
under existing E2/9/12. No quality checker was closed. Evidence:
data/generated/campaign_clarification_policy_diagnostics/verification_20261016.json.
See docs/CONVERSATIONAL_EVIDENCE_GAPS.md.

P6 E2 clarification source/intent/question adapters — 2026-10-16 (High / Must
have; Critical / Must have authority boundaries): two-file groups add a bounded
native-correlated reader of all six existing permission tables, exact successful
durable interpreter-response projection and a server-only question-draft callback
for registered one-shot admission. Source access is checked before/after; full
counts/history/hash/orphan/size checks deny corruption and outages never imply
absence. Fresh authority/coverage/authority snapshots and refreshes around model
transport reject context changes. Question prompt/schema/profile/variables, owned
worker and single-use checks gate the adapter. Its result is always unreleased.
103 focused offline tests pass (16 new), compilation/dependencies pass. No cloud/
model calls, writes, approvals or activation. Synthetic resolved-prompt callback
fixtures are not a real registered question runtime or deployed IAP proof.

Only existing claim/offer schema scope is supported; CI-SKU-0021–0023 and missing
label/base-price/stock/shipping stores return UNAVAILABLE. Full study-report bytes
and structured exclusions are not inferred from metadata/qualifications. Current
legacy interpreter schemas deny the new intent format. Authoritative BigQuery
clarification-policy publication/reader, new intent/question prompt registration
and installation, missing source adapters, question-quality gates, durable answer
capture/review/recheck and chatbot integration remain pending in existing E2/9/12.
No unavailable quality checker was closed; serving stays disabled. See
docs/CONVERSATIONAL_EVIDENCE_GAPS.md for source/test limits and remaining gates.

P6 E2 conversational evidence-gap foundation — 2026-10-16 (High / Must have;
Critical / Must have trust boundaries): a three-file group adds versioned DRAFT
requirements, pure scoped/fresh coverage planning and a pinned LLM question
request/response contract. Applicability uses supplied interpreted feature flags,
not keywords. Only relevant missing fields are selected, with three questions per
turn; complete approved fields suppress repeats. Missing records, query failures
and pending/rejected/stale/conflicting records have separate dispositions.
Request/intent/scope drift or stale/missing coverage denies planning. Submissions
remain UNVERIFIED and generation_authorized always false. Ten new tests and 29
affected offline tests, compilation/dependencies pass. No cloud/model calls, writes,
approvals, prompt registration or UI changes. Captures/native IDs are supplied
contracts, not authoritative BigQuery proof. The local configuration is a draft,
never a runtime source-of-truth fallback. Authoritative coverage/intent adapters,
protected question generation/quality checks, answer storage/review/recheck and
chatbot installation remain pending within existing E2/9/12 gates. No unavailable
quality checker was closed. See docs/CONVERSATIONAL_EVIDENCE_GAPS.md.

P6 E2 durable input/structured checking/transcript implementation — 2026-10-16
(Critical / Must have — shared pilot): opt-in generation admission now appends an
immutable input manifest in the existing dispatch transaction, validates exact
plan/envelope reconstruction and rereads stored pins around its one-shot provider
callback. The authoritative checker accepts versioned projected copy/sections only
with exact stored manifest, selection/catalog/transcript, provider outcome and
initial/final access/policy/prompt/claim-evidence checks. No unavailable check grants
release. User selected 30-day raw transcript retention: the separate partition-
expiring body table and rolling-cutoff reader verify complete ordered hash-matched
bodies. Opt-in authority publication writes bodies atomically; exact replay and
original global slots are preserved. Stored-transcript input composition never
fills gaps from local history. Both new BigQuery tables verified empty; reapply
created nothing. No business rows, models, IAM/deployment or activation changed.

All 220 affected offline tests, compilation/dependencies pass. Six isolated native
transaction-extension cases passed with fixture deletion/absence verified; all 13
native jobs independently verified terminal. Native proof covers extension SQL and
synthetic fixtures, not the complete protected admission/IAP service; concurrent
submissions have one winner but interval overlap was not measured in this probe.
Original server-held prepared envelopes remain required. Hash-only publications
are not silently backfilled; local/unpersisted raw transcript coverage and chatbot
installation remain incomplete. Planner/policies DRAFT, release false, 41 unavailable
checks and 0/159 full benchmark runs remain. Next within existing E2: combined
protected-path verification, remaining checks/measurements and guarded interaction
installation before shared activation; E4/E5 and Priority 9/12 gates stay open.
See docs/CAMPAIGN_E2_IMPLEMENTATION.md and
 data/generated/campaign_e2_diagnostics/durable_planner_inputs_verification_20261016.json.
This supersedes the implementation-pending portions of the following checkpoint
only within the explicit opt-in/offline/native-extension scope.

P6 E2 native authority and planner adapter checkpoint — 2026-10-16 (Critical /
Must have — shared pilot): all 12 isolated native transaction/recovery/denial/race
cases passed. Real transactions overlapped 18.799 seconds with one complete winner
and positive workspace-fence loser rollback; full 14-table fixture snapshots verify
unrelated preservation. Both owned datasets were deleted and absence verified;
serving metadata counts match (not a full checksum). Initial unsupported array-join/
empty-array correlation failure remains FAILED with cleanup. Production SQL now
uses grouped exact source comparisons, scalar canonical JSON snapshot parameters
and explicit required partition filters. No serving rows, models, IAM or deployment
were changed. Synthetic identities/access and product projection fixtures do not
prove deployed IAP or production source-table operation.

Versioned planner copy projection and server-owned input preparation/outcome bridge
are implemented and offline verified. Exact raw response/options, ordered supplied
transcript text hashes, rendered prompt, evidence, image bytes and invocation pins
bind diagnostic results. All 191 affected offline tests, compilation and dependencies
pass. Durable input-manifest admission/rereads and structured authoritative-quality
runner installation remain pending; the existing runner still accepts exact text.
Raw/unpersisted chat transcript coverage and chatbot installation remain incomplete.
Original catalog defaults remain unchanged. Policies/planner DRAFT, release false,
41 unavailable checks and 0/159 full benchmark executions remain explicit. Next:
durable dispatch-input binding, structured runner composition with before/after
checks, then raw transcript ingestion and per-interaction integration; remaining
E2/E4/E5 checks/measurement stay open. See docs/CAMPAIGN_E2_IMPLEMENTATION.md and
data/generated/campaign_e2_diagnostics/native_planner_adapters_verification_20261016.json.
This supersedes the native-pending/structured-unsupported checkpoint below only
within the diagnostic/native fixture scope described here.

P6 E2 catalog/selection/transcript/provider authority — 2026-10-16 (Critical /
Must have — execution binding/shared pilot): successive 2–3-file groups add a
separate opt-in run_authoritative_quality path and immutable request-authority
schema. The new reader opts into a catalog from current ACTIVE BigQuery product
rows with exact BEAUTYFEEDS/N&T provenance; original 20-SKU validator defaults and
legacy serving/admission remain unchanged. Exact stored country/platform, accepted
selection and ordered stored message/response hashes are bound, not inferred.
No raw chat text is stored; unpersisted local turns and unresolved free-text choices
remain unsupported. A structured first request can have an explicit empty manifest.
Current independent claim/evidence permissions enforce exact wording/qualifications,
formula/market/channel/audience and approval windows. Durable invocation/parents/
profile, exact prompt binding, returned model/outcome and response-byte hashes are
required; initial/final authority and permission changes withhold results. Only
exact provider text is supported; structured planner-output projection still denies.
Review metadata is not proof of GCS/report bytes or every dispatched input byte.

The server-only authority publisher is disabled by default. It requires CREATOR
workspace and owner READ_DRAFT access, fresh native source capture, one global
native slot per request, in-transaction access/source checks and workspace fencing.
Rows append immutably before dispatch; no reset/remint/automatic retry. Recovery
uses the exact original server-held plan; no durable intent store or cold plan
reconstruction. The row retains principal/time; no separate audit_events action
or actual deployed IAP verification is claimed. All 439 affected offline tests
pass (33 new); compilation/dependencies pass. An initial nonexistent test-module
selection was corrected to the existing ControlIO test class. Native schema setup
created only campaign_request_authorities in asia-south1; reapply created nothing,
both reporting metadata row count zero. No selection/claim/approval rows, model
calls, IAM/deployment changes or activation. New SQL positive/race/recovery probes
remain pending, separately from schema verification and existing ControlIO probes.
No live authority capture/end-to-end campaign is claimed. Planner/policy stay DRAFT;
E2 remains open with 41 unavailable checks and 0/159 full benchmark executions.
Next within existing E2: isolated native authority/read/publisher/race/recovery
checks, stage-specific structured-response/dispatch-input adapters, then existing
remaining quality/benchmark/release work. Keep 2–3-file groups and focused tests.
See docs/CAMPAIGN_E2_IMPLEMENTATION.md, docs/IMPLEMENTATION_PLAN.md and
data/generated/campaign_e2_diagnostics/authority_bindings_verification_20261016.json.

P6 E2 mandatory request execution binding — 2026-10-16 (Critical / Must have —
execution binding/shared pilot): run_current_quality now requires an authoritative
request/history capture before source checking and a distinct fresh native capture
after the checker and final policy/prompt reread. Supplied input/history hashes,
scope/principal, operation/stage/status, selected SKU and coarse channel compatibility
are checked. Complete binding changes, stale or reused captures, missing source or
revocation withhold diagnostics; final freshness preserves the original expiry.
Context v2 pins both native read IDs and stable binding content/hash. Coverage
explicitly records ORIGINAL_20_SKUS, persisted request records rather than a chat
transcript, and missing country/exact-platform authority. The real workflow reader
still rejects N&T IDs and legacy-to-N&T relabelling; no catalog widening occurred.
All 265 affected offline tests pass (ten new composition tests); focused reruns cover
later freshness/stage guard changes. Compilation/dependencies pass. Synthetic reader-
interface positives do not prove stored N&T execution; real decoder/SDK fixtures
verify denial. No cloud/IAP/model calls, writes, schema changes or activation.
Release stays disabled, E2 remains open, 41 checks unavailable and full benchmark
execution 0/159. Next is explicit authoritative compatibility/transcript coverage,
then claim/evidence and durable invocation/provider-outcome bindings within the
existing E2 sequence, native verification and remaining check/release work.
Keep 2–3 file groups and focused tests. See docs/CAMPAIGN_E2_IMPLEMENTATION.md and
docs/IMPLEMENTATION_PLAN.md.

P6 E2 persisted-request authority reader — 2026-10-16 (Critical / Must have —
execution binding/shared pilot): one bounded native-correlated BigQuery transaction
now captures current workspace/membership/conversation and complete stored request
history, requiring exact target, ACTIVE owner READ_DRAFT access and unchanged
workflow-v1 validators/fingerprints. Immutable compact bindings pin accepted input,
nullable message hash, selected record/revision, access and ordered full-record
history hashes. Limits are 50 requests, 64 KiB per row, 1 MiB capture and 60-second
freshness; missing/duplicate/overflow/stale/unauthorized/corrupt data deny. Original
native SQL/config/status are independently fetched and rechecked; provider retries
and query cache are disabled. All 220 affected offline tests pass (21 new reader
tests), compilation/dependencies pass. No live cloud/IAP verification, writes,
model calls, schema/catalog changes or activation occurred. History is explicitly
PERSISTED_REQUEST_RECORDS, not user/assistant transcript text or a request-time
prefix. Existing workflow contracts still reject N&T IDs outside their original
20-SKU scope. At this reader-only checkpoint the adapter was not yet connected to
run_current_quality; the later mandatory composition checkpoint above supersedes
that gap. E2 remains open with 41 rules
unavailable and 0/159 full benchmark cases executed. At that point the next 2–3-file group was:
initial/final authoritative capture composition and drift rejection, with explicit
catalog/stage/channel/transcript compatibility. Claim/evidence and durable provider/
invocation bindings remain in the existing sequence. See
docs/CAMPAIGN_E2_IMPLEMENTATION.md and docs/IMPLEMENTATION_PLAN.md.

P6 E2 policy/prompt execution-context slice — 2026-10-15: opt-in diagnostic
composition now rereads authoritative policy and runtime prompt eligibility around
current product checks, validates installed model/profile/stage compatibility and
checks access before source reads and after final context rereads. Exact candidate
scope, supplied request/history hashes, policy/prompt/profile/access/evidence/report
pins bind an immutable context digest. Context changes, revoked access and expiry
withhold results. DRAFT planner eligibility denies; DRAFT acceptance storage never
grants release. 77 focused offline tests pass (16 new), compilation/dependencies
pass. No live cloud/IAP verification, model calls, writes or activation. Request/
history authority, durable invocation/provider-outcome binding, claim/evidence
adapters, remaining checks and chatbot integration remain pending. E2 stays open.
See docs/CAMPAIGN_E2_IMPLEMENTATION.md and
 data/generated/campaign_e2_diagnostics/execution_context_verification_20261015.json.

P6 E2 N&T product-fact adapter — 2026-10-15: separate bounded exact-SKU
BigQuery reader and opt-in protected checker composition now cover current name/
size for CI-SKU-0021–0023. Independent native-job correlation, fixed source scope,
expiry and access before/after server annotation deny conflicts or revocation.
32 focused offline tests pass (14 new), compilation/dependencies pass. No live
cloud/IAP verification, model calls, writes or activation occurred. Legacy readers
remain scoped as before. This is diagnostic composition, not installed chatbot
release, complete execution pins or claim permission. E2 remains open. See
docs/CAMPAIGN_E2_IMPLEMENTATION.md and
 data/generated/campaign_e2_diagnostics/product_adapter_verification_20261015.json.
Next: policy/prompt/model/evidence context adapters, remaining checks, full
benchmark/retrieval execution and per-interaction release integration.

P6 E2 first implementation groups — 2026-10-15: deterministic literal-field,
SKU/citation/window and channel-structure components plus a repeatable partial
runner and conservative release primitive are implemented. All 114 affected
offline tests pass (18 new); compilation/dependencies pass. Six annotated actual
E1 texts match 6/6 A01 projections but only 3/6 global E1 labels. Full E1 cases
executed remain 0/159; the runner does not invent model/RAG quality measurements.
Forty-one checks remain UNAVAILABLE and global release stays withheld. SourceFact
observations are supplied server inputs, not authoritative reads/authentication.
The release primitive is not wired into the local or shared chatbot. No cloud
reads/writes, model calls, policy changes or activation occurred. E2 remains open.
See docs/CAMPAIGN_E2_IMPLEMENTATION.md and
data/generated/campaign_e2_diagnostics/verification_20261015.json.
Next: authoritative read adapters/full execution pins, remaining semantic/safety/
usability checks, full benchmark/retrieval runs and per-interaction integration.
E1 remains complete; its reviewed labels are not observations or release grants.

P6 E1 COMPLETE — scoped user review recorded on 2026-10-15. User explicitly
approved the 47 criteria and 159 benchmark labels within the delivered scope.
config/evaluation/campaign_e1_review_acceptance_v1.json binds review digest
dc80755c1873d0e586431fe5bc551e03fdb84b6b46a335b8c296099096738862 and accepts the baseline while preserving scientific,
market/compliance, claim-use, final-campaign and serving holds. No reviewer runtime
role or IAP identity is inferred. Existing artifacts stay unchanged and the review
is a separate scoped overlay. All 96 affected offline tests pass, syntax/dependencies
pass; no closeout cloud writes or model calls. The E1 plan checkbox is now complete.
See docs/CAMPAIGN_E1_SIGNOFF.md and
data/generated/campaign_e1_diagnostics/closeout_verification_20261015.json.
Next is E2 actual quality checkers/repeatable runner and release integration;
E4/E5/R1/G4 remain open. P6 is incomplete. BigQuery policy remains DRAFT and no
claim approval, legal clearance, final campaign approval or activation follows
from the E1 review. This supersedes the pending-review checkpoint below.

P6 E1 closeout packet — 2026-10-15: technical review materials now consolidate all
47 latest-v0.2 criteria with four-stage conditional applicability, four channel
deliverable rubrics, seven source-authority/conflict rules, 16 conservative
country/channel dispositions, eight visual rubric items and pinned original-image
specimens. Sixteen additional proposed scenarios bring the review corpus to 159,
including explicit pending/rejected claims and three further multi-turn cases.
The 94 isolated-rule examples are explicitly distinct from global campaign-release
labels; no model quality was measured. All 94 affected offline tests pass (eight
new), syntax/dependencies pass. No cloud writes or model calls occurred.
See docs/CAMPAIGN_E1_SIGNOFF.md and
data/generated/campaign_e1_diagnostics/closeout_verification_20261015.json.
E1 technical packet is READY_FOR_HUMAN_REVIEW. The fixed E1 exit criterion still
requires reviewed criteria/labels; no user/manager signoff is invented. Unknown
formula/report evidence and unreviewed markets are represented as holds, not
implicit approvals or new E1 implementation prerequisites. Actual checking,
measured thresholds and rendered artifact calibration remain E2/E4/E5. Keep E1
open until explicit review acceptance/corrections; DRAFT policy and serving gates
remain unchanged. This supersedes earlier assertions that the rubric/source
precedence/coverage materials were not yet prepared.

P6 E1 brand/catalog and versioned draft checkpoint — 2026-10-15: user explicitly
authorized adding N&T products to the existing catalog, extending the preserved
20-SKU baseline with CI-SKU-0021 Renew 30 mL, CI-SKU-0022 Rejuvenate 35 mL and
CI-SKU-0023 Prevent 35 mL. Native append-only import added three products, 41
source-specific ingredients, 54 ordered links and three original hash/generation-
pinned GCS images. Exact full before/after projections preserve every original
row in all four tables. The first native import failed on ARRAY/EXCEPT support;
positive child rollback and identical snapshots were verified before the single
explicit corrected slot. Its native success and same-job recovery passed with
unchanged GCS generations. Failed evidence is retained. No existing IDs were
remapped and no synthetic sales/campaign records or claim approvals were added.
Existing shared-chat readers still deny the new source/IDs; integration is pending.

New BigQuery policy draft-v0.2 contains one header and 47 rules with the user ban
on all AI people/before-after imagery and US/CA/GB/AU benchmark scope. Independent
full decoding verifies v0.1 unchanged and both versions not serving-ready.
Twenty-six real-brand proposed cases extend the original 117 to 143. User confirms
website summaries only and an appearance-focused wording baseline, not claim
permission. Formula versions/reports, label review, country/channel overlays,
visual rubrics and approved claim records remain pending. All 86 affected offline
tests pass (32 new), syntax/dependencies pass; no model invocation, IAM/deployment,
manager approval or activation. E1 and P6 remain open. See
docs/CAMPAIGN_E1_REVIEW.md and
data/generated/campaign_e1_diagnostics/verification_next_20261015.json.
Next E1 work: criteria/label review, reviewed factual-only brand cases within
available evidence, source/wording decisions and visual rubric examples. E2/E4
runner/release enforcement and measured thresholds remain required separately.

P6 E1 draft benchmark checkpoint — 2026-10-15: user directed E1 implementation,
selected US/CA/GB/AU for the first benchmark with Europe deferred, prohibited all
AI people and all before/after imagery, and delegated product selection to the
playbook. Slide 98 supplies Renew Resurfacing Night Serum, Rejuvenate Restorative
Face Oil and Prevent Anti-Aging Face Oil. Names are selected; authoritative
SKU/formula/current evidence mapping remains pending. New draft-v0.2 review
package has all 47 source-pinned criteria and 117 proposed cases (105 synthetic,
12 playbook-name-only; eight multi-turn). Its validator returns
VALID_DRAFT_NOT_READY, never approval or measured model-quality results.
All 54 affected offline tests pass, including 14 new package guards; syntax and
dependencies pass. See docs/CAMPAIGN_E1_REVIEW.md and
data/generated/campaign_e1_diagnostics/verification_20261015.json.
Original specification/hash and BigQuery draft-v0.1 remain unchanged. The newly
confirmed restrictions are local draft overlays, not yet versioned BigQuery
configuration or runtime enforcement. No cloud/model calls, serving writes,
approvals or activation occurred. E1 remains open. Immediate E1 work is real
product/evidence mapping, realistic brand cases, criteria/label review and new
versioned configuration; E2 runner/release gates and E4 measurements remain
separate. The earlier management publisher/executor/native gaps remain open.

P6 reviewer-management capture/binding checkpoint — 2026-10-15: a single fresh
native-correlated query now captures the operator policy, permission fence and
action-specific grant/review/permission inputs. Explicit action roles and full
historical checkpoint pins gate capture release; expired/revoked source grants
remain usable only for authorized review cleanup. Disabled management plans now
require a matching decoded policy snapshot and check its exact authoritative
head/full history inside the transaction before either replay or mutation.
All 152 affected offline tests pass (25 new capture/binding cases); syntax and
dependencies pass. No cloud calls/writes, model calls, approvals or activation
occurred in this slice. Evidence:
data/generated/campaign_review_management_diagnostics/verification_20261015.json.
This closes the capture/source-binding gap stated in the previous checkpoint,
within implementation/offline scope. Native positive/race verification remains
pending. Next: controlled operator-policy publication/bootstrap using the shared
permission fence, trusted management execution/recovery, then isolated native
verification. Concurrent revocation ordering requires that common writer fence;
SQL snapshot assertions alone do not establish cross-writer serialization.
P6 remains incomplete; acceptance rules/planner remain DRAFT and serving disabled.

P6 operator-policy store checkpoint — 2026-10-15: separate canonical fixed-ID
head/history schemas and a fresh native-correlated reader are implemented.
Two BigQuery tables, campaign_review_operator_policy_heads/versions, were
created/verified empty in ci_brand_memory, asia-south1; reapply created nothing.
A native empty-store read denied missing policy. No operator identities, policy
rows, grants or approvals were seeded. All 59 focused offline tests pass (23 new
storage/setup/read cases), with syntax/dependencies passing. Evidence:
data/generated/campaign_operator_policy_diagnostics/verification_20261015.json.
Supplied identity/record metadata does not prove deployed IAP or publisher signoff;
positive/revoked/racing stored-policy native fixtures remain pending. Current
management SQL still needs transaction-local policy-source binding and trusted
execution/captures. Next: those captures/binding, controlled policy publication
and management execution/recovery, then native verification. P6 remains incomplete,
acceptance rules DRAFT and serving bootstrap not performed.


P6 reviewer-management foundation: campaign_review_publication_contract.py and
campaign_review_publication_query.py now define five pure grant/review transitions
and disabled fenced plans. Explicit operator/reviewer/revoker policy is supplied,
not inferred from bootstrap or ADC and not yet authoritatively stored/configured.
Review creation binds current grant and exact stored DRAFT permission content;
reviews are immutable except terminal revocation. Expired records remain
revocable; expired grants cannot authorize new reviews. Shared control CAS,
exact replay and atomic REVIEW_CONTROL_* audits preserve the existing schema.
All 91 focused offline tests pass (21 new cases), with compilation/dependencies
passing. No cloud calls, real grants/reviews, approvals or activation occurred.
Next: authoritative operator-policy/capture and disabled execution/recovery,
then native management verification. P6 remains incomplete; acceptance rules
DRAFT and serving bootstrap not performed. Management receipts are a separate
namespace not handled by existing permission receipt readers.


P6 native bootstrap checkpoint: all 16 reader/executor checks passed across CORE,
FAULTS and DENIALS. Two conflicting owners/manifests share one reserved fixture
job; only its exact winning config commits, while the loser stays UNKNOWN.
Original/cold/expired-window recovery, duplicate admission, rollback/no-remint,
postcommit, lost response and orphan/partial/overflow/existing-genesis denials
are verified. Five INSERT attempts produce four unique jobs (three COMMITTED,
one positive NOT_COMMITTED); 16 native reads correlate exactly. All eight owned
13-table datasets were deleted and absence verified. All 139 focused offline
tests, syntax and dependencies pass. Evidence:
data/generated/campaign_bootstrap_diagnostics/verification_20261014.json.
No production bootstrap slot, serving/model writes or activation occurred.
Authentication/owner identities remain synthetic; this is not deployed IAP or
business approval. Next: fenced reviewer grant/review-capture publication with
explicit operational authority. P6 remains incomplete; rules DRAFT and serving
bootstrap not performed.


P6 authoritative bootstrap runtime checkpoint: campaign_bootstrap_reader.py now
captures all 13 tables in one bounded native-correlated read with explicit marker/
count/size checks. campaign_bootstrap_executor.py adds disabled trusted IAP owner
execution, fresh empty-state admission, one global native job, original-job-only
recovery and fresh exact-genesis receipt release. UNKNOWN/conflict/known rollback
never advances a slot; expired dispatch windows permit read-only recovery with a
fresh owner assertion. Advanced storage withholds the immediate-genesis receipt.
All 132 affected offline tests pass (23 new runtime cases), with syntax/dependencies
passing. No live cloud calls, bootstrap writes, grants or activation occurred.
Next: isolated native bootstrap reader/executor verification, then fenced reviewer
grant/review publication. P6 remains incomplete; operational owners unconfigured,
policies DRAFT and serving tables unbootstrapped by this work.


P6 singleton-bootstrap foundation: campaign_bootstrap_contract.py and
campaign_bootstrap_query.py implement pure disabled genesis proposals for an
explicit IAP owner/audience and 1–300 second manifest. The fixed global native
slot is independent of caller/configuration/version; no retry/alternate slot is
available. A complete supplied 13-table snapshot and transaction-local emptiness/
exact-genesis checks deny orphan, partial, duplicate or conflicting state. Only
control and immutable history are inserted; reviewer grants/claim approvals are
not inferred. All 109 affected offline tests pass (14 new bootstrap cases), with
compilation/dependencies passing. No cloud calls, native bootstrap or activation
occurred. Next: authoritative bootstrap captures and disabled original-job
executor/recovery, then isolated native verification and grant/review publication.
P6 remains incomplete; policies DRAFT and serving tables unbootstrapped.


P6 native trusted publication/receipt checkpoint: all 13 adapter/receipt cases
passed in an owned expiring 13-table dataset. Five publication submissions have
four COMMITTED and one positive NOT_COMMITTED outcomes; 66 authoritative reads
verify current/historical receipts, original recovery, absence, revocation,
postcommit/rollback and corruption denials. Bounded initial UNKNOWN observations
recover the original job without another mutation. All 95 focused offline tests,
compilation and dependencies pass. Successful and initially failed fixture datasets
were deleted and absence verified; the failed report remains FAILED. See
data/generated/campaign_publication_adapter_diagnostics/verification_20261014.json.
Authentication is synthetic; deployed IAP, actual manager approvals and concurrent
release-time revocation are not natively proven. Serving/model writes and activation
remain zero. Next: disabled controlled singleton bootstrap and fenced reviewer
grant/review publication. P6 remains incomplete, policy DRAFT and serving tables
unbootstrapped.


P6 historical receipt checkpoint: campaign_publication_receipt.py now verifies
exact audit plus immutable permission/control prefixes and committed revisions
from a fresh bounded BigQuery snapshot. PublicationExecutor.recover_receipt gates
release on current IAP/reviewer authority; later expiry/revocation does not erase
historical proof or restore eligibility. Absent receipt remains UNKNOWN; recovery
never mutates. All 89 affected offline tests pass (11 new receipt cases), with
compilation/dependencies passing. No live cloud calls or activation occurred.
Next is isolated native adapter/receipt verification, then controlled bootstrap
and grant/review publication. Internal intent persistence and larger-history
retention remain unimplemented. P6 remains incomplete and policies DRAFT.


P6 trusted publication adapter checkpoint: campaign_publication_executor.py now
provides disabled-by-default IAP-authenticated internal intent preparation, fresh
explicit grant/review captures, single deterministic submission after authoritative
NotFound, original-job-only recovery and current-authority result release gates.
All 78 affected offline tests pass (12 new adapter cases); compilation/dependencies
pass. No new cloud calls, serving writes or activation occurred. Native adapter
verification and authoritative historical business-receipt recovery remain pending,
then controlled bootstrap/grant/review publication. Internal intents are server-owned,
not an HTTP serialization format. P6 remains incomplete; rules stay DRAFT.


Priority 5 Batch 13 (High / Must have — shared pilot) is COMPLETE: real IAP/ADMIN
review and hash confirmation registered Vertex planner 8077808464061005824,
version 1, as DRAFT revision one with one BigQuery history row. Independent full
current/history decoding, exact Vertex parity and successful original native job
SQL/config/creator match the APPLIED receipt. Snapshot SHA-256 is
78b7c867f499c0aab130fff55914ecce89667ac9e7190026dd9399183108340c; record digest is
02959bbc5feff4ff1f2882b0edec4d2d08fb5e1e107781e432f763c0ad6cb947.
No ACTIVE planner or model invocation. Original job is
prompt_register_de372de65e1778fc05a6a1e3d47aa42de4b4c3d6afe726c201a549ad3bdb1ff0.
The independent operator cannot read the runtime's anonymous query results; it
verified successful exact transaction metadata and full authoritative rows without
replaying a mutation or widening IAM. The runtime UI verified its own receipt.
Configuration-only Ready revision ci-prompt-registrar-00007-gcc closes registration
writes; bootstrap stays disabled. Native comparison verifies the same image,
workspace, IAP, runtime and capacity. Existing IAM/membership rows are preserved.
Priority 5 is 16/16 implementation milestones complete within documented scope.
This supersedes earlier 15/16/pending-registration checkpoints retained below.
At the registration checkpoint, next work was Critical / Must have native verification within existing
Batches 14–16 in 2–3 file groups, then Priority 6 High / Must have measured
benchmark/evaluation and reviewed policy/threshold gates before activation.
Company Workspace/shared marketing UI SSO and serving generation remain pending.
See docs/PROMPT_REGISTRATION.md and docs/IMPLEMENTATION_PLAN.md. The completion
slice changes only operational configuration, evidence and documentation; no code
tests were repeated. The last affected code checkpoint passed 105 tests.

The subsequent Batch 14 native storage/recovery slice (Critical / Must have —
shared pilot) is complete: src/validation/prompt_evaluation_integration.py and
prompt_evaluation_fixtures.py relocate production write/read SQL to eight exact,
owned expiring tables. All 12 native cases passed: atomic control/report/cases,
authoritative decoding, same-job recovery, new-job exact reuse, conflict/explicit
rollback, discarded-response recovery, post-commit failure without an invented
receipt, expiry, control replacement/stale denial/historical replay and revocation
with unrelated fixture preservation. Seven native assertion codes were verified
against recorded commit statuses and are now enforced on future runs. All 85
affected offline tests pass (14 new probe guards); compilation and dependencies
pass. The dataset was deleted and absence verified; eight serving table row counts
match before/after metadata, not a full row checksum. Two initial fixture-only
SDK/config failures remain FAILED with verified cleanup and regression tests.
Serving writes, Vertex/model calls, GCS reads, IAM/deployment changes and activation
were zero. Typed identity and benchmark/policy/observations are synthetic fixtures,
not deployed IAP verification, measured quality or live approval. Priority 5 remains
16/16 implementation milestones complete within scope. At that checkpoint, next was existing
Batch 14 overlap and partial/orphan-state native gates; then Batch 15 lifecycle and
Batch 16 dispatch-binding/promotion-versus-claim verification; then Priority 6
measured quality and reviewed policy. Keep 2–3 file implementation groups. See
docs/PROMPT_EVALUATION.md and docs/IMPLEMENTATION_PLAN.md for evidence and limits.

Batch 14's planned work is now COMPLETE within implementation/isolated native
verification scope (Critical / Must have — shared pilot). The subsequent three-file
code group adds src/validation/prompt_evaluation_remaining.py, nine guard tests
and an explicit --remaining-only route on the existing probe. All eight remaining
native cases passed, closing the 20 planned native cases across two reports:
genuine overlapping control/report transactions with one complete winner and
positive loser rollback; missing head/history, a history gap, reports without/
with missing cases and orphan cases. Six readers and nine writers deny with
expected errors; exact sorted snapshots of all eight fixture tables match after
denials. Race winners have exactly one workspace-fence increment, full canonical
decoding and preserved rows outside the permitted scope. All 94 affected offline
tests pass; compilation and dependencies pass. Native child intervals prove actual
overlap; distinct fixture native slots exercise SQL fencing without changing
production job IDs. Dataset deletion/absence and unchanged serving row metadata
are verified. The earlier 12 native cases were not repeated. No production writer,
service or schema changes, serving writes, Vertex/model calls, GCS reads, IAM/
deployment changes or activation occurred. Supplied fixture identities/observations
do not authenticate users, measure quality or approve business content. No extra
Batch 14 scope is added. At that checkpoint, next planned verification was Batch 15
lifecycle, then
Batch 16 dispatch binding/promotion-versus-claim, then Priority 6 measured quality
and reviewed acceptance policy. These retain the fixed priorities and 2–3 file
groups. See docs/PROMPT_EVALUATION.md and docs/IMPLEMENTATION_PLAN.md.

Batch 15 is now COMPLETE within implementation/isolated native verification scope
(Critical / Must have — shared pilot). All 21 planned native cases passed across
two final PASSED reports: 13 core lifecycle/recovery/retry cases and eight remaining
UNKNOWN/expiry/policy/revocation/concurrency cases. Native atomic replacement
exposed an insufficient 512 MiB cumulative query cap; the bounded write cap is now
768 MiB, with replacement/rollback verified. Probe metadata/snapshot/cleanup RPCs
are bounded at 30 seconds; the fixture store retains the allowed 20-second RPC cap.
Nine expected native fault/denial observations match exact commit classifications.
The promotion race has 46.688 seconds of real native overlap, one complete winner
and positive loser rollback at the unpartitioned workspaces fence; this is not an
isolated global-head-CAS conflict test. Full control/history prefixes, both candidate
versions, workspace fence/immutable fields and exact unrelated/denied fixture rows
are checked. Global-slot competition grants no retry to the losing UNKNOWN intent.
All 79 focused tests pass, including 23 probe guards; compilation/dependencies pass.
The earlier full offline suite passed 2,185 tests; continuing verification must
focus on impacted files/functionality and direct dependencies, without unnecessary
full-suite repetition. Both owned ten-table datasets were deleted and absence
verified. Four intermediate FAILED reports retain verified cleanup; they do not
replace the two final PASSED group reports. Serving metadata row counts match,
not a full serving-row checksum. Serving writes, Vertex/model calls, GCS reads,
IAM/deployment changes and activation were zero. Fixture identities/observations
are synthetic, not deployed user authentication or measured campaign quality.
Priority 5 remains 16/16 implementation milestones complete within documented scope.
At that checkpoint, next was existing Batch 16 native dispatch binding/promotion-
versus-claim (Critical / Must have), now complete below, then Priority 6 measured
benchmark and reviewed acceptance policy (High / Must have), in 2–3 file groups.
No new batch is added. See
docs/PROMPT_LIFECYCLE.md and docs/IMPLEMENTATION_PLAN.md; combined evidence is
data/generated/prompt_lifecycle_diagnostics/batch15_completed_20261014/verification.json.

Batch 16 is now COMPLETE within implementation/isolated native verification scope
(Critical / Must have — shared pilot). All 19 cases pass in three final reports:
nine core, eight denials and two race cases. Native dispatch exposed an insufficient
512 MiB cumulative cap and 60-second script timeout, including a timeout after
commit without a receipt. Registered dispatch now has a 768 MiB cumulative cap,
120-second script timeout and 90-second bounded pending result wait; statement
limits, caller deadlines/cancellation and legacy settings remain enforced. Inline
typed expectations and one combined assertion retain every exact source comparison
while removing 19 native statements and staying below the 64-child recovery bound.
Atomic binding/invocation/attempt/audit/fence, exact readback, duplicate/lost/UNKNOWN
recovery, rollback after binding, postcommit failure without invented receipt,
expiry, policy/revocation and missing/corrupt/stale state all pass. Nine correlated
native fault/denial observations are verified. The promotion-versus-claim race has
41.691 seconds of actual overlap, one complete claim winner and positive rollback
at the unpartitioned workspaces fence; isolated lifecycle-control-only contention
is not claimed. Exact sorted snapshots of all 20 fixture tables match after denied
mutations; race rows outside the permitted winner scope are preserved.
All 145 focused tests pass, including 24 probe guards and direct ControlIO/budget
dependencies; compilation/dependencies pass. No full suite was repeated. Three
final owned expiring datasets were deleted and absence verified. Thirteen FAILED
intermediate reports retain verified cleanup; an earlier PASSED race is separate
from the final-source evidence. Serving row-count metadata matches, not a full row
checksum. Serving writes, Vertex/model calls, GCS reads, IAM/deployment changes and
activation were zero. Synthetic identities/evaluations and fixture runtime
resolution do not prove deployed IAP, measured quality or business approval;
provider/one-shot adapter/result-release gates remain offline verified. The binding
deadline case is not a live-approved-policy expiry test. The registered planner
remains DRAFT, with shared serving disabled. Priority 5 remains 16/16 implementation
milestones complete; Batches 14–16's planned isolated native gates are complete.
Next is Priority 6 (High / Must have), measured benchmark/evaluation and reviewed
policy/thresholds before authorized promotion. Shared UI/deployment activation
retains Priority 9/12 gates. No new batch is added; keep 2–3 file groups and focused
tests. See docs/PROMPT_RUNTIME.md and docs/IMPLEMENTATION_PLAN.md; combined evidence
is data/generated/prompt_runtime_diagnostics/batch16_completed_20261014/verification.json.

Priority 6 scope clarification — 2026-10-14: accuracy, grounding, compliance and
usability apply to every enabled interaction, including interpretation,
clarification, campaign generation and refinement. Priority 6 explicitly owns
reviewed stage-specific acceptance criteria, implementation/verification of
per-response and artifact-release quality gates, and bounded versioned decision
evidence in BigQuery under existing E1/E2/E4/E5/R1/G4 milestones. Insufficient
evidence and failed/unavailable/inconclusive required checks must clarify, block
or require review; prompt approval never bypasses current runtime checks. Reuse
G1/G2/X1 contracts and privacy controls. Priority 6 completion requires measured
benchmark results plus verified runtime gates for every enabled interaction;
Priority 9/12 wiring/deployment must preserve them. Evaluation is High / Must
have; response/artifact release enforcement is Critical / Must have. This is
authorized planned scope, not implemented behavior or a new priority/batch.

## GCP Project

P6 isolated publication checkpoint — 2026-10-14: all 15 native CORE/denial/race
cases pass, including atomic head/history/audit writes, exact replay, original-job
and discarded-result recovery without resubmission, rollback/post-commit error
classification and genuinely overlapping genesis with one winner/rolled-back
loser (6.143-second overlap). Full owned 13-table snapshots verify permitted
changes and unrelated preservation; all three successful datasets and the one
failed initial dataset were deleted and absence verified. 66 focused tests,
compilation and dependencies pass. Evidence is
data/generated/campaign_publication_diagnostics/verification_20261014.json.
Native evidence pins publication-only profile v2 to 768 MiB, 120-second root
timeout/plan window, with typed empty temp tables instead of empty array params;
profile settings bind the command/native ID. The initial budget/config failure
remains FAILED. Synthetic reviews/identities do not authenticate managers,
substantiate brand claims or measure quality. Serving rows/model calls/IAM/
deployment/activation were zero; no independent serving-row checksum was taken.
Next is disabled trusted execution/recovery, then controlled bootstrap and
grant/review publication; actual approval/evidence mapping and runtime quality
enforcement/benchmarks remain pending. The 47 rules remain DRAFT, the 13 serving
permission/review/control tables remain unbootstrapped, and P6 is incomplete.
See docs/IMPLEMENTATION_PLAN.md for exact evidence and limits.

P6 BigQuery DRAFT setup — 2026-10-14: user-authorized setup created
ci_brand_memory.campaign_acceptance_policy_versions and
ci_brand_memory.campaign_acceptance_rule_versions in asia-south1 and verified
one header plus all 47 rules for naked-and-thriving-campaign-acceptance,
draft-v0.1. Approval is null and serving is disabled. Specification hash is
6c3e95a6a33e2a6993736db5407860b0367d9aae1f1be839f63c8a4e38ac6e18.
src/ingestion/configure_campaign_acceptance.py provides exact preflight,
atomic append/original-job recovery and fresh full-row --check verification.
27 focused tests and compilation pass. No policy approval, claim eligibility,
runtime reader/enforcement integration, promotion or serving activation is
implied. Existing policy specification remains the immutable v0.1 source;
new reviewed changes need a new version. See docs/IMPLEMENTATION_PLAN.md.

Priority 6 proposed policy — 2026-10-14: docs/CAMPAIGN_ACCEPTANCE_POLICY.md
defines accuracy, grounding, compliance and usability rules plus Holiday 2025
playbook conflicts. User confirms Naked & Thriving is the actual brand and its
website may be evidence after explicit SKU/formula mapping; this supersedes
earlier styling-only restrictions, including the older role constraint. Preserve
SKU identities/provenance and synthetic labels. Website publication is not
automatic benefit permission or underlying study verification. Use approved
claim library plus Marketing Manager final campaign review; drafts only.
English for Canada and broad Europe. Production versioned BigQuery policy/claim
records and runtime gates remain pending; documentation does not approve or
activate policy.

Project ID:

project-661d501a-e709-4276-a26

Region:

asia-south1

Cloud Storage bucket:

gs://creative-brand-memory-project-661d501a-e709-4276-a26

## BigQuery datasets

- ci_product
- ci_sales
- ci_creative
- ci_brand_memory
- ci_application

## Application workflow infrastructure

The ci_application dataset and 11 campaign-workflow-v1 tables were created and
verified in asia-south1 on 2026-10-10. All tables were empty at setup verification.
The idempotent resource setup is src/ingestion/setup_workflow_tables.py;
run --check for read-only verification or --apply to create missing resources.
It refuses incompatible schemas/layouts and never writes or deletes data rows.
Pure request/job/attempt/stage-event/model-invocation/output/artifact/profile/idempotency validators
are implemented in src/application/workflow_contract.py. Attempt/event validators
check parent scope, lifecycle and ordered progress; they do not prove dispatch,
grant access or persist rows. Output/artifact contracts preserve POC
provenance and scoped pinned object metadata; they do not prove current access,
evidence eligibility or GCS byte integrity. The server-only IAP identity verifier and
fresh BigQuery membership/conversation reader are implemented in
src/application/identity.py and src/application/access_reader.py. The protected
gateway in src/application/protected_service.py connects them for job
status/output summaries and protected artifact downloads through current
parent-conversation access. Downloads resolve full BigQuery records, verify pinned
GCS generation/hash/size/format through src/application/artifact_download.py,
and reread access and manifest before returning bytes. It is not
wired to the local UI. Opt-in management/bootstrap and generation admission guards
are now implemented under 4.4 below; general loaders/persistence, model binding,
UI integration and shared SSO activation remain pending. Do not infer that empty memberships grant access.
See docs/WORKFLOW_DATA_DESIGN.md for the contract and verification evidence.
Model-invocation validators check pinned stage profiles, dispatch/outcome metadata,
nullable usage, exact Decimal costs and revisioned update proposals. They do not
authorize dispatch, calculate prices, reconcile billing or provide transaction fencing.
Pure evidence row/bundle validators are implemented in
src/application/evidence_contract.py. They validate versioned source-specific
projections, provenance, hashes, source/citation identity, image/review/vector pins,
parent scope and output links. Multiple source records may support one citation.
They do not read local evidence, create snapshots, verify current eligibility,
download image bytes or implement atomic publication. Fresh BigQuery/GCS checks
and a reader-to-projection adapter are still required before durable writers.
Pure audit row/replay validators are implemented in src/application/audit_contract.py.
They bind exact audit schemas to captured user/anonymous access decisions or typed
service observations, resource scope, revisions, mutation outcomes and timestamps.
Replay requires identical logical keys/content. They do not authenticate actors,
grant access, validate domain transitions, prove commit outcomes or persist audits.
Pure workspace control validators are implemented in src/application/workspace_contract.py.
They check complete ACTIVE/DISABLED rows, initial revision-one proposals, immutable
identity/creation time, explicit status changes and ACTIVE-only fence increments.
The access reader applies these row checks. Proposals do not authenticate, bootstrap,
persist or lock anything. Pure membership contracts are implemented in
src/application/membership_contract.py and shared by the access reader. They bind
principal IDs to IAP issuer/subject, validate roles/status/origin and revisioned
creation/change proposals, and evaluate ADMIN mutations against supplied access
snapshots. They do not authenticate actors, prove current access, bootstrap users
or persist assignments. Pure conversation contracts are implemented in
src/application/conversation_contract.py and shared by the access reader. They
validate complete rows, private creation with actor-derived ownership, revisioned
creator/owner sharing proposals and structural ACTIVE-to-ARCHIVED transitions.
Archived rows cannot mutate or restore through these contracts. The separate
4.4 management path now defines CREATOR/owner archival and atomic auditing;
execution is disabled by default and active related jobs block archival.
Proposals do not authenticate, persist, prove uniqueness or acquire locks.
The offline sharing coordinator contract is implemented in
src/application/sharing_coordinator_contract.py. It binds a stable hashed intent
to immutable workspace-fence/conversation/audit proposals, validates bounded
unique input rows and current supplied creator/owner access, and distinguishes
PREPARED, REUSED and UNKNOWN outcomes. Receipt IDs reserve a scoped retry prefix
and encode full retry/command digests in the existing audit schema. Only atomic
APPLIED sharing audits may use this namespace. Unknown commit plus no receipt
never produces a write proposal; historical reuse does not change current state.
The coordinator contract performs no query execution, authentication,
persistence or live commit verification.
Controlled management paths and transaction/recovery verification remain required
before writers.
The parameterized sharing SQL/query contract is now implemented in
sql/share_conversation.sql and src/application/sharing_query.py. It defines
transaction-local access/receipt checks, scoped conditional fence/visibility
updates, explicit audit insertion, rollback/rethrow and post-commit result
validation. Query settings bound bytes/time and disable cache/session mode.
This module constructs SQL/SDK configuration without submitting it. Offline
tests do not verify native SQL execution, transaction races or job outcomes.
The server-only adapter in src/application/sharing_executor.py and intent sealer
in src/application/sharing_intent.py now connect verified headers, current
sharing access, schema preflight, stable job IDs and original-job reconciliation.
Recovery never submits a new query; failed scripts require positive correlated
child commit/rollback evidence before classification. Signing keys are supplied
only through trusted runtime configuration, with no stored key/default identity.
Execution is disabled by default. Adapter tests use offline fixtures, including
real ES256 signatures. The isolated native sharing probe in
src/validation/sharing_integration.py passed all 11 cases in asia-south1,
including native overlapping transactions, rollback, failure after commit,
duplicate submission and original-job recovery. Its owned dataset was deleted;
no serving users or business data changed. The previous full-suite baseline was 944 tests.
See docs/SHARING_INTEGRATION.md for evidence and limits. Execution stays disabled
by default. The separate read-only recover_receipt method and
src/application/sharing_receipt.py now recover validated business-intent receipts
from a single bounded BigQuery snapshot with fresh access checks. Receipt absence
stays UNKNOWN and cannot authorize a new mutation; later visibility changes are
preserved. The receipt does not classify a specific native mutation job.
Verification: 181 impacted tests and six focused native fixture cases passed,
with the owned dataset deleted. See docs/SHARING_RECEIPT_RECOVERY.md.
The pure controlled-retry proposal/seal contract in src/application/sharing_retry.py
now preserves the original envelope/window, binds positive supplied noncommit to
the exact predecessor and caps retries at three with deterministic job IDs.
All 211 affected tests passed, including 30 new retry tests. It performs no native
verification, access checks or submission; typed observations/seals are not proof.
See docs/SHARING_RETRY_CONTRACT.md. The trusted executor now provides issue_retry,
submit_retry and recover_retry, and accepts retry tokens for read-only receipt
recovery. New dispatch requires freshly correlated positive ancestor rollback,
current BigQuery access/controls/receipt checks and unchanged business pins/sealed
fence. Known retry jobs recover without resubmission; UNKNOWN never remints a new
attempt. All 252 affected offline tests passed, including 41 new adapter tests.
No cloud calls occurred in that adapter slice. See docs/SHARING_RETRY_EXECUTOR.md.
The separate --retry-only native probe now passed ten cases, including positive
rollback/retry, overlapping competing retries, stable duplicate/discarded-response
recovery, stale fences, revocation, post-commit failure and historical receipt reuse.
All 266 affected offline tests passed, including 14 new probe guards. The owned
fixture dataset was deleted, serving writes were zero and execution was not
activated. These are scoped native SQL/seal/job checks, not deployed IAP/HTTP
executor verification. See docs/SHARING_RETRY_INTEGRATION.md. The opt-in retained
sealer and signing policy in src/application/sharing_retained_sealer.py and
src/application/sharing_signing_contract.py now preserve original key/profile,
seven-day intent windows and stable IDs across planned rotation and nested retries.
Retired keys remain retry-signing keys for their original cohort. Revocation,
retention/transition checks and installed profile compatibility are tested offline;
304 affected tests pass, including 38 new tests. See docs/SHARING_SIGNING_RETENTION.md.
The opt-in src/application/sharing_secret_loader.py now composes retained sealers
from explicit regional numeric Secret Manager versions with ADC client creation,
exact response-name/CRC32C checks, bounded retry-disabled RPCs and no partial/cache
fallback. Secret-version pins have pure transition checks. All 335 affected offline
tests pass, including 31 new real-SDK/mocked-RPC tests; dependency checks pass.
No live secret access/provisioning or default activation occurred. Trusted project
number verification and live resources remain pending. Pure publication and refresh
contracts in src/application/sharing_publication_contract.py and
src/application/sharing_refresh_contract.py now bind increasing revisions/parent
hashes, immutable retired-key/version/profile history and fail-closed replica
generations/readiness leases. STOPPED records active-key revocation in history and
produces no load ticket. Higher observed revisions survive failed key loading;
stale callbacks/rollback/conflicts are rejected against supplied current state.
All 379 affected offline tests pass, including 44 new tests. These are proposals
and supplied observations, not authoritative reads/publication, locks, loaded-key
proof or instant cross-replica revocation. See docs/SHARING_PUBLICATION_REFRESH.md.
Step 4.1 (High / Must have — shared pilot) is complete offline: four separate
control schemas and a pure supplied-snapshot read contract are implemented in
src/application/sharing_publication_schema.py and
src/application/sharing_publication_read_contract.py. They define unique head,
complete bounded chain/hash/history checks, trusted checkpoint floors and retention.
All 443 affected offline tests pass, including 33 new tests. See
docs/SHARING_PUBLICATION_STORAGE.md. The resource/read slice of 4.2 (Critical /
Must have — shared pilot) is now implemented: src/ingestion/setup_signing_tables.py,
src/application/sharing_publication_codec.py and sharing_publication_reader.py
provide scoped missing-table creation, exact canonical metadata/hash decoding and
fresh bounded single-transaction reads with exact native-job correlation. The four
control tables were created and verified empty in asia-south1 on 2026-10-11;
reapplying setup created nothing. All 482 affected offline tests pass, including
39 new tests. All 14 isolated native SQL/codec read cases passed; the owned fixture
dataset was deleted. See docs/SHARING_PUBLICATION_READER.md for evidence and
limits. The 4.2 writer slice now implements server-only explicit IAP global
operator/consumer grants, frozen authoritative captures, explicit-schema fenced
publication/checkpoint transactions, immutable replay stamps, stable original
job IDs and read-only uncertain-outcome recovery. All 519 affected offline tests
pass, including 37 writer/probe tests. All 13 isolated native writer cases passed,
including three actual overlapping races, rollback/post-commit recovery and
preservation of other policy/consumer rows. The owned dataset was deleted; a
read-only serving-table check verified all four tables with zero rows.
The writer requires an existing singleton head and is disabled by default;
process-local admission is not a durable queue or deployed IAP/HTTP integration.
See docs/SHARING_PUBLICATION_WRITER.md. Controlled genesis bootstrap is implemented
in src/application/signing_bootstrap.py and sql/signing_bootstrap.sql. It pins one
verified IAP owner and immutable five-minute manifest, reserves three global
native job slots independent of owner/configuration, requires fresh positive
correlated ancestor rollback before a later slot, and atomically creates the
initial publication/history/head. UNKNOWN cannot advance; existing partial policy
state denies without reset. Default bootstrap is disabled and no serving route
is wired. All 546 affected offline tests pass, including 27 new bootstrap/probe
tests. All 13 isolated native bootstrap cases passed, including identical and
conflicting callers sharing one native job, rollback/retry, post-commit recovery,
the three-slot cap and preserved unrelated rows. The owned dataset was deleted;
read-only verification still shows four empty serving controls. See
docs/SHARING_SIGNING_BOOTSTRAP.md. Step 4.2 is complete within its implementation
and isolated native verification scope; current Priority 4 progress is recorded below.
The 4.3 runtime sealer slice is implemented in src/application/sharing_runtime.py:
fresh BigQuery reads and pinned secret loads are serialized with state/key replacement
and signing/resolution result gates. Missing or lagging authoritative consumer
checkpoints deny readiness; failures retain revision floors and drop key references.
Execution is disabled by default. All 575 affected offline tests pass, including
29 new runtime tests. No live calls occurred. See docs/SHARING_RUNTIME_REFRESH.md.
The opt-in RuntimeProtectedSharingExecutor in sharing_runtime_executor.py now
composes serialized fresh dispatch and result-release gates, including current
access after pinned key loading. Known-job, UNKNOWN, REUSED and issuance exits are
gated; original IDs/recovery/commit classification remain unchanged. The plain
executor rejects runtime composition without dispatch/release hooks. All 613
affected offline tests pass, including 38 new adapter tests and a real signed IAP
fixture case. No live calls or activation occurred. See
docs/SHARING_RUNTIME_EXECUTOR.md.
The owned native runtime probe in src/validation/sharing_runtime_integration.py
now has 26 guard tests; all 639 affected offline tests pass. CLI and native ADC
lookups confirmed project number 1042059373096. After initial SERVICE_DISABLED
preflight, Resource Manager and Secret Manager APIs were approved and enabled.
All 14 isolated native runtime cases passed, including restart/checkpoint floors,
real rotation, disabled/revoked keys, fresh-access denial, STOPPED policy and
original-job committed recovery after discarded submission response with no
second mutation submission. Both owned resources were deleted and absence verified.
An initial overly strict exact-TTL ownership guard was fixed with bounded server
stamp tolerance/regression tests; its refused secret cleanup was recovered against
exact name/labels/captured creation time/etag, and that failed report remains FAILED.
The four serving signing controls were checked and remain empty. See
docs/SHARING_RUNTIME_INTEGRATION.md. Step 4.3 (Critical / Must have — shared pilot)
is complete within implementation/isolated verification scope. Priority 4 is
3/7 complete, 4/7 remaining; next is 4.4, cumulative deadlines/cancellation and
protected API/management/admission gates. No serving activation is authorized by probes.
The 4.4 cooperative budget slice (Critical / Must have — shared pilot) now adds
src/application/operation_budget.py and runtime/executor checkpoints. One server
monotonic deadline and one-way signals span nested calls; runtime lock acquisition
is cancellable and stopped waiters never invalidate an active owner's keys.
Post-dispatch stops withhold results while preserving original IDs, sealed windows
and read-only recovery; UNKNOWN never becomes rollback proof. All 685 affected
offline tests pass, including 46 new tests. No cloud calls or activation occurred.
The subsequent 4.4 provider/ADC slice now caps cloud RPCs, metadata/access/result
iteration, child-job/receipt reads, pinned secrets and IAP certificates by remaining
time. src/application/operation_transport.py adds explicit opt-in ADC/BQ/regional
Secret Manager REST composition, rechecks at the wire after ADC refresh and disables
automatic 401/redirect replay. Legacy gRPC composition gains RPC caps, not this
HTTP-context guarantee. Sealed native settings/IDs and UNKNOWN/recovery semantics
stay unchanged. Late provider responses cannot establish a validated floor;
already validated higher floors remain retained. All 732 affected offline tests
pass, including 47 new tests in this slice. No live ADC, cloud calls or activation
occurred. See docs/OPERATION_TRANSPORT.md. The subsequent 4.4 host/worker slice adds
sharing_operation_host.py, sharing_operation_worker.py, sharing_host_contract.py
and operation_channel.py. Explicit prestarted POSIX workers inherit deadlines,
reserve TERM/KILL cleanup and retire permanently after uncertain stops without
automatic restart/resubmission. Existing auth/runtime/ADC/executor gates remain;
live worker floors persist across calls and retired hosts require explicit new
authoritative cold-start recovery with original sealed tokens. All 764 affected
offline tests pass, including 32 new host/process tests. No live ADC, cloud calls
or activation occurred. See docs/SHARING_OPERATION_HOST.md. Startup is outside the
request enforcement guarantee; deployed IAP/HTTP/disconnect/teardown remains
unverified. Unwrapped runtime/state/close remain cooperative. The subsequent
4.4 sharing HTTP slice (Critical / Must have — shared pilot) in sharing_api.py
adds disabled-by-default ASGI routes for the seven existing sharing operations,
strict bounded request/result projections, IAP-only header forwarding, exact
HTTPS-origin checks, bounded admission and trusted context/deadline propagation
through an owned supervisor pool. Disconnect/cancellation drains supervisor work;
explicit aclose stops admission and joins admitted requests before host cleanup.
There is no automatic startup/restart, resubmission, role fallback or commit
inference from HTTP. Delivery diagnostics are bounded server-only observations,
not durable access/mutation audits. All 795 affected offline tests pass, including
31 new API tests and seven cases with real local workers. Real ES256 fixtures
verify tampering denial before mocked BigQuery reads; duplicate/lost-response
fixtures preserve the original job. No live ADC/cloud calls, serving changes,
deployment, UI integration or activation occurred. See docs/SHARING_API.md.
The final 4.4 implementation (Critical / Must have — shared pilot) adds
workflow_read_api/host/worker.py, workflow_control_api/host/worker.py,
access_capture.py, workflow_codec.py, control_io.py, management_service.py and
generation_admission.py. Protected read/download bytes and captured audits share
the server-created HTTP trace; bounded IPC verifies size/hash before release.
Management uses fresh IAP/access, exact schema preflight, native before-image
pins, workspace fencing and atomic audits. Explicit bootstrap manifests/global
retry slots, installed synchronization mappings, last-admin and owner/lifecycle
safeguards are enforced. Cold original-job recovery cannot register a write.
Generation guards claim existing PLANNED calls only; private incarnation receipts
and global native slots prevent competing/uncertain entries from dispatching.
Only the first verified fresh claim may invoke an installed owned-worker adapter;
recovery never grants another call. General row creation/results are now implemented
under 4.6; actual model binding remains disabled. Defaults stay disabled.
All 23 isolated native checks passed across four fixture groups, including an
overlapping admin race, duplicate intents/stages, rollback/post-commit classification,
controlled bootstrap retry, cold recovery and one non-billable adapter callback.
Every owned dataset was deleted and absence verified; serving/model writes were
zero. The full regression checkpoint passed 1,631 tests. The subsequent isolated
process-incarnation guard passed 62 affected tests; syntax and dependencies pass.
See docs/WORKFLOW_RUNTIME_GATES.md for exact counts and retained
failed-run evidence. 4.4 is complete within implementation/isolated verification
scope. Priority 4.5 is also complete within implementation/affected-test scope:
fresh authoritative readers and evidence projections now cover product/knowledge,
current POC policy/claims, analytics, ranked campaign text and pinned verified
images; a deterministic complete planning bundle reconciles overlapping claims;
bounded-fresh access/service audit envelopes retain replay context. All 246 affected
tests pass. No live cloud/model calls or writes occurred. Priority 4.6 then added
the durable writers described below.
Priority 4.6 is complete within implementation and isolated native verification
scope. Its first three-file batch adds
sql/create_workflow_start.sql, src/application/workflow_start_query.py and focused
tests. The pure plan validates fresh ACTIVE workspace/CREATOR/conversation rows,
separates the user access audit from service CREATE_REQUEST/CREATE_JOB mutation
audits, fences the workspace and defines atomic APPLIED/exact-REUSED behavior with
retry-key/generation-intent conflicts rejected. The second three-file batch adds
sql/claim_workflow_attempt.sql, src/application/workflow_attempt_query.py and
focused tests. It separately rechecks fresh access and the exact queued parent,
fences the workspace, moves the job to RUNNING, creates attempt one and appends
the first retrieval-stage event with three service mutation audits. Exact complete
replay is accepted; partial or conflicting state is rejected. All 320 affected
tests passed at that point. The third three-file batch adds
sql/append_workflow_evidence.sql, src/application/workflow_evidence_query.py and
focused tests. It accepts only a complete validated 4.5 planning bundle, binds it
to fresh access and the exact RUNNING job/active worker attempt, fences the
workspace and atomically appends all explicit-schema evidence rows plus one
service audit. Exact full replay is accepted; partial, extra or changed snapshots
are rejected without deleting unrelated rows. All 393 affected tests passed at
that point. The fourth three-file batch adds sql/create_model_invocation.sql,
src/application/workflow_invocation_query.py and focused tests. It derives one
revision-one PLANNED planner invocation from the exact request profile, rechecks
fresh access, current RUNNING parents and the complete stored evidence snapshot
plus its append audit, then fences and atomically inserts the invocation and
CREATE_MODEL_INVOCATION audit. Exact replay is accepted; conflicting invocation
IDs or another planner call for the request are rejected. It grants no dispatch;
the existing protected 4.4 admission path retains the separate durable dispatch
claim. All 460 affected tests passed at that point. The fifth three-file batch adds
sql/record_model_invocation_result.sql,
src/application/workflow_invocation_result_query.py and focused tests. It consumes
an exact revision-two DISPATCHED invocation/receipt, records explicit SUCCEEDED,
FAILED, BLOCKED or UNKNOWN state, preserves returned-model/response identity and
stores nullable usage with Decimal-only cost estimates, currency and pricing
version. Result and accounting use separate audits. UNKNOWN atomically moves the
job and attempt to RECOVERY_REQUIRED with two additional audits; late provider
results remain recordable for billing without granting output publication. All
467 affected tests passed at that point. The sixth three-file batch adds
sql/publish_workflow_output.sql, src/application/workflow_output_query.py and
focused tests. It requires exact successful invocation/evidence/event parents and
fresh trusted observations for every embedded GCS generation/hash/size/MIME pin,
then proposes one append-only POC output, seven remaining stage events and terminal
successful attempt/job updates with ten audits in one fenced transaction. Exact
replay is accepted; a different output for the job, partial histories or partial
audits are rejected. All 538 affected tests passed at that point. The seventh
three-file batch adds sql/finalize_workflow_failure.sql,
src/application/workflow_failure_query.py and focused tests. It consumes only an
exact recorded FAILED/BLOCKED invocation and ordered stage prefix ending in the
same outcome/error, then atomically appends remaining events and closes the attempt
and job without an output, with separate audits. It accepts exact replay and rejects
partial histories/audits or any existing output; late recorded failures can close,
but timeouts cannot synthesize failure. All 544 affected tests pass. No cloud/model
calls or writes occurred. The eighth implementation batch adds the disabled-by-default
src/application/workflow_writer_executor.py and focused tests. It accepts all seven
typed plans, preflights the eleven exact schemas, recovers an existing deterministic
job before submission and permits one jobs.insert only after authoritative NotFound.
Lost responses reconcile the same job ID; exact SQL/config/native identity and the
plan-specific result validator gate COMMITTED. Failed scripts use correlated child
COMMIT/ROLLBACK evidence, while unavailable/mismatched/malformed outcomes remain
UNKNOWN without resubmission. All 603 affected offline tests pass. The final opt-in
native probe in src/validation/workflow_writer_integration.py passed six owned-dataset
cases: atomic terminal publication, stable same-job duplicate recovery, stale conflict
rollback, explicit rollback, post-commit failure recovery and discarded-response
original-job recovery. It identified and corrected the cumulative BigQuery script
budget for assertion-heavy writers to a bounded 512 MiB. The fixture dataset was
deleted and absence verified; serving row metadata was unchanged, with zero model
calls and GCS reads. These plans do not perform GCS verification themselves or
activate serving writes. Priority 4.6 completed the sixth of seven fixed Priority 4
milestones; Priority 4.7 completion is recorded below. See
docs/WORKFLOW_START_WRITER.md and docs/WORKFLOW_WRITER_INTEGRATION.md.
Priority 4.7 then added
src/application/workflow_submission_service.py and focused tests. The service is
default-disabled, verifies IAP identity, reads fresh exact workspace/membership/
conversation rows, enforces CREATOR ownership, derives stable workflow identities
from the verified principal and retry key, creates the existing atomic start plan,
submits through the 4.6 executor, and rechecks identity/access before returning a
minimal receipt. Assertion-heavy writes use the bounded 512 MiB cumulative script
budget established by the native probe. Exact plan caching is bounded and only for
same-process retry; it is not authoritative state or a cold-restart fallback. All
84 affected offline tests pass. No cloud/model/GCS call or serving write occurred.
The second three-file batch adds workflow_submission_host.py,
workflow_submission_worker.py and focused host/worker tests. The fixed operation
uses strict bounded request/receipt projections, parent-owned deadlines and
cancellation, a prestarted owned process, server-only ADC composition and permanent
retirement without automatic mutation replay after an uncertain call. A real local
subprocess with signed IAP fixtures is included; all 142 affected tests pass. No ADC,
cloud, GCS or model call occurred. The third three-file batch adds
workflow_submission_api.py, focused API tests and standardizes the worker's internal
failure code for the shared safe-error mapper. Only POST /v1/submissions/submit is
accepted; exact origin, bounded JSON, admission, disconnect cancellation, joined
cleanup and minimal no-store receipts reuse the established ASGI lifecycle. All 187
affected tests pass, including a real signed-IAP local worker disconnect case. No
ADC, cloud, GCS or model call occurred. The fourth two-file batch adds
workflow_history_service.py and focused tests. The reader is
default-disabled and uses one bounded BigQuery transaction for fresh current
workspace/membership/ACTIVE-owner access plus keyset-ordered jobs and exact request
parents. Canonical HMAC cursors pin principal/scope/snapshot/last key and expire;
there is no local history fallback. Minimal reconnect projections omit request,
owner, principal, trace and audit data. All 277 affected history/contract tests
pass, including eight new history tests. No ADC, cloud, GCS or model call occurred.
The fifth three-file batch adds workflow_history_host.py,
workflow_history_worker.py and focused owned-process tests. Only list_jobs crosses
IPC; paging input, cursor shape, minimal fields, timestamps, ordering and response
scope are checked again in the trusted parent. Cursor-key bytes are encoded only in
local server-to-worker configuration. The child lazily composes ADC-backed readers,
never writes or falls back locally, and retires on uncertain timeout without
restart. All 341 affected tests pass, including six new real signed-IAP local-worker
cases. No ADC, cloud, GCS or model call occurred. The sixth two-file batch adds
workflow_history_api.py and focused HTTP tests. Only POST /v1/history/list_jobs is
accepted over a prestarted host. Exact origin, bounded JSON, page/cursor validation,
IAP-only forwarding, minimal ISO timestamp responses, no-store headers, disconnect
cancellation and joined cleanup reuse the established ASGI lifecycle. HTTP cannot
start or replace workers or configure cursor keys. All 385 affected tests pass,
including six new real signed-IAP history-HTTP cases. No ADC, cloud, GCS or model
call occurred. The seventh three-file batch adds src/ui/workflow_session.py,
focused tests and a thin
integration in src/ui/campaign_chat.py. The pure adapter applies validated receipts
and history pages, never maps UNKNOWN/NOT_COMMITTED to queued, rejects ID, scope,
cursor, overlap, ordering and revision conflicts, and emits minimal job rows. The
exact CI_DURABLE_WORKFLOW_UI_ENABLED=true opt-in is disabled by default. If enabled
before authenticated runtime composition, Streamlit now stops before catalog,
interpretation or generation instead of falling through to the local synchronous
path. All 74 affected UI/workflow tests pass, including six new state/UI tests. No
ADC, cloud, GCS or model call occurred. The eighth three-file implementation batch
adds src/ui/workflow_runtime.py, focused tests and Streamlit integration. Trusted
bootstrap may install one runtime using fixed server scope and already-started
submission/history hosts. Durable mode copies only one IAP assertion from the
Streamlit request context, restores and cursor-pages BigQuery history, supports explicit refresh and
routes reviewed generation submissions through the protected host instead of the
local synchronous generator. Scope/policy/cookies cannot cross the bridge;
unavailable or denied operations fail closed, and uncertain receipts never claim
QUEUED. All 82 affected UI/workflow tests pass, including eight new runtime/UI
tests. No runtime was installed, and no ADC, BigQuery, GCS or model call occurred.
The ninth implementation batch connects the already-prestarted protected-read host
to the runtime and chatbot. Fresh status gates output reads; successful summaries
expose only bounded artifact ID/kind/filename/MIME/size/hash descriptors. Deferred
Streamlit downloads perform no GCS read during page rendering and invoke the
existing fresh access/manifest and pinned generation/hash/size/format checks only
when clicked. Runtime checks bind status, output and downloaded bytes to the exact
scope, job, SKU and descriptor. All 177 affected tests pass. No runtime was
installed, and no ADC, BigQuery, GCS or model call occurred. The closing signed-IAP
integration test now verifies owner submission, stable duplicate identity, paged
history, cold reconnect, refresh, deferred protected download, foreign/revoked
denial and changed-manifest denial before byte release. The full affected slice
passes 179 tests. Priority 4.7 and all seven fixed Priority 4 milestones are complete
within their default-disabled implementation and offline/owned-process verification
scope. Continue at Priority 5 (Critical / Must have — shared pilot): versioned
prompt library and core guardrails. See docs/WORKFLOW_SUBMISSION_SERVICE.md and
docs/IMPLEMENTATION_PLAN.md.
Priority 5's first offline contract batch adds src/application/prompt_contract.py
and focused tests. It validates canonical Vertex-authored prompt snapshots, exact
project/location/prompt/version pins, variables, generation and safety settings,
output schemas, separate content/snapshot hashes, and revisioned lifecycle changes
through terminal blocking and controlled retired-version rollback. All 12 focused
tests and the 128-test affected contract slice pass. It performs no Vertex/BigQuery
read or write, authentication, evaluation proof, unique-active fencing or runtime
resolution; P1/P2 remain open. Priority 5's second two-file implementation batch
adds src/application/vertex_prompt_reader.py and focused tests. It requires an
operator-selected numeric prompt/version pair, performs one bounded retry-disabled
exact dataset-version GET, verifies the returned full project/location/prompt/version
name and canonicalizes only supported text, model, variable, generation, tool,
safety and output-schema settings into VertexPromptSnapshot. Invalid/latest IDs are
rejected before transport; discovery, current-version substitution and prompt
mutation are absent. The installed agentplatform 2.4.0 exact dataset-version path
is isolated behind a narrow transport because its public prompts.get does not accept
a version ID. All 12 focused tests and the 140-test affected contract/generation
slice pass; compilation and whitespace checks pass. No live Vertex, BigQuery, GCS,
model or IAM call occurred. P1/P2 remain open until an authored exact version is
read and its validated DRAFT/lifecycle record is authoritatively persisted. The next
fixed Priority 5 batch is the exact BigQuery prompt-registry schema/setup contract;
it must create no prompt rows or activation state. That third three-file batch is
now complete in src/application/prompt_registry_schema.py,
src/ingestion/setup_prompt_registry_tables.py and focused tests. It defines the
unpartitioned full canonical current registry and the append-only daily-partitioned
lifecycle history, requires the existing compatible ci_application dataset and
can only create missing exact-schema tables. All 13 focused tests and the 117-test
affected storage/prompt slice pass; compilation and whitespace checks pass. On
2026-10-12 ADC verified the configured project/dataset in asia-south1, created
ci_application.prompt_registry and prompt_registry_history, and verified both with
zero rows. Repeated --apply created nothing. No prompt/lifecycle row, activation,
existing schema/data, IAM or unrelated resource changed. P2 remains open. The next
fixed Priority 5 batch is the pure registry row codec and registration-plan
validation; it must submit no query and write no row. That fourth two-file offline
batch is now complete in src/application/prompt_registry_codec.py and focused tests.
It encodes every exact destination field and decodes native BigQuery rows by
reconstructing the prompt contracts, recalculating all hashes and replaying a
bounded ordered lifecycle from the exact initial DRAFT through the current head.
It rejects malformed/coerced/extra/missing fields, noncanonical configuration,
identity/hash changes, duplicate/truncated/reordered history and head divergence.
All 12 focused tests and the 138-test affected prompt/storage/codec slice pass;
compilation and whitespace checks pass. No client, cloud call, query, row write,
Vertex import or activation occurred. P2 remains open. The next fixed Priority 5
batch is the parameterized initial-registration SQL/query-plan contract and offline
tests; it must not submit the query or write rows. That fifth three-file offline
batch is now complete in sql/register_prompt.sql,
src/application/prompt_registry_query.py and focused tests. It accepts only the
codec-validated revision-one DRAFT, atomically inserts exact current/history rows,
returns REUSED only for matching immutable registration state, detects duplicate/
partial/conflicting state and contains no update/delete/merge/activation path.
Stable job IDs are derived from exact Vertex identity so changed content for the
same version cannot escape future native-job correlation. Query configuration is
parameterized, cache/session disabled and bounded to 64 MiB, 60-second jobs and
20-second statements in asia-south1. All nine focused tests and the 147-test
affected prompt/storage/codec/query slice pass; compilation and whitespace checks
pass. No client, query submission, BigQuery/Vertex row or resource change occurred.
P2 remains open. The next fixed Priority 5 batch is the default-disabled exact-job
executor and read-only uncertain-outcome recovery with offline SDK fixtures; it
must not be activated or run against serving tables.
That sixth two-file offline batch is now complete in
src/application/prompt_registry_executor.py and focused tests. It preflights the
two physical schemas, looks up the stable native job before submission, permits at
most one retry-disabled insert after authoritative NotFound, and recovers only the
same original job. Exact SQL/configuration mismatch, missing jobs and ambiguous
provider outcomes stay UNKNOWN and never trigger replacement submission. A failed
script needs correlated native child commit/rollback evidence; only a validated
bounded receipt can report APPLIED or REUSED. Eleven focused tests and the 69-test
prompt slice pass; all repository Python files compile and whitespace checks pass.
No ADC client, live query, BigQuery row, Vertex resource, IAM or activation changed.
P2 remains open. The next fixed Priority 5 batch is an isolated native BigQuery
fixture probe of registration apply/reuse/conflict/rollback and discarded-response
recovery in an owned temporary dataset that must be deleted afterward. It must not
read, write or copy serving prompt-registry rows and does not activate the executor.
That seventh two-file native batch is now complete in
src/validation/prompt_registry_integration.py and focused containment tests. Six
native cases passed in asia-south1: atomic apply, stable duplicate recovery,
transactional reuse without new rows, changed-content conflict rollback, injected
rollback without partial rows and discarded-response recovery from the original
job. The owned dataset was deleted and absence verified; serving row counts stayed
unchanged and Vertex calls were zero. The first calibration run stopped before its
cases on a fixture-only count-query byte cap and also verified cleanup; after that
cap was raised to 32 MiB, the full rerun passed. Eight focused containment tests
and the final 77-test prompt slice pass; compilation and whitespace checks pass.
The executor remains disabled and P2 remains open. The next fixed Priority 5 batch
is a two-file fresh bounded BigQuery prompt-registry reader and offline tests for
exact Vertex identity and eligible task/prompt head resolution. It must decode the
complete chain with the existing codec, fail closed on duplicate/partial/conflicting
state, use no local fallback, and perform no write, Vertex call, activation or model
dispatch. Operator registration/diff composition follows that reader foundation.
That eighth two-file offline batch is now complete in
src/application/prompt_registry_reader.py and focused tests. Each request performs
fresh exact-schema preflight and one bounded parameterized transaction. Exact reads
bind the fixed Vertex project/location and numeric prompt/version IDs; eligible
reads require one ACTIVE head for the task/prompt key. Both require exactly one
head, count-matched complete partition-filtered history and full codec replay.
Duplicate/missing heads, partial/conflicting chains, scope or native-job mismatch
and provider failures fail closed without local/cache fallback. Ten focused tests
and the 87-test affected prompt slice pass; compilation and whitespace checks pass.
No cloud call, row write, Vertex call, activation or model dispatch occurred. P2
remains open. The next fixed Priority 5 batch is a two-file read-only operator
import-preview/canonical-diff service and offline tests. It will combine one exact
Vertex read with exact and ACTIVE BigQuery reads, return existing state or a
hash-bound initial DRAFT proposal and bounded diff, and fail closed on ambiguity.
It will not authenticate/authorize the registrar, invoke the registration executor,
write a row, activate a prompt or dispatch a model. Authorized registration command
composition follows that reviewable preview contract.
That ninth two-file offline batch is now complete in
src/application/prompt_import_preview.py and focused tests. It performs one exact
Vertex read and fresh exact/ACTIVE BigQuery reads; only authoritative not-found is
an expected absence. It returns matching existing state or a reconstructed initial
DRAFT proposal, plus an ordered nine-field canonical diff with full byte lengths/
hashes and 512-character review snippets. The preview hash binds selection,
candidate, exact/active heads, proposal and diff; object construction independently
recomputes proposal/diff and rejects omission or substitution. Eleven focused tests
and the 98-test affected prompt slice pass; compilation and whitespace checks pass.
No cloud call, write, auth decision, activation or model dispatch occurred. P2
remains open. The next fixed Priority 5 batch is a two-file authenticated
registration-command service and offline tests. It must require fresh IAP identity
and current BigQuery `MANAGE_MODELS` authorization, bind actor/preview hash, reissue
the exact preview, submit the default-disabled executor at most once and freshly
verify the exact registered state before reporting success. UNKNOWN outcomes,
revocation/state drift and missing post-write state fail closed without replacement
submission. It must not enable the executor, bootstrap membership, activate,
evaluate or promote a prompt, or dispatch a model. Live serving registration stays
a separate explicit operator action.
That tenth two-file offline batch is now complete in
src/application/prompt_registration_command.py and focused tests. It accepts only
a server-held approved UNREGISTERED preview, requires fresh IAP identity plus
current BigQuery MANAGE_MODELS access before and after the single executor call,
binds the actor/hash/proposal, and supports only exact approved-state recovery after
a lost prior response. Policy version and non-regressing membership revisions are
required. Success needs a validated committed APPLIED/REUSED receipt and a fresh
exact BigQuery state identical to the proposal. UNKNOWN/noncommit/missing receipt,
revocation and state/hash/post-read drift fail closed without replacement submission.
Eleven focused tests and the 109-test affected prompt slice pass; compilation and
whitespace checks pass. No cloud call, access mutation, row write, activation or
model dispatch occurred. P2 remains open until a real exact Vertex version is
authored and an authorized live registration is explicitly run. Priority 5 Batch 11
is complete (**High / Must have — shared pilot**) in
src/application/planner_prompt_parity.py, the shared instruction helper in
src/generators/campaign_brief_generator.py and focused tests. It derives the current
planner instruction, three-variable envelope, allowed request domains, effective
model/generation/tool/safety settings and output schema shape from runtime contracts
without a second editable instruction body. Its immutable report hashes full values,
bounds previews to 256 characters, requires review for language differences and
blocks variables/settings/safety/schema/policy behavior drift. Dynamic eligible
enums remain fresh runtime inputs and code-validated constraints. The report never
authorizes registration and performs no Vertex/BigQuery/model call. Ten focused,
97 prompt-named and 113 separately affected generation/workflow tests pass;
compilation and whitespace checks pass. No cloud/resource change occurred.
At the Batch 12 checkpoint, Priority 5 had a fixed total of 16 batches: 12 complete
and four remaining. Current completion after Batch 15 is recorded below.
Batch 12 (High / Must have — shared pilot) is complete as of 2026-10-12. Vertex
prompt 8077808464061005824, exact version 1, was created in asia-south1 with model
gemini-3.5-flash and PASS parity against the unchanged Batch 11 runtime contract.
Independent read-only recovery returned RECOVERED and repeated authoring returned
REUSED, with identical IDs and canonical hashes. The wire adapter now wraps disabled
function calling as toolConfig.functionCallingConfig.mode = NONE, fixing the
INVALID_DATASET_METADATA cause. The exact reader performs one bounded retry-disabled
raw SDK GET, validates the required wrapper before SDK conversion, and normalizes
only the verified project-number alias 1042059373096. Native discovery avoids the
unsupported version-list display_name filter, validates exact parent/unique identities
and locally matches names within a complete 32-version bound. Overflow, partial or
conflicting state and unknown outcomes fail closed without replacement creation.
Original-operation polling uses retry-disabled bounded RPCs, a 300-second default
budget and timeout diagnostics retaining the operation name. This single-operator
authoring command does not provide a distributed concurrent-create lock.
All 279 affected offline tests pass, including 45 focused reader/parity/authoring
tests; repository Python compilation and dependency checks pass. Defaults remain
write-disabled. Live verification created one planner dataset and one version;
recovery and duplicate reuse submitted no mutations. No BigQuery write, activation,
IAM change, approval change or model invocation occurred. See docs/PROMPT_AUTHORING.md
and data/generated/prompt_registry_diagnostics/batch12_completed_20261012/verification.json.
Historical native diagnosis/stack evidence remains in
data/generated/prompt_registry_diagnostics/710af3c224c44b5a8dafdfc9bf73b65f/;
its owned probe and the earlier minimal probe were deleted with absence verified.
The original metadata error exceeded gRPC's header limit and was masked as
ResourceExhausted/429; no token quota conclusion follows from that transport error.
Batch 13 (High / Must have — shared pilot) now has its two-file operator
implementation in src/application/planner_prompt_registration.py and focused tests.
Default read-only inspection checks exact planner parity and authoritative
exact/ACTIVE BigQuery state, and exports the complete canonical review plus diff.
Exports are never read back as approvals, prompts, registry state or recovery data.
Apply requires trusted IAP audience, explicit workspace, real hidden-input
assertions, fresh MANAGE_MODELS access before/after preparation, exact hash
confirmation and the existing authenticated registration/executor/post-read path.
Confirmation time is outside provider budgets; errors expose safe bounded codes.
Same-command recovery retains the server-held preview; no cold-restart approval
store, new retry job, bootstrap or activation is added. All 338 affected tests
pass, including 17 new operator tests; syntax checks pass. Live exact-version
inspection passed and found UNREGISTERED/no ACTIVE planner. A fresh bounded
BigQuery prerequisite query returned zero workspaces, memberships and registry
rows, plus zero history rows with changed_at >= 1970-01-01. IAP audience is absent;
apply was verified to deny before ADC or assertion input. No data/IAM/Vertex
mutation, approval change, activation or model call occurred. See
docs/PROMPT_REGISTRATION.md and the batch13 review/verification exports under
data/generated/prompt_registry_diagnostics/. Batch 13 remains incomplete pending
real protected IAP configuration, explicit authorized serving bootstrap and live
DRAFT registration/current-history/receipt verification. At this checkpoint,
Priority 5 was 12/16 complete; do not infer that this dependency authorizes bootstrap or that
personal ADC grants application roles. Batch 14 persists evaluation evidence/promotion
gates; Batch 15 implements authorized lifecycle promotion, retirement and rollback;
Batch 16 resolves the BigQuery-eligible exact-version prompt at runtime. P1/P2/P3
remain open to their broader registration/lifecycle/runtime scope. Do not add another
Priority 5 batch without an explicitly agreed scope change, weaken required controls
or use latest/local fallback. No serving activation is implied by Batch 12.
On 2026-10-12 the user selected continued offline Batch 14 work while Batch 13's
live registration remains pending. Batch 14's first two-file contract slice is
implemented in src/application/prompt_evaluation_contract.py and focused tests.
Supplied immutable benchmarks/policies/results bind task, cases/input/expected/
evidence hashes, exact REGISTERED snapshot/revision/digest, model/evaluator pins,
invocation IDs and completion/expiry. Coverage includes ambiguity, missing evidence,
pending/rejected claims, styling/synthetic sources, injection, cross-SKU and stale
approvals. Explicit Decimal thresholds use exact rational/integer comparisons;
no approved live targets are invented. Claims/evidence-scope/injection/safe-finish
checks require zero failures. Missing/unknown cases cannot pass; safety failures
remain FAILED even with partial results. Supplied promotion gates check complete
passed/unexpired results, current supplied policy/benchmark, exact history anchor,
retained evaluation ID and evaluation-completion chronology, then pin the current
revision/digest. They neither authenticate, read, run benchmarks, persist, prove
results/commits nor activate a version. The first contract slice passed 358 affected
tests, including 20 new evaluation tests; compilation/whitespace checks passed. No cloud/model call,
serving/resource/IAM mutation or activation occurred. See docs/PROMPT_EVALUATION.md.
Batch 14's remaining implementation is now complete and verified offline, in
successive 2–3 file groups. prompt_evaluation_storage.py defines four explicit
ci_application schemas: scoped current controls, immutable control history,
evaluation reports and per-case observations. Strict codecs recompute canonical
hashes/derived statuses and reject corrupt, partial or rebound versions. The opt-in
setup_prompt_evaluation_tables.py checks all metadata and creates only missing
tables; no live setup occurred. prompt_evaluation_query.py/store.py implement
current IAP ADMIN/membership/expiry checks, workspace-fenced control CAS and
atomic append-only complete report/case transactions with exact control/registry
history pins. Stable logical native jobs are recovered before one submission;
lost responses reconcile the original job. Exact native SQL/config/identity,
validated receipts and correlated failed-script COMMIT/ROLLBACK observations
gate outcomes; UNKNOWN never grants a replacement submission. One bounded fresh
snapshot reads access, controls/history, report/cases and registry/history without
local fallback. The default-disabled prompt_evaluation_service.py requires fresh
IAP/MANAGE_MODELS access before writes and after commit/read before release,
supports retained-original-plan read-only recovery and returns a promotion capture
with workspace/prompt, control revision/digest, policy/benchmark hashes and registry
revision/digest. Batch 15 must revalidate and atomically fence these pins; this
capture is not a write grant or lock. All 401 affected tests and 63 focused Batch
14 tests pass, including 43 new storage/query/store/service tests and signed-IAP
offline fixtures. Compilation, dependency and whitespace checks pass. No ADC,
cloud/model calls, physical table creation, serving writes, IAM changes or
activation occurred. Native SQL/replay/concurrency verification and reviewed live
benchmark/policy/quality measurements remain activation prerequisites; E1/E2/E4
are still open. At the Batch 14 checkpoint, Priority 5 was 13/16 implementation milestones complete within
documented verification scope (1–12 and 14), with 16 fixed batches. Batch 13's live
registration remains pending. Its next milestone was Batch 15 (Critical / Must have), authorized
lifecycle promotion, one-active-version fencing, immutable transitions, retirement
and rollback; then Batch 16 (Critical / Must have), exact eligible runtime version
resolution and invocation pins without fallback. See docs/PROMPT_EVALUATION.md.
Batch 15 is now complete within offline implementation/affected-test scope. The
pure prompt_lifecycle_contract.py defines scoped captures, actor-derived changes
and two shared ci_application control/history schemas. Global selection is per
prompt key across workspaces. Metadata-only setup_prompt_lifecycle_tables.py
checks both tables and creates only missing compatible resources; it was not run
against GCP. prompt_lifecycle_query.py defines explicit-schema atomic lifecycle
writes with current IAP ADMIN/membership/workspace, original expiry, full registry/
control histories and complete ACTIVE-set checks. Promotions fence current passing
evaluation, cases, policy/benchmark and native expiry, retaining the exact original
evaluation-control anchor. Activation atomically retires the old ACTIVE version;
rollback retains original evaluation identity and current quality gates. Explicit
registration, retirement and blocking preserve provenance; BLOCKED cannot restore.
Only lifecycle columns, shared control and workspace fence update; histories append
and unrelated rows remain. prompt_lifecycle_store.py adds bounded fresh reads,
exact native job/config/schema correlation, one insert after NotFound, original-job
recovery and correlated failed-script COMMIT/ROLLBACK observations. Global native
revision slots coordinate competing actors/workspaces. Explicit retries are capped
at three slots and require fresh positive rollback of every exact predecessor;
commands, captured parents, timestamps and original expiry cannot rebind. Known
retry jobs recover; UNKNOWN/committed/mismatched ancestors never grant new insert.
Expired/exhausted or unverifiable chains stop without reset or automatic retry.
The default-disabled prompt_lifecycle_service.py requires signed IAP and fresh
MANAGE_MODELS access, checks every changed version and shared receipt before
release, withholds after revocation and preserves later lifecycle changes during
historical recovery. Retained plans are server-held; no cold intent store or
HTTP/UI activation is added. All 457 affected tests and 56 focused lifecycle tests
pass; compilation, dependency and whitespace/link checks pass. Native SQL/race/
retry verification is unperformed. No GCP ADC/API/model calls, physical table
creation, serving writes, IAM, deployment or activation occurred. At the Batch 15 checkpoint, Priority 5 was
14/16 complete within documented implementation/verification scope (1–12, 14–15),
with 16 fixed batches. Batch 13 live registration and Batch 16 were open. The next
item at that checkpoint was Batch 16 (Critical / Must have), exact BigQuery-eligible Vertex retrieval,
canonical/profile verification and invocation pins without fallback. Existing
physical resources, native verification and reviewed benchmark/policy/quality
gates remain required before activation. See docs/PROMPT_LIFECYCLE.md.
SSO remains under A1–A5/G5/Priority 12: an explicitly allowed personal Gmail
pilot through Cloud Run IAP/external-user OAuth is feasible without a Workspace
subscription; future company Workspace users/groups remain the team target.
Both require verified IAP identity and explicit BigQuery roles; Gmail-to-company
permissions do not transfer by matching email/name. This is planned activation,
not deployment/bootstrap authorization or an ADC fallback.
Real serving bootstrap requires
trusted owner/resource pins and remains an explicit deployment action. Native
fixtures verify local ADC and real owned secret access, not deployed IAP/runtime IAM.
Serving rollout, deployed HTTP/IAP/teardown and trusted UI runtime installation remain
pending. No serving control rows, product data, approvals, IAM or default activation
changed. Only the two approved API prerequisites remain enabled after fixture cleanup.
Test affected flows per change and reserve the full
suite for broad integration/release checkpoints; document exactly what ran.

Priority 5 Batch 16 (Critical / Must have — shared pilot) is complete within offline
implementation scope. prompt_runtime_contract/reader/service/query.py resolve the
unique fresh BigQuery ACTIVE version with complete lifecycle/evaluation histories,
current passing/unexpired policy/benchmark and original report-control anchor.
The default-disabled IAP CREATOR/owner service reads only the selected exact Vertex
version, canonical-verifies its full snapshot/profile/model/settings/output/safety
contract and rereads authoritative access/selection before release. No local/latest
fallback is provided. Immutable scoped prompt/profile/registry/evaluation/policy
pins append to the separate explicit-schema invocation_prompt_bindings table in
the opt-in generation admission dispatch transaction. Complete source pins,
native expiry, shared lifecycle write fencing, invocation claim and its audit are
atomic. Original eleven workflow schemas are unchanged. The metadata-only
setup_prompt_runtime_tables.py was not run live. Registered composition requires
an explicitly installed PromptRuntimeService and require_registered_prompts=True;
fresh exact Vertex/eligibility and stored binding checks gate the existing one-shot
owned-worker callback and its result. UNKNOWN/recovery/provider failures cannot
replay a model. No runtime/adapter was installed or activated. All 530 distinct
affected tests pass, including 37 focused runtime tests and real offline ES256
fixtures; compilation/dependencies/whitespace checks pass. No ADC/cloud/model/GCS,
serving/resource/IAM/deployment changes occurred. Native SQL/race/deployed adapter
verification remains pending. Priority 5 has 16 fixed batches, 15/16 implemented
within documented scope (1–12 and 14–16); Batch 13 live registration is still
pending real IAP and explicit workspace/ADMIN setup. Continue with existing
Batches 14–16 native verification gates in 2–3 file groups before activation;
reviewed quality and the Gmail/Workspace SSO deployment gates remain required.
See docs/PROMPT_RUNTIME.md and docs/IMPLEMENTATION_PLAN.md for exact limits.

On 2026-10-12, the user authorized physical table setup for existing Priority 5
Batches 14–16 (Critical / Must have — shared pilot). All seven ci_application
evaluation/lifecycle/invocation-binding tables were created in asia-south1 using
the existing metadata-only setup modules. Exact schemas/layouts passed; repeated
--apply created nothing and an independently correlated read-only query verified
zero rows in each table, with zero processed/billed bytes. Before/after metadata
for all 46 pre-existing tables/views across five datasets was unchanged; this is
metadata preservation evidence, not a full row-content comparison. All 23 focused
storage/contract/query tests passed. No business rows, products, approvals, IAM,
application roles, prompts, model calls or activation changed. Physical setup is
complete; native transactions/races/recovery, reviewed quality and deployed gates
remain pending. Priority 5 stays 15/16 within documented scope; Batch 13 still
requires real IAP and explicit workspace/ADMIN setup. Continue with the existing
native verification gates in 2–3 file groups. See docs/PROMPT_PHYSICAL_TABLES.md.

Batch 13 was resumed on 2026-10-12 at the user's request. The fresh live exact
Vertex/BigQuery review passed unchanged parity and returned UNREGISTERED/no ACTIVE
planner; all 28 focused operator/registration-command tests passed. IAP audience
remains absent. Cloud Run inventory returned SERVICE_DISABLED for run.googleapis.com,
which does not establish an empty inventory. No browser is available to the
assistant for the one-time OAuth Console setup. The user selected guided personal
Gmail Console setup under the existing SSO/deployment plan. Next: OAuth External
consent, actual protected service and verified audience, explicit controlled
workspace/ADMIN bootstrap, then authenticated exact-version DRAFT registration
and current/history/native-receipt verification. No API enabling, IAM/bootstrap,
registration or activation occurred in the resumption inspection. Batch 13 is
still pending; see docs/PROMPT_REGISTRATION.md. Do not replace IAP verification
with ADC/email identity or manually insert memberships to close this dependency.

The user subsequently confirmed External OAuth audience and the saved custom
Web application IAP callback. On 2026-10-12, run.googleapis.com and
iap.googleapis.com were enabled and independently verified through the enabled
services API. The native asia-south1 Cloud Run inventory succeeded and returned
no services. No service deployment, IAM grants, bootstrap, registration or
activation occurred in this step. Next is Console custom OAuth configuration,
protected-service preparation and real IAP resource/audience/identity checks,
then explicit controlled workspace/ADMIN bootstrap and DRAFT registration.
OAuth branding is in Testing; user-reported client/callback setup does not prove
successful IAP sign-in. Keep credentials out of code/logs/chat. Batch 13 remains
pending; see docs/PROMPT_REGISTRATION.md for the public client ID and progress.

On 2026-10-13, the empty IAP screen required protected-service deployment before
OAuth attachment. The opt-in single-operator console in
src/ui/prompt_registration_console.py and deploy/prompt-registration/app.py now
reuse Batch 13's authenticated exact-version review/register command. Exact HTTPS
origin, signed IAP identity, principal/workspace-bound five-minute server-memory
review, current ADMIN access, exact hash confirmation and explicit write opt-in
gate registration. Uncertain submission retains the same command; no automatic
resubmission, role bootstrap, promotion, generation or durable approval store is
added. Assertions are not displayed/exported or stored in app session state.
All 63 affected tests passed, including 17 console tests, real ES256 tampering and
Streamlit rerun/default-denial cases; syntax/dependencies/source import passed.
Cloud Build and Artifact Registry APIs were enabled. A regional repository,
isolated regional source bucket, dedicated builder and role-free runtime account
were created. Builder grants are repository writer, source-object viewer and log
writer only; the default compute account/product bucket were unchanged. The bucket
has verified uniform access/public prevention. One bounded native build succeeded:
c9397d39-a72f-4d6e-a86b-ad51b4bef21c. Its immutable digest is deployed to Cloud Run
ci-prompt-registrar in asia-south1; Ready revision ci-prompt-registrar-00001-d4k.
Canonical origin is https://ci-prompt-registrar-1042059373096.asia-south1.run.app;
trusted audience is /projects/1042059373096/locations/asia-south1/services/ci-prompt-registrar.
Native metadata verifies IAP enabled, max one instance at service/revision levels,
min zero, concurrency one, 1 CPU/1 GiB, 300-second timeout, only IAP-agent
service-level invocation and credential-free HTTP 302. This is identity-only:
no workspace setting, no runtime project/data roles and registration writes false.
All 53 existing BigQuery table/view metadata and five datasets match the prior
checkpoint. New build/deployment/IAM resources changed, but business rows,
products, approvals, prompt lifecycle and shared workflow activation did not;
model calls, product-bucket operations and service-account keys were zero.
Console custom OAuth attachment, selected Gmail IAP access, real deployed
identity/WebSocket/reconnect verification, explicit controlled ADMIN bootstrap,
scoped runtime registration permissions and live DRAFT receipt/current-history
verification remain pending. Browser access is unavailable to the assistant;
the user enters the secret directly in Console. This console has cooperative
provider budgets and server-memory review, not the Priority 4 hard-worker/durable
runtime guarantee or completed marketing SSO. Priority 5 remains 15/16; no new
batch is added. See docs/PROMPT_REGISTRATION.md, deploy/prompt-registration/README.md
and data/generated/prompt_registry_diagnostics/batch13_console_deployment_20261013.json.

On 2026-10-13 the user reported OAuth attachment. A credential-free canonical
service request verified HTTP 302 to accounts.google.com using the exact custom
client ID 1042059373096-3g5hk97mvb07t1boq82fm7s26lc6qs61.apps.googleusercontent.com.
The service-scoped IAP accessor grant was added and reread; its only explicit
member is user:Prateek.Cholkar@gmail.com with roles/iap.httpsResourceAccessor.
IAP remains enabled, registration writes remain false and no workspace is set.
The grant does not establish application ADMIN access. No OAuth secret was
displayed/stored and no application rows or prompt versions were written.
Next: real browser identity verification, explicit controlled workspace/ADMIN
bootstrap and exact-version DRAFT registration/current-history receipt checks.
Batch 13 remains incomplete; Priority 5 stays 15/16 within documented scope.

The subsequent user browser screenshot verified the initial deployed application
identity check (Google identity verified) and showed bootstrap pending. Its JSON
display failed: native request logs recorded HTTP 429 for Json.D1YSMpgY.js at
2026-10-13T07:41:35.202559Z. That exact asset exists in pinned Streamlit 1.65.0.
With maximum one instance/concurrency one, the long-running Streamlit WebSocket
occupies the only request slot and prevents asset delivery. The configuration-only
fix raised request concurrency to 16; Ready revision ci-prompt-registrar-00002-t7k
serves 100% using the same immutable image. Native checks confirmed concurrency
16, service/revision maximum one, IAP enabled, only IAP-agent invocation, writes
false and no workspace. No code, business rows, application roles, prompt writes
or models changed. Initial identity works; post-fix authenticated rendering and
reconnect remain unverified. Next: refresh/rendering verification, explicit
controlled workspace/ADMIN bootstrap, exact-version DRAFT current/history receipt.
Batch 13 remains incomplete; Priority 5 remains 15/16. See
deploy/prompt-registration/README.md and docs/PROMPT_REGISTRATION.md.

The user subsequently confirmed post-fix identity rendering and selected application
workspace ci-poc-marketing. The displayed subject/principal mapping is consistent;
the bootstrap path reauthenticates signed request headers rather than trusting
the chat values. A three-code-file group adds workspace_bootstrap_console.py,
focused tests and an optional prompt-console integration. Defaults deny bootstrap;
an installed exact-owner ADMIN-only manifest with at most five-minute validity,
disabled registration and explicit workspace confirmation are required. The form
reuses ProtectedManagementService's atomic slot-one bootstrap. A consumed entry
is recovery-only, even after exceptions; original-plan reconstruction cannot
submit or issue a new write. No renewal/retry-slot UI exists. All 102 affected
tests pass, including 10 new bootstrap tests; syntax/dependencies pass. Native
checks verified all eleven workflow tables empty and runtime writes on only
workspaces/memberships/audit_events through a custom updateData-only role.
Runtime has ci_application READER access and project BigQuery jobUser; IAM grants
do not establish application ADMIN membership. Dataset IAM required allowlisting;
the supported etag-protected access-entry update preserved old grants. The unused
reader custom-role definition remains unbound. Native build
09a1da82-9c6e-423c-8345-52dd62d33628 succeeded with digest
sha256:2f9a43c475ded09e8ed92770b40394e365e059a9a016a2dfdd0c20c53e160b72.
Ready revision ci-prompt-registrar-00003-kxz serves 100% with the exact-owner
ADMIN-only server manifest for ci-poc-marketing, expiring 2026-10-13 at 13:45:02 IST.
Native configuration verifies the manifest, IAP, concurrency 16, maximum one
instance and registration writes false; no registration workspace is set.
Next: signed browser confirmation within the manifest window, authoritative
workspace/membership/audit verification, then DRAFT review/registration. No business
rows, products, prompts, models or application roles changed yet. Batch 13 remains
incomplete and Priority 5 remains 15/16. See docs/PROMPT_REGISTRATION.md.

The first bootstrap manifest expired without submission. On the user's disabled
button report, native lookups returned NotFound for all three reserved bootstrap
slots and a fresh bounded query found zero ci-poc-marketing rows across all eleven
workflow tables. Only after this absence verification, an explicit second manifest
was installed with the same workspace, owner, ADMIN-only roles, command and slot-one
job ID. Window two is valid 2026-10-13 at 16:16:47–16:21:47 IST. Ready revision
ci-prompt-registrar-00004-qmg serves 100%; native comparison verifies the exact new
manifest with image/runtime/IAP/capacity/other settings unchanged. Registration
writes remain false. No original mutation was resubmitted or business rows written
by this configuration update. No automatic renewal was added. Next: signed browser
confirmation, authoritative workspace/membership/audit verification, then DRAFT
registration. Batch 13 remains open and Priority 5 remains 15/16. See
docs/PROMPT_REGISTRATION.md and
data/generated/prompt_registry_diagnostics/batch13_bootstrap_window_precheck_20261013.json.

## Source of truth

On 2026-10-13 the real signed browser bootstrap returned COMMITTED/APPLIED.
Independent exact native SQL/config/creator and full-row verification confirms
one ACTIVE ci-poc-marketing workspace, one ACTIVE ADMIN-only BOOTSTRAP membership
and one audit; the other eight workflow tables remain empty in that scope.
Local ADC cannot read the runtime's anonymous query-result dataset. No broader
grant or mutation replay was used; successful exact transaction metadata and
fresh full authoritative rows independently verified the bootstrap. Runtime now
has updateData-only grants on the two registry tables and a custom project role
containing only aiplatform.datasetVersions.get, plus the existing BigQuery jobUser.
Vertex IAM is project-scoped read-only; application code pins the exact version.
An initial role binding failed while role creation was pending; it succeeded once
creation completed. Native readback verified the final exact grants. Revision
ci-prompt-registrar-00006-4c9 serves the unchanged verified image with bootstrap
disabled, ci-poc-marketing configured and initial DRAFT registration enabled.
IAP/runtime/capacity pins are unchanged. Fresh read-only parity is PASS with the
unchanged exact snapshot, UNREGISTERED/no ACTIVE. Signed fresh preview-hash
confirmation and authoritative DRAFT receipt/current-history verification remain
pending; no prompt registration/model call has occurred. Priority 5 stays 15/16.
See docs/PROMPT_REGISTRATION.md and the batch13 diagnostic evidence files.

Batch 13 bootstrap feedback (High / Must have) now explains expired/not-yet-open
manifest windows and consumed UI entries. Minimal validated transaction receipts
and safe errors survive reruns after fresh owner/config checks; they do not grant
current access. Action diagnostics exclude assertions, credentials and exception
bodies. All 105 affected tests pass, including expired-button feedback, error and
UNKNOWN retention, committed receipt reruns and owner-change denial. Compilation
and dependencies pass. Native checks after the silent-click report found no
reserved bootstrap job, workspace, ADMIN membership or bootstrap audit. The cause
of the earlier silent click remains unproven; the displayed window was expired.
No automatic renewal, replay or new write path was added. Build/deployment and
coordinated confirmation evidence are tracked in docs/PROMPT_REGISTRATION.md.
Batch 13 remains open and Priority 5 stays 15/16 until live DRAFT verification.

BigQuery is the authoritative database and vector store for this project.

- Loaded product facts, ingredient evidence, claim approvals, brand memory, campaigns, sales, performance, knowledge chunks, and text/image vectors must be read from BigQuery for retrieval and campaign grounding.
- Current BigQuery eligibility views determine which claims and knowledge may be used.
- Local generated JSON files are exports, audit artifacts, or pipeline recovery checkpoints. They must not be used as a fallback source for serving retrieval or grounding when BigQuery is unavailable.
- Existing BigQuery embedding records take precedence over local embedding checkpoints. Checkpoints may resume unfinished ingestion only after matching current source identity, content hash, model, and dimensions; the resulting records must be loaded and verified in BigQuery before retrieval uses them.
- Config and research files may supply settings or evidence for ingestion. Editing them does not change authoritative state until the corresponding loader completes and validates its BigQuery write.
- GCS remains the original image store; BigQuery stores image metadata, provenance, object references, and vectors.
- Any future external search index must be a derived copy. BigQuery remains authoritative for eligibility and evidence.
- Prompt templates are authored in Vertex AI Studio and stored as immutable versions
  in Vertex AI Prompt Management. An explicit registration/import operation validates
  an operator-selected exact version and publishes its full canonical content/hash plus
  exact Vertex project/location/prompt/version pins in BigQuery. Import never activates
  a version. Repository code owns the contracts, schemas and tests, not the editable
  prompt body; registration review must show the canonical diff from the active version.
  BigQuery remains authoritative for prompt lifecycle, eligibility and promotion.
  Serving must resolve the BigQuery-selected exact Vertex version, verify its hash and
  settings, and fail closed; it must never use Vertex `latest`, Studio state or local
  templates as a serving fallback. Vertex prompt registration does not approve claims,
  replace evidence or authorize a runtime version.

## Existing product tables

ci_product.product_master
ci_product.ingredients
ci_product.product_ingredients
ci_product.product_assets

## Product dataset status

20 skincare/beauty products were curated from BeautyFeeds.

Canonical keys:

CI-SKU-0001 through CI-SKU-0020

Current loaded data:

- Products: 20
- Unique ingredients: 409
- Product ingredient relationships: 764
- Product assets: 20
- Image failures: 0

Product images are stored in GCS under:

products/<canonical_sku>/primary.jpg

Example:

gs://creative-brand-memory-project-661d501a-e709-4276-a26/products/CI-SKU-0001/primary.jpg

## Data provenance

Real product data:
source_system = BEAUTYFEEDS

Generated POC data must use:
source_system = SYNTHETIC_POC

Never present generated sales/campaign data as actual historical business data.

## Current repository structure

src/
  ingestion/
    select_products.py
    validate_products.py
    load_products.py

data/
  raw/
  curated/
  generated/

## Authentication

Local environment uses Google Application Default Credentials.

Do not create or commit service-account JSON keys.

The following should already work:

gcloud config get-value project
bq ls

Python Google Cloud clients should use ADC.

## Product ingestion behavior

load_products.py:

- reads data/curated/selected_20_products.csv
- downloads primary product images
- uploads images to GCS
- loads product_master
- loads ingredients
- loads product_ingredients
- loads product_assets
- uses BigQuery staging tables + MERGE
- must remain idempotent

BigQuery staging tables must use destination table schemas explicitly.
Do not rely on pandas schema inference.

NUMERIC fields must use Python Decimal values.

## Canonical identity rule

canonical_sku is the cross-domain primary business key.

Everything downstream must reference the same key:

CI-SKU-xxxx
  -> product
  -> ingredients
  -> images
  -> claims
  -> sales
  -> campaigns
  -> campaign creatives
  -> campaign performance
  -> RAG chunks

Do not create unrelated or remapped product IDs downstream.

## Target creative model

Product:
  product metadata
  ingredients
  approved claims
  product images

Historical campaign:
  campaign metadata
  campaign copy
  campaign images
  audience
  channel
  performance

Decision data:
  daily SKU sales
  campaign performance
  trend metrics

Brand memory:
  brand guidelines
  approved claims
  product knowledge
  historical campaigns
  creative learnings
  rejected creatives

## RAG principle

Do NOT embed raw daily sales rows.

Use:

RAG:
- product knowledge
- brand rules
- claims
- historical creative descriptions
- campaign learnings

Structured analytics:
- sales
- revenue
- CTR
- conversion
- ROAS
- campaign performance

The orchestrator combines both.

## Multimodal principle

For images retain:

1. Original image in GCS
2. BigQuery metadata
3. Image embedding
4. Generated visual description

Embeddings are for retrieval.

The actual retrieved product/campaign images should also be passed to Gemini for visual reasoning.

## Grounding rules

The LLM must not invent:
- ingredient concentrations
- product benefits
- approved claims
- product characteristics
- historical campaign performance

Claims must come from retrieved approved evidence.

The final campaign should retain evidence references where possible.

## Current implementation plan

Continue from the implemented POC backend using [docs/IMPLEMENTATION_PLAN.md](docs/IMPLEMENTATION_PLAN.md).
The chatbot UI is the active delivery workstream. The plan tracks incomplete output/API contracts,
RAG evaluation/ranking, Google Workspace SSO and application roles, user model selection,
prompt library, observability, Responsible AI guardrails, explainability,
and token/compute cost management in small milestones alongside UI development.
Update milestone status and validation evidence as work is completed; preserve existing data and approvals.
Follow the fixed scope/reporting rules and Priority 4 IDs 4.1–4.7 in that plan.
Report progress against those existing IDs rather than creating new next-step lists.
Implementation subtasks stay within their milestone; proposed scope changes must
be explained and agreed with the user before adding them to the baseline.
Tag every planned/implemented change with Critical, High, Medium or Low, plus
Must have or Good to have and the applicable scenario (shared pilot, production,
or before optional-feature activation). Follow the plan's delivery lanes:
dependency-ready Critical/High Must have work comes first. Keep optional work
under its existing IDs; deferral does not mean completion or permission to bypass
required access, grounding, integrity, recovery or cost controls.

## Business ecosystem milestone sequence

Create the connected business ecosystem for the 20 existing SKUs.

Sequence:

1. Generate 365 days of daily SKU sales
2. Generate ~3 historical campaigns per product
3. Create campaign assets
4. Generate campaign performance
5. Make campaign periods affect synthetic sales
6. Add brand guidelines
7. Create approved product claims
8. Create RAG knowledge chunks
9. Generate embeddings
10. Build multimodal retrieval
11. Generate grounded campaign brief
12. Generate final banner

## Sales requirements

Create:

src/generators/sales_generator.py

Target:

20 SKUs x 365 days = 7,300 rows

Sales must be realistic and deterministic.

Model:

daily_sales =
baseline demand
x weekday effect
x seasonality
x SKU-specific trend
x promotion/campaign effect
x controlled random variation

Use a fixed random seed.

Do not generate unrelated random sales.

Campaign uplift will later need to align with campaign periods.

## Engineering expectations

- Python
- modular design
- strong typing where useful
- reusable modules
- config-driven values
- idempotent data loaders
- deterministic synthetic generation
- structured logging
- unit tests for transformation logic
- Focus tests on impacted files/functionality and direct dependencies. Repeat a
  full suite only when the change scope justifies it or the user requests it.
- no secrets committed
- preserve provenance
- do not overwrite unrelated BigQuery data
- Report verification and detailed, ordered next steps when completing a milestone.
