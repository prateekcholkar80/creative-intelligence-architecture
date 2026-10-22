# Conversational evidence gaps

2026-10-16 · Priority 6 E2 · High / Must have clarification capability;
Critical / Must have evidence and approval boundaries.

## First implementation group

Three files add a pure, opt-in contract:

- `config/clarification/evidence_requirements_v1.json`: versioned DRAFT requirements
  for label directions, warnings, scientific assertions, offers, prices, availability
  and shipping. There are no fixed user-facing question templates.
- `src/application/campaign_evidence_gaps.py`: configurable applicability and
  missing-field planning plus a structured LLM question request/response contract.
- `tests/test_campaign_evidence_gaps.py`: ten offline cases.

The planner accepts interpreted feature flags, not keyword matches. Captures bind
workspace/conversation/request/SKU/country/channel plus exact request and interpreted
intent hashes. Only requirements triggered by those features are evaluated. Exact
available fields suppress repeat questions; configurable missing fields determine
what to ask. A changed destination or request/intent invalidates old coverage.
At most three related evidence questions are selected per turn; remaining gaps are
explicitly deferred. The model may write natural question wording but cannot change
the selected fields, requirement IDs or plan digest through this contract.

A confirmed ABSENT record or partial current approved record leads to CLARIFY.
UNAVAILABLE retrieval leads to RETRY_RETRIEVAL, never an invented empty result or
user workaround. PENDING_REVIEW, REJECTED, STALE and CONFLICT lead to NEEDS_REVIEW
and suppress questions in this initial conservative profile. Submitted answers
are explicitly UNVERIFIED. Approved/current coverage never grants generation,
claim approval, commercial approval or campaign release. Even NO_EVIDENCE_GAPS
means only that this bounded profile found no missing requirements.

## Authority and installation limits

EvidenceCoverage is a supplied server contract. A native job ID or CURRENT_APPROVED
label inside it is not independent BigQuery verification. No authoritative source
adapter, LLM interpretation, scientific/offer eligibility checker or approved
question-prompt runtime is installed in this group. Unsupported interpretation
features, missing captures, scope conflicts and expired captures deny planning.
Semantic correctness/safety of generated question wording is not established by
its structural validator; existing per-interaction quality gates remain necessary.

The local JSON file is a proposed configuration, not a runtime source of truth.
Production must retrieve the exact version from BigQuery and verify its canonical
hash/status. The module does not open this file or use it as a retrieval fallback.
DRAFT configuration supplies offline design evidence only. No new policy/prompt
was registered, approved or activated. BigQuery/GCS/model calls and writes were zero.
Existing workflow catalog defaults, serving behavior and claim permissions stay
unchanged. This does not close any of the 41 unavailable E2 quality checkers.

## Verification and remaining implementation

29 affected offline tests pass (10 new), compilation and dependency checks pass.
No native/cloud or model quality measurements occurred.

Continue within existing E2 in 2–3-file implementation groups:

1. Add authoritative exact-scope coverage adapters that distinguish successful
   empty reads, partial records, pending/rejected approvals and query failure.
   Bind interpreted request features to stored request/intent authority. Then use
   the protected, registered LLM path to generate questions from the pinned plan
   and run applicable question-quality checks before returning them.
2. Capture answers as scoped unverified submissions with provenance and applicable
   retention; validate/review them through separate approval paths. Reread current
   records and recompute gaps after each turn. Do not repeat already submitted
   fields or promote a user's statement to approved wording/offer terms.
3. Integrate the guarded clarification/answer/resume sequence into the chatbot
   through the existing Priority 9/12 installation and serving gates. Remaining
   E2/E4/E5 quality checks, benchmarks and approval gates remain open.

## Authoritative permission coverage and protected question drafts — 2026-10-16

High / Must have clarification; Critical / Must have source, access and approval
boundaries. Subsequent two-file groups implement:

- `campaign_evidence_coverage.py`: one fixed-project/region bounded native-correlated
  snapshot of all six existing permission head/history tables, after owner access
  and exact metadata checks. Complete counts, payload/total size limits, orphan/
  duplicate/history/hash/future-row guards precede decoding. Original native job,
  exact SQL/config and terminal success are checked independently; query failure
  propagates and is never turned into an empty result. Access is checked again.
- `campaign_evidence_intent.py`: `campaign-evidence-intent-v1` projection validates
  exact raw response bytes against one successful durable INTERPRETER invocation,
  STOP/safety/model/profile/schema and selected formula/audience. Supported feature
  enums come from policy configuration; no keywords infer applicability. Legacy
  interpreter schemas fail rather than silently mapping unsupported flags.
- `campaign_evidence_conversation.py`: fresh authority/intent/coverage/authority
  composition pins request, source, policy and interpretation context. The question
  adapter is a server-installed callback for the existing registered one-shot
  admission path. It requires an owned worker, a separate INTERPRETER request with
  the expected question prompt/version/variables/profile, an APPROVED policy marker
  and unchanged fresh source/access context around the transport. A local lock and
  consumed flag prohibit another call on the same adapter, including after failure.
  Question fields and plan hash are validated against the exact pinned plan.

The protected callback returns only a `QuestionDraft`, whose release_allowed is
always false. Structural output checking is not semantic question-quality checking.
No public endpoint or UI wiring was added; durable provider-outcome writing and
per-interaction quality/release gates remain separate obligations. An owned worker
or supplied APPROVED field alone is not IAP or policy-publication proof; compose
this callback through ProtectedGenerationAdmission with real registered runtime
verification. Synthetic positive callback tests use a clearly identified resolved-
prompt fixture; they do not prove a live registered question prompt.

### Coverage supported by existing schemas

Scientific claim permissions include selected formula, approved wording,
qualifications and independently current evidence metadata. A reviewed content
hash/website summary is not a retrievable full study report: study_report_reference
is never marked covered by this adapter. Missing independent reviews conflict.

Offer permissions cover structured terms, eligibility and validity windows. Free-
text qualifications do not establish complete structured exclusions; current
price, inventory and shipping records are not inferred from offer metadata.
Multiple matching records return CONFLICT, with no latest/best-match shortcut.
DRAFT records remain PENDING_REVIEW; revoked records become REJECTED; expired or
not-yet-effective records become STALE. Scope mismatches are not reused.

Existing permission codecs support CI-SKU-0001–0020. CI-SKU-0021–0023 remain
UNAVAILABLE in this adapter; no catalog widening or remapping occurred. Structured
label/directions/warnings, base-price, availability and shipping stores are not
implemented here and return UNAVAILABLE, never ABSENT. A complete empty snapshot
can establish ABSENT only for a supported record kind/scope. The global snapshot
has explicit finite size/history bounds; exceeding them denies rather than hiding
records. It is not a production-scale indexed discovery design.

### Verification and remaining gates

103 focused offline tests pass, including 16 new coverage/intent/conversation cases;
compilation and dependencies pass. Tests cover empty versus failed reads, unsupported
schemas, exact scope, current versus pending/stale/conflicting records, orphan/
partial histories, job-correlation rejection, revocation, changed interpreter bytes,
legacy schemas, prompt mismatch, one-shot behavior and pre/post-call context changes.
No cloud calls/writes, model calls, policy/prompt registration or activation occurred.
The new query/service composition is not live-IAP/native verified in this slice.

The policy is still supplied canonical configuration. No authoritative BigQuery
clarification-policy publication/reader is installed; no runtime may load the local
draft as fallback. The new intent and question prompt versions have not been
registered/approved or installed. Current legacy schemas correctly deny this flow.
Before enabling question generation, finish these existing source/runtime adapters
and applicable question-quality gates. Then capture answers as scoped unverified
submissions, apply separate validation/review, and reread evidence/recompute gaps.
Guarded answer/resume and chatbot installation retain the existing E2/9/12 gates.
No unavailable quality checker was closed and generation/serving stay disabled.

## Exact BigQuery clarification-policy source — 2026-10-16

Critical / Must have source boundary within existing P6 E2. A three-file group adds
`campaign_clarification_policy.py`, `configure_clarification_policy.py` and policy
store tests; a subsequent two-file group connects the conversation and tests.

The fixed-project/region tables in ci_brand_memory are
`campaign_clarification_policy_versions` and
`campaign_clarification_policy_revocations`. Each version stores canonical policy
JSON and its SHA-256. A separate append-only revocation namespace allows readers
to deny a version without rewriting its configuration. This slice defines no
revocation writer, approval publisher or lifecycle promotion route.

The bounded reader requires exact policy ID/version/hash and exact destination
metadata. It independently fetches/correlates the terminal original BigQuery job,
then rejects missing/duplicate/partial/oversize rows, changed canonical content,
future timestamps, revoked versions and expired capture windows. No `latest` lookup
or local/cache fallback exists. No implicit time-to-live is assigned to immutable
configuration; the read capture itself expires after 60 seconds.

`read_bound_gaps` now takes a ClarificationPolicyRequest, not caller-supplied policy
JSON. It reads owner-authorized request authority first, obtains the stored policy,
collects intent/evidence, rereads policy and finishes with fresh request authority.
Both policy records and native read IDs are checked; the record digest joins the
context digest. Policy drift, revocation, request/access drift or expiry withholds
planning/drafts. The question adapter also requires policy_approval_verified.
This reader always returns approval_verified=false: a stored APPROVED string does
not establish a current authorized review. No business approval is inferred.

The explicit CLI can create only the two exact tables and publish DRAFT JSON.
It refuses APPROVED ingestion, has no runtime local-file loading, and uses one
stable native publication slot per policy ID/version regardless of supplied content.
The transaction denies content conflicts/revocation and appends at most one exact
version. Recovery reuses the original job; it never remints an uncertain slot.
The operator uses ADC for DRAFT configuration ingestion, not an application reviewer
role or an authenticated campaign-user grant.

### Actual verification

112 focused offline tests pass (nine new); compilation and dependencies pass.
Tests include exact readback, missing/duplicate/revoked rows, changed bytes/pins,
expiry, native correlation failure, idempotent metadata setup, content-independent
job slots, APPROVED ingestion denial, browser-supplied policy rejection and
unverified approval-marker denial before model transport.

Native setup created both tables, then inserted exactly one DRAFT version.
Reapply created no tables, recovered the identical original COMMITTED job and
independently reread identical policy and record hashes. The second metadata read
has one version and zero revocations. The first setup's zero counts were captured
before publication, not a claim of zero rows after the insert.

Policy SHA-256: `97497414d6aed432b013ed5565ef717b3705198334d2c2e68c3c5c92012620df`.
Record SHA-256: `d05bc59de46387b6eaead3e3e45dd7e3c8e33fed81cd35448d01e3b95ce71b5e`.
See [native IDs, verification and source hashes](../data/generated/campaign_clarification_policy_diagnostics/verification_20261016.json).

No model/Vertex calls, prompt registration, IAM/deployment changes, business
approvals or serving activation occurred. Writes were confined to new configuration
metadata and this one DRAFT policy row; product/business records were not recreated.
Native revocation/race fixtures and combined deployed IAP verification were not run.
Revocation checks are implemented/offline tested, not a verified concurrent writer
ordering guarantee.

### Remaining existing work

The configuration source gap is now closed for exact versioned reads/DRAFT ingestion.
Operational policy review/revocation publication remains separate; approval_verified
cannot become true from this reader. Next prepare/register the new intent/question
prompt versions through existing reviewed registry controls and implement the missing
structured label/price/availability/shipping source adapters. Current legacy schemas
and N&T permission-codec limits remain explicit. Durable unverified answer capture,
separate review/recheck, question-quality gates and chatbot integration remain open.
Serving stays disabled and no unavailable quality checker was closed in this slice.

## Versioned prompt definitions and structured source adapters — 2026-10-16

High / Must have capability, Critical / Must have trust boundaries; existing P6 E2.
Successive 2–3-file groups implement the requested definitions and missing source
adapters. Chatbot installation was explicitly excluded and remains incomplete.

### Intent and question definitions

`campaign_clarification_prompts.py` defines v1 recipes for `campaign-evidence-intent`
and `campaign-evidence-questions`, with output schema versions
`campaign-evidence-intent-v1` and `campaign-evidence-questions-v1`. Feature enums derive
from the exact pinned BigQuery requirements policy. There is no hardcoded question
wording or keyword classifier. Intent context rendering requires the persisted
utterance hash, selected audience/formula and unchanged policy, with single-pass
substitution so inserted braces remain data. The definitions preserve source model/
generation/safety settings when constructing a registration-ready exact snapshot;
target Vertex prompt/version IDs must be explicitly provided. No IDs are invented.
Initial-import parity checks and installed RuntimeBehavior builders cover key,
task, text, variables and schema. These definitions are not runtime local fallbacks.
The new intent projector also requires its exact matching stored prompt binding,
not only a claimed output-schema name.

`clarification_prompt_review` reads the exact BigQuery policy and exports both
canonical definitions and hashes. The actual artifact is
[prompt_definitions_v1_review.json](../data/generated/campaign_clarification_policy_diagnostics/prompt_definitions_v1_review.json),
status **DRAFT_DEFINITIONS_NOT_REGISTERED**. Definition version v1 names the recipe;
Vertex numeric versions are assigned only by actual Vertex creation/import. No
Vertex prompt/version was created, no BigQuery prompt was registered and nothing
was promoted. Existing registry preview/ADMIN/hash-confirmation and evaluation/
activation controls are preserved; live creation/import remains operational work.

### Structured sources

Two exact-schema tables were created in ci_brand_memory:
`campaign_structured_evidence_records` and
`campaign_structured_evidence_revocations`. Both remain empty. Records are immutable
canonical JSON with content hash, source reference/hash, exact SKU/kind/country/
channel/audience/formula scope and validity window. This slice implements reads and
validation, not a user submission/approval/revocation writer.

`campaign_structured_evidence.py` now covers CURRENT_LABEL, CURRENT_PRICE,
CURRENT_FULFILMENT, CURRENT_SHIPPING and OFFER_EXCLUSIONS. Coverage is integrated
into `campaign_evidence_coverage.py`; directions and warnings reuse one scoped
label read. Only the fields actually present and eligible are covered. Price and
currency, availability enums, effective timestamps and formula consistency have
typed checks. Missing fields stay missing. Unknown availability is not coverage.

Before/after owner access, metadata, fixed client/region, exact original native job,
counts/overflow/size/hash/duplicate/scope/revocation/freshness checks protect reads.
Query failure/missing tables never imply absence. A successfully verified empty
query now establishes ABSENT for these represented source kinds. Conflicting
records hold rather than choosing latest/best-match.

UNVERIFIED/PENDING_REVIEW rows hold for review. An APPROVED field alone never
establishes eligibility: a separately current PRODUCT_FACT evidence-review record
must hash-match the complete structured body, selected SKU and source formula.
Current record/review validity bounds shorten capture expiry. Revocation, expired
windows and missing/changed reviews withhold coverage. This is factual source
coverage, not cosmetic/scientific claim permission, legal clearance or a commercial
campaign grant. Original/source file bytes are not verified merely by storing a
source hash. Scientific full-study-report retrieval remains separate.

Exclusions must name the same current legacy offer and exactly preserve its
qualifications; fields from unrelated offers cannot be combined. No new commercial
offer is approved by the fact record. Native source discovery supports all 23
catalog IDs; the legacy scientific/offer and independent review codecs retain their
original-20 scope. An N&T record cannot bypass those review limitations: where a
valid independent review cannot be represented, use stays held. Existing product
records and legacy readers were not recreated, remapped or widened.

### Actual verification and remaining work

143 focused offline tests pass, with compilation/dependencies passing. Tests cover
prompt schemas/parity/hash-matched rendering, binding rejection, positive reviewed
label/price/availability/shipping/linked-exclusion projections, changed hashes,
false approval markers, formula/scope/expiry/revocation conflicts, query failure,
job mismatch, partial/duplicate reads, read reuse and metadata idempotency.

Native verification created the two tables empty and independently correlated five
empty-source queries for CI-SKU-0021; reapply created nothing and metadata counts
remain zero. This operator ADC probe is not deployed IAP verification. Positive
reviewed records, native revocation/races and combined request/model execution were
not exercised live. No evidence/business rows or approvals were seeded; no models,
Vertex creation, prompt registration, IAM/deployment or serving changes occurred.
See [native IDs and source hashes](../data/generated/campaign_structured_evidence_diagnostics/verification_20261016.json).

The requested definitions/adapter implementation is complete in this opt-in scope.
Operational Vertex creation/import and reviewed promotion, policy review authority,
independent N&T review compatibility, full study/source-byte validation, durable
unverified answer capture/review/recheck and applicable question-quality gates
remain before guarded chatbot installation. The existing E2/E4/E5/9/12 gates and
41 unavailable quality checks remain unchanged. No new priority is introduced.

## Vertex authoring and registration operator — 2026-10-16

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

The authoring command defaults to discovery/readback. Explicit apply creates one
initial version only after validated wire metadata; an ambiguous response permits
read-only discovery in that invocation, never a second create. Orphan/duplicate
state denies. Settings come from the exact BigQuery-registered planner snapshot;
policy is freshly read before and after. Both prompts inherit its model, thinking,
safety and output limits, while replacing the task-specific text/schema. These are
stored Vertex resources, not inference calls or runtime eligibility grants.

The registration operator checks fresh policy around exact Vertex/registry parity,
retains a bounded server-owned issued command, requires a real IAP ADMIN and human
preview-hash confirmation, and rereads policy after execution. A policy change after
commit withholds the result rather than claiming rollback. Default CLI inspection
is read-only; its exported diagnostic review cannot authorize registration. The
existing deployed console remains planner-only. Do not paste assertions into chat.

Next, in existing 2–3-file groups:

1. **Critical / Must have:** expose the two exact-key reviews through the protected
   operator route, then perform fresh ADMIN hash-confirmed DRAFT imports and verify
   authoritative BigQuery receipts. DRAFT imports still grant no execution.
2. **Critical / Must have:** complete independently authorized policy review and
   question-quality/runtime eligibility gates before enabling question generation.
3. **High / Must have:** add unverified answer capture, scoped review and fresh
   evidence recheck with the agreed 30-day raw-text retention. Answers never become
   approved evidence through submission alone.
4. **Critical / Must have:** preserve all per-interaction quality, IAP and revocation
   gates when installing the conversational flow under existing E2/9/12 scope.

## Protected console integration — 2026-10-16

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

### Server configuration and release boundary

The existing `CI_PROMPT_REGISTRATION_CONSOLE_ENABLED`, IAP audience, HTTPS origin,
workspace and `CI_PROMPT_REGISTRATION_WRITES_ENABLED` controls are unchanged.
`CI_CLARIFICATION_REGISTRATION_ENABLED=true` opts into the selector; absent/false
preserves the planner-only UI. Set `CI_CLARIFICATION_REGISTRATION_CONFIG` to this
JSON object as a server environment value (not a user field or source-of-truth
export):

```json
{
  "policy_id": "campaign-evidence-clarification",
  "policy_version": "draft-v1",
  "policy_sha256": "97497414d6aed432b013ed5565ef717b3705198334d2c2e68c3c5c92012620df",
  "settings_prompt_id": "8077808464061005824",
  "settings_version_id": "1",
  "targets": {
    "campaign-evidence-intent": {"prompt_id": "5933813566455939072", "version_id": "1"},
    "campaign-evidence-questions": {"prompt_id": "7384254121445949440", "version_id": "1"}
  }
}
```

These are exact lookup pins. BigQuery policy/registered settings and exact Vertex
readback still establish content; environment configuration grants no approval.
Only the two supported clarification keys are accepted. An existing session whose
configuration changes denies use rather than replacing its original command.
Session commands and policy bindings are process-local, not serializable approval
credentials. Tokens and clients are never retained in the review store. A lost
process loses the approval and needs authoritative operator recovery; this slice
does not implement a durable cross-process review store or concurrency proof.

The runtime service needs read access to the clarification-policy tables before
these reviews can work. Actual runtime IAM, package inclusion, deployed IAP and
revocation must be verified during deployment; fixture tests do not establish them.
Keep writes disabled for that verification. For import, prepare each exact review,
inspect its full canonical content, enter its fresh preview hash and submit as the
current ADMIN. DRAFT import is separate from policy approval, evaluation, promotion
and campaign serving. Do not share assertions in chat.

## Package 1 protected chatbot connection — 2026-10-16

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

The trusted bootstrap may supply `evidence_service=ProtectedEvidenceChatService(...)`
to the existing `WorkflowUiRuntime`; no browser installs or replaces it. The runtime
exposes `evidence_readiness(headers)` and server-only `prepare_evidence(...)`. The
latter accepts the observed interpreter response but rebinds it to its durable native
outcome; a browser response/hash is not approval. Returned gaps are private diagnostics.

No live bootstrap supplies these dependencies yet. The default local POC is unchanged;
in durable mode no installed service means disabled input, never local inference.
Registering DRAFT prompts therefore cannot accidentally enable interpretation through
the old UI route. The new readiness service reports current DRAFT policy/lifecycle
holds, but does not claim to implement missing release checks or a full activation
readiness verdict. It must remain a hold until actual runtime and evaluation work
under the fixed MVP packages is complete. No arbitrary stage/schema conversion,
raw-answer approval, model retry or business permission is added by this connection.

## Package 1 admitted intent/question connection — 2026-10-16

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

The `intent_adapter(...)` factory returns a callback for the existing registered
`ProtectedGenerationAdmission` path. Its refresh runs inside the claimed worker,
reads the persisted raw transcript, finds the hash-matched current message and
captures current policy/access. It does not accept local UI history as evidence.
`question_adapter(...)` separately binds the successful parent intent to a missing-
evidence plan and returns an admitted question callback. Parent and question requests
retain different identities as enforced by the existing question adapter.

`EvidenceInvocationDraft.observation` is a provider observation, not a persisted
receipt. Tokens map from provider prompt/candidate/cache/thought fields; absent
counts/model identity remain null and no monetary estimate is generated. Successful
provider STOP/safety/shape checking does not establish semantic acceptance. A changed
post-call source or exception returns no draft and never retries the model. The
claim remains subject to durable reconciliation; this slice supplies no failure/
UNKNOWN publisher and does not pretend a provider failure was free or rolled back.

After an independently successful result publication, `prepare_evidence_from_result`
rereads the exact invocation/binding and rejects missing/DISPATCHED/unknown or
mismatched metadata. It does not manufacture a success row or receipt. Interpreter
publication/recovery must be connected before this can run end to end. Positive
adapter tests use explicitly synthetic approved-policy fixtures; the real policy
reader still withholds approval. No deployment/bootstrap/approval flag was changed.

## Package 1 interpreter publication and worker — 2026-10-16

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

`ProtectedInterpreterOutcomeService` requires explicit enablement, a fixed worker
ID and server-issued plans. Authorization is checked before/after native operations;
SQL rechecks exact source membership and active workspace/conversation before new
mutation or replay. The result transaction updates only the supplied scoped request,
invocation, workspace fence and two audits. It never deletes or modifies planner
jobs/attempts or grants campaign readiness from feature extraction. Returned model
identity/tokens remain the supplied provider observations; validation does not claim
independent provider attestation. Authoritative result binding remains required by
the downstream gap reader.

`InterpreterWorker` requires registered-prompt admission and matching publisher
worker identity. Trusted bootstrap may install one instance using
`install_interpreter_worker`, but this slice has not installed it in a running host
or marketing chatbot. Browser data cannot create a worker or select replacement
publication plans. Failure after dispatch is conservatively observed UNKNOWN only
when authoritative dispatched state can still be captured; host cancellation/deadline
performs no further I/O. Failed capture/publication is not treated as rollback or a
successful durable receipt. Recovery is read-only and needs the original retained
server plan. A lost process needs separately verified reconstruction; this is a
stated remaining limitation, not permission to call the model again.

The script dry run used explicit synthetic fixtures against current schemas and
processed zero reported bytes; it did not run the transaction. Isolated native
atomicity/concurrency/fault/recovery verification remains necessary before live
installation. No per-response question output is releasable through this code;
quality/policy review and evaluation retain their existing holds.

## Native transaction verification and question rendering — 2026-10-16

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

The installation helper validates both global slots before changing either, under
stable lock ordering. It accepts only actual typed runtime/worker objects, prestarted
hosts, identical ADC clients/IAP settings and registered evidence adapters. Conflicts
leave both slots unchanged. It is callable trusted bootstrap code, not an executable
production composition recipe and does not itself construct missing hosts/profiles.
A marketing service bootstrap must invoke it using verified real dependencies; the
registration-console deployment is not that marketing runtime. Do not use fixture
profile hashes or fake host PIDs to claim deployment completion.

Question turns are transient server-held views (maximum 32 scopes, at most 30 seconds
past checking), not a BigQuery fallback or approved evidence. Existing per-read access,
readiness, scoped bindings and fresh recomputed quality enforce delivery. The current
readiness/quality readers withhold release, so no real question text becomes visible.
No raw-answer submission writer or new retention policy is introduced. Parent raw
transcripts retain the agreed 30-day policy. Positive renderer layout tests use
explicit synthetic gate overrides only.
