# Phase 2 architecture review: standard libraries and custom-code reduction

**Future-state update — 2026-10-21:** use
[Future state architecture for Creative Intelligence](FUTURE_STATE_RAG_ARCHITECTURE.md)
for the proposed background preparation, product readiness, campaign retrieval,
durable execution and evaluation boundaries. This document remains the detailed
library replacement map. The immediate MVP target stays **one complete Rejuvenate
campaign using saved product data and uploads**; no framework or managed-service
migration is added to its prerequisites.

The future-state proposal prefers a measured Workflows/Cloud Run worker experiment
for infrastructure orchestration, with one orchestration owner and BigQuery business
authority. LangGraph remains an optional application-routing experiment. Do not
stack competing workflow, queue and graph retry/state machines, or treat framework
checkpoints as source permission or proof of provider completion. Generic library
experiments can remain independent of the service migration once the MVP is delivered.

Review date: 2026-10-20. Status: **planning recorded; adoption candidates require
validation**. Requested by the user: deeply review library replacement opportunities,
retain this work in Phase 2, and continue the MVP as implemented.

Classification: **High / Good to have for Phase 2 maintainability**. Preservation
of authority, privacy, original-job recovery and release boundaries is
**Critical / Must have for every migration**. These classifications do not add
MVP prerequisites or change the fixed six-package queue.

## Decision and scope

Prefer maintained libraries for generic infrastructure where a bounded migration
actually removes responsibilities from our code. Retain explicit domain policy,
source applicability, durable execution evidence and qualification rules. Do not
introduce LangChain and LangGraph as a blanket replacement for the application.

The preferred investigation order is dependency reproducibility, contract
validation, telemetry, API lifecycle, then one LangGraph orchestration experiment.
LangChain provider/retrieval adapters are optional and must beat the existing
Google SDK implementation on maintenance without losing evidence. This is a
direction for Phase 2, not a decision to install all listed packages.

The current MVP continues unchanged, including hybrid retrieval/reranking,
generative rendering, supported formats/markets, measured evaluations and approvals.
The qualifying full pipeline benchmark remains **0/159 at the supplied checkpoint**.
This review ran no pipeline, model, cloud query or deployment; it does not establish
a newer live status. The new campaign and upload status remain governed by their
existing receipts, not by this plan.

## Review method and findings

Static inventory parsed all **338 Python files under `src/`**, totaling **58,230
physical lines**, including comments, blank lines and diagnostic runners:

| Area | Files | Lines |
| --- | ---: | ---: |
| Application | 203 | 35,379 |
| Common | 3 | 119 |
| Generators | 13 | 3,032 |
| Ingestion | 32 | 3,941 |
| Retrieval | 15 | 2,430 |
| UI | 13 | 2,024 |
| Native validation/diagnostics | 58 | 11,305 |
| Root package initializer | 1 | 0 |

Inventory used Python AST/import inspection, exact function-body comparisons,
targeted reads of the modules below, representative tests, requirements, Dockerfiles,
and the controlling project documents. It is a cross-cutting architecture review,
not a line-by-line correctness audit, runtime profile or proof of dead code.
Counts are **not estimates of removable code**. No percentage savings is claimed.

Specific findings:

1. The project already delegates cloud clients, signed-token verification, image
   decoding and PDF parsing to Google SDKs, google-auth, Pillow and pypdf.
   Another wrapper can add maintenance rather than remove it.
2. `workflow_contract.py` is 1,003 lines and implements substantial shape/type
   checking alongside domain invariants. Across the repository, 215 import
   statements reference `dataclasses`; that is an inventory indicator, not 215
   redundant models. Structural and semantic validation need separate treatment.
3. AST comparison found an identical `_fields_expr` function in
   `workflow_output_query.py`, `workflow_failure_query.py`,
   `workflow_invocation_result_query.py`, `workflow_evidence_query.py` and
   `workflow_invocation_query.py`. A second identical `fields_expr` pair occurs in
   `workflow_attempt_query.py` and `workflow_codec.py`. This is demonstrated
   consolidation potential; it does not justify an ORM migration.
4. `SharingApi.__call__` owns raw ASGI body delivery, disconnect observation,
   thread-pool dispatch, responses and shutdown. Several other APIs already
   inherit from it: count the shared implementation once when estimating savings.
5. `operation_channel.py` manually frames bounded JSON over nonblocking sockets;
   `operation_budget.py` implements cooperative deadlines and cancellation.
   The host also supplies process isolation and uncertain-worker retirement.
   Async task cancellation alone cannot replace those guarantees.
6. `ProtectedAttachmentBatches.advance` calls `state` before and after one file.
   `state` verifies every saved entry against its original native job and record.
   Repeating that over a batch creates a growing amount of verification work.
   The UI advances through reruns. This is a code-level latency hypothesis,
   not proof of the dominant deployed bottleneck or a reason to cache authority.
7. `VertexEmbedder.__call__` constructs the image prediction REST request and
   implements backoff, while text embeddings already use `google-genai`.
   This is a narrower SDK-replacement candidate than rewriting all RAG.
8. `requirements.txt` mixes unbounded dependencies, version ranges and an exact
   pypdf pin. Deployment Dockerfiles reuse immutable dependency images; marketing
   also installs that exact pypdf version. Image reproducibility already exists
   at the digest boundary, but rebuilding the dependency set needs a documented
   lock/constraints process. No root `pyproject.toml` or dependency lockfile was
   found in the inspected inventory.

Historical tables in `IMPLEMENTATION_PLAN.md` describe older scope and runtime
states. Its controlling queue and later checkpoints take precedence; this review
does not rewrite historical evidence or treat an old completion claim as live proof.

## Replacement map

All paths below are relative to the repository root. Effort and benefit are
qualitative estimates pending a prototype. “Must have if adopted” is a migration
acceptance rule, not authorization to start during MVP.

### Contract validation — Pydantic and selectively jsonschema

**High / Good to have; Critical compatibility gates. Effort: medium.**

Inspect `src/application/workflow_contract.py`, `prompt_contract.py`,
`campaign_operator_host.py::config_from_wire`,
`campaign_planner_evaluation.py::prepare_planner_evaluation`, and `workflow_codec.py`.

Use strict Pydantic models or `TypeAdapter` for new/versioned external config and
wire shapes. Replace repeated field-set, type, enum and size plumbing where the
model is the single schema definition. Use `jsonschema` selectively for existing
JSON Schema documents that must remain independently versioned; do not maintain
two competing authoritative schemas for the same message.

Retain cross-record ownership, freshness, native-job matching, release decisions
and canonical serialization. Do not replace digest input with `model_dump_json`.
Pydantic strict validation differs between Python and JSON inputs; unknown fields,
bool/int distinctions, dates, finite numbers, Decimal, null versus missing, duplicate
JSON keys, depth and total-byte limits need explicit parity cases.
[Pydantic strict mode](https://docs.pydantic.dev/latest/concepts/strict_mode/),
[jsonschema validators](https://python-jsonschema.readthedocs.io/en/stable/validate/).

First experiment: one operator configuration codec, leaving stored records and
registered prompt schemas unchanged. Exit: the same valid/invalid corpus, public
error mapping and exact legacy serialization; fewer handwritten structural checks.
Do not migrate every dataclass, especially simple internal value objects.

### HTTP boundary and async lifecycle — Starlette, optionally FastAPI, plus AnyIO

**High / Good to have; Critical access/cancellation gates. Effort: medium–high.**

Targets: `src/application/sharing_api.py`, `workflow_control_api.py`,
`operation_budget.py`, `operation_channel.py`, `sharing_operation_host.py`.
Starlette is the first candidate for routing, request/response and lifespan
plumbing. Consider FastAPI only if typed endpoint/OpenAPI needs justify it.
AnyIO task groups and cancellation scopes can replace some manual async joining,
watcher cleanup and deadline propagation. Its effective-deadline and bounded
shielding facilities are relevant to shutdown.
[Starlette lifecycle](https://starlette.dev/lifespan/),
[requests](https://www.starlette.io/requests/),
[AnyIO cancellation](https://anyio.readthedocs.io/en/stable/cancellation.html).

Keep IAP validation, exact origins, allowlisted operations, duplicate-key rejection,
bounded streaming bodies, safe errors, admission limits and worker supervision.
Framework background tasks are not durable jobs. Cancelling a thread await does
not prove the thread or an external write stopped. Keep the process boundary until
equivalent termination/retirement is demonstrated. Prefer standard socket/async
primitives over new IPC dependencies only where the no-pickle protocol survives.

Pilot one existing read-only endpoint through the same protected service. Compare
`tests/test_sharing_api.py`, `test_operation_transport.py` and host/budget tests;
test partial body, disconnect, timeout, blocked SDK, shutdown and late success.
Retire the shared raw ASGI machinery only after its subclasses are migrated.
The deployed Streamlit path must be traced separately; this change does not
automatically improve upload responsiveness.

### Pipeline coordination — LangGraph, independently of LangChain

**High / Good to have; Critical dispatch/recovery gates. Effort: high.**

Targets: coordination portions of
`src/application/campaign_planner_evaluation.py::DurablePlannerEvaluation.execute`,
`campaign_interpretation_evaluation.py`, `interpreter_worker.py`,
`campaign_operator_worker.py` and the future completed MVP correction path.
Potential replacements are stage routing, explicit graph transitions, bounded
branching, progress projection and human-wait coordination.

LangGraph offers checkpointed graph state; completed task results and resumable
execution can simplify orchestration. Side effects still require application
idempotency. Interrupted work can re-enter user code; a graph resume is never
permission to repeat an uncertain provider call.
[Persistence](https://docs.langchain.com/oss/python/langgraph/persistence),
[checkpointers](https://docs.langchain.com/oss/python/langgraph/checkpointers),
[functional execution guidance](https://github.com/langchain-ai/docs/blob/main/src/oss/langgraph/functional-api.mdx).

**Storage decision before adoption:** BigQuery remains authoritative. Begin with
an ephemeral graph reconstructed from existing durable records, with each node
calling the current protected service. This can test routing simplification but
does not replace durable recovery or claim persistent graph checkpoints. If durable
graph checkpoints are necessary, assess a BigQuery-compatible implementation for
atomicity, concurrency, retention, latency and maintenance. The official persistence
pages reviewed describe other backends; they do not establish a supported BigQuery
solution for our contracts. Do not silently add Postgres/Redis/SQLite as another
workflow authority. A custom checkpointer may erase the expected code savings.

Every execution node must preserve: fresh access/source checks → exact durable
reservation → dispatch only when proven newly admitted → exact outcome publication.
Recovery reads the original slot; UNKNOWN stays UNKNOWN. Disable framework-level
model retries and parallel dispatch until explicitly proven safe. Graph thread IDs
come from the authenticated server scope, not user input. Checkpoints carry bounded
references/hashes, not an unrestricted copy of transcripts, images or credentials.
Graph interrupts coordinate waiting; approval still requires the current scoped,
exact-version authority record.

First experiment: route one completed evaluation workflow using recorded outcomes
and a counting fake transport. Then isolated native fixtures; only later a bounded
authorized real run. Reject the migration if graph plus persistence glue exceeds
the removed orchestration or introduces a second state machine.

### Model and embedding adapters — official SDK first; LangChain optional

**Medium / Good to have; Critical provider-evidence gates. Effort: medium.**

Targets: `src/generators/embeddings_generator.py::VertexEmbedder`,
`src/retrieval/campaign_creatives.py::embed_query`,
`src/application/campaign_evaluator_transport.py`, and
`src/common/gemini_generation.py`.

Assess the installed Google `PredictionServiceClient.predict` for replacing the
handwritten image-prediction HTTP envelope. Verify the exact regional endpoint,
multimodal payload, dimension and ADC transport behavior before choosing it.
Retain vector finiteness/dimension, input hash and GCS generation checks.
[Google prediction client](https://docs.cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform_v1.services.prediction_service.PredictionServiceClient).

Keep direct `google-genai` by default. Trial `langchain-google-genai` only if Phase 2
provider interchange or graph integration removes substantial adapter code. Its
current documentation describes a wrapper over `google-genai`; an extra layer alone
is not simplification. Pin Vertex/project/region explicitly, prevent API-key fallback
and match retry, safety, thinking, schema and timeout settings.
[Google model integration](https://docs.langchain.com/oss/python/integrations/chat/google_generative_ai).

Exit: exact request semantics and image bytes, original response text/hash, returned
model ID, safety/finish states and nullable input/output/cache/thinking usage survive.
No automatic output-repair calls or zero-filled unknown usage. Changed prompt or
schema bytes need new versions and evaluation; a serializer change is not approval.

### Retrieval and chunking — selective LangChain adapters

**Medium / Good to have; Critical grounding gates. Effort: medium–high.**

Targets: `src/retrieval/campaign_creatives.py`, `generated_banner_search.py`,
`product_knowledge.py`, `evidence_packet.py`; chunk construction in
`src/generators/knowledge_chunks_generator.py` and extraction below.

A LangChain BigQuery vector adapter is a candidate for generic similarity-query
plumbing, not a replacement for exact authoritative facts or eligibility checks.
The documented integration demonstrates BigQuery vector search; it does not prove
compatibility with this repository's schema or native query evidence.
[BigQuery integration](https://docs.langchain.com/oss/python/integrations/vectorstores/google_bigquery_vector_search).

Compare against the **completed MVP** hybrid/fusion/reranking baseline. Require
same eligible candidates, stable ties, no-match behavior, native job IDs, query
parameters, billing caps, cache policy, score semantics and before/after authority
checks. Keep BigQuery as vector authority; no FAISS, Chroma or external vector store.
Disable automatic table/index creation and embedding writes in read-only paths.
If obtaining this evidence requires bypassing most of the adapter, retain the SDK SQL.

Text splitters may become useful for new corpora; they cannot silently replace
existing provenance-preserving segmentation. Any change to boundaries, overlap or
normalization requires a new chunk/parser version and separate embeddings.

### Upload execution and UI progress — retain SDKs; assess a managed queue

**High / Good to have; Critical identity/idempotency gates. Effort: high.**

Targets: `src/application/campaign_attachment_batch.py::{state,advance}`,
`campaign_attachment_service.py::{put_original,record,submit}`,
`src/ui/campaign_attachments.py::show_attachment_batches`.

Pillow, GCS preconditions/checksums and Streamlit's multi-file control already
provide standard functionality. A LangChain loader will not fix partial batch
completion. First measure per-stage timings, query/RPC counts, bytes and batch-size
scaling. Assess eliminating duplicate work within a single authorized operation
without sharing stale access or claim decisions across operations.

If browser-independent completion remains needed after MVP, assess Cloud Tasks
with its official client as a **service architecture option**, not a drop-in
library. Enqueue only a committed batch reference, never a browser bearer token.
A trusted worker must validate current owner/workspace/source authority through an
explicit server-issued job capability; service identity alone cannot impersonate
the user. Never replay expired IAP assertions as background authorization.

Delivery can repeat and arrive out of order, so the existing immutable per-file
IDs, original native jobs, conflict checks and recovery remain mandatory.
[Cloud Tasks limitations](https://docs.cloud.google.com/tasks/docs/common-pitfalls).
Queue status is not a saved receipt. Resolve the non-atomic BQ-commit/enqueue gap
with a reconciled durable dispatch record before adoption. Test disconnect,
duplicate delivery, revocation, reordered tasks and lost enqueue/commit responses.
Retain 5 MiB per-file limits, allowed mixed types and batch limits. Do not create
queue infrastructure as part of this plan.

### Parsing, rendering and authentication — keep existing libraries

**Low / Good to have for further abstraction; Critical existing boundaries.**

`src/application/campaign_document_extraction.py` already uses pypdf, csv and json;
`campaign_attachment_service.py::inspect_file` uses decoded file validation.
Retain parser process limits, exact version, page/Unicode-character offsets,
segment hashes, original bytes and OCR_REQUIRED behavior. Generic document loaders
are not an improvement unless they preserve all of these. Additional OCR needs a
separate quality/provenance assessment, not implicit inferred text.

`src/generators/banner_preview_generator.py` and layout generators already use
Pillow. Keep product/copy compositing and fidelity rules. No image framework
substitutes for source-grounded generation or final pixel evaluation.

`src/application/identity.py::IapIdentityAdapter.authenticate` already uses
google-auth token verification. Keep `access_policy.py`, current BigQuery membership,
ownership/revocation and exact audience/issuer checks. Replacing them with generic
OAuth middleware or an agent framework's tool permissions would lose application
semantics. No new auth library is proposed.

### Telemetry — OpenTelemetry with existing Cloud Logging

**High / Good to have for Phase 2 standardization; MVP telemetry stays required.
Effort: medium.**

Targets: `src/application/campaign_attachment_service.py::{_save_stage,_measure_save}`,
`sharing_api.py::ApiDeliveryObservation`, `access_capture.py`, operation hosts and
provider/retrieval boundaries. Replace duplicated timer/correlation/export plumbing
with OpenTelemetry spans and metrics, retaining safe structured application events.
It provides standard instrumentation APIs.
[Python instrumentation](https://opentelemetry.io/docs/languages/python/instrumentation/).

Propagate context across threads/processes explicitly. Use bounded-cardinality
metrics; keep per-job IDs in traces/logs as permitted, not metric labels.
Disable raw prompt, image, token and transcript export by default. Check regional
export and retention requirements. Traces can be sampled or lost and never replace
BigQuery audit, provider or evaluator records. LangSmith is optional future tooling,
not required to use LangGraph; external data export needs a separate decision.

Exit: correlated intake→BQ/GCS→model→judge timings with content redaction, no
authority changes, measured overhead, and removal of the replaced timer wrappers.

### Persistence, SQL and retries — consolidate cautiously, retain domain logic

**Medium / Good to have; Critical durable compatibility gates. Effort: medium.**

Keep the Google BigQuery/GCS clients. Consolidate the demonstrated SQL projection
duplicates into the existing codec family only after byte-for-byte SQL/config
comparison; identical semantics with different SQL text may still fail original-job
matching. Preserve legacy builders for existing in-flight plans if bytes differ.
An ORM or SQL transpiler is not warranted by this duplication.

`campaign_evaluator_store.py`, `sharing_jobs.py`, `control_io.py` and
transaction builders preserve native job identity and uncertain outcomes. Keep
their assertions/fences/atomicity logic. Generic database retries or LangGraph
checkpoint success are not commit proof.

For the bounded ingestion embedding backoff, assess `google.api_core.retry.Retry`
before adding Tenacity. Preserve attempt ceilings, error classification and overall
budget; a library's defaults may differ. Do not apply it to uncertain model
dispatch, approval publication or mutation recovery.
[Google retry semantics](https://googleapis.dev/python/google-api-core/latest/retry.html).
Keep `operation_transport.py` protections until the SDK demonstrably supports the
same deadline after ADC refresh and no unexpected 401/redirect replay. Its private
Secret Manager transport hooks deserve compatibility tests, not blind deletion.

### Resource provisioning — Terraform import as an optional service-management step

**Medium / Good to have; Critical IAM/data-preservation gates. Effort: high.**

Targets: repeated create/verify portions of
`src/ingestion/setup_campaign_document_processing.py`,
`setup_campaign_evaluator.py`, `setup_clarification_runtime_access.py`,
plus stable Cloud Run/table/grant configuration.

Terraform could replace procedural desired-resource setup and drift reporting.
Import existing resources first; review a zero-change plan before any managed
change. Keep narrow runtime principals, etags/independent verification and
application record writers. Prefer additive member ownership where appropriate;
authoritative IAM policy/binding resources can remove unrelated access.
[Import workflow](https://developer.hashicorp.com/terraform/language/import),
[Google table IAM resource semantics](https://github.com/hashicorp/terraform-provider-google/blob/main/website/docs/r/bigquery_table_iam.html.markdown).

Adoption requires explicit ownership of state, backend/locking/access/backup,
deletion prevention, schema and partition-expiry parity and no unexpected replace
or destroy. This is optional operational consolidation, not an MVP provisioning
rewrite or a library installation.

### Dependency/build and testing tooling

**High / Good to have for Phase 2 consolidation; Critical migration verification.
Effort: low–medium initially.**

Use one reproducible dependency approach: evaluate `uv` lock/sync against
pip-tools constraints, select one, and retain immutable image deployment.
Separate runtime/dev dependencies; declare directly used packages explicitly.
Reconcile installed image versions with manifests and test regional SDK behavior
before choosing exact versions. `uv` supports locked dependency synchronization.
[Locking and syncing](https://docs.astral.sh/uv/concepts/projects/sync/).
No lockfile or dependency installation is introduced by this review.

Keep the unittest suite and native BigQuery probes. Hypothesis stateful tests can
exercise sequences of reserve/dispatch/timeout/recover/revoke/duplicate operations
against a small explicit reference model.
[Stateful testing](https://hypothesis.readthedocs.io/en/latest/stateful.html).
Use the existing fake services and real signature fixtures; do not replace native
race/rollback proof with mocks. A pytest migration has low benefit by itself;
introduce it only if fixture/parameterization savings are demonstrated.

RAG evaluation frameworks may later help secondary metric computation/reporting.
They cannot replace `campaign_full_quality_checks.py`, stage/source applicability,
the 47-rule coverage contract, durable judge records or qualification approval.
Do not count library scores as new qualifying cases without the existing gates.

## What remains application-owned

Retain these responsibilities regardless of framework:

- BigQuery record/vector authority and GCS byte/generation authority.
- Signed IAP identity, current owner/member/role checks and source revocation.
- Exact registered prompt/policy/model/settings and request/source bindings.
- Clinical/benefit claim scope and brand/market/channel applicability.
- Stable native job IDs, transaction fences, original SQL/config correlation,
  UNKNOWN outcomes, immutable audits and cold recovery without reminting.
- Provider response bytes/model/nullable usage; approved artifact hash and generation.
- Stage-aware quality, required evidence, full measured scoring and exact-version
  release authority. Framework pauses, retries and dashboards grant none of these.
- Thirty-day raw transcript/evaluation retention and bounded content handling.

## Event-driven Phase 2 sequence and acceptance

Start implementation **after the fixed MVP exit evidence is accepted**, unless
the user explicitly reprioritizes it. No dates, countdowns or scheduled migration.
These are execution slices of the Phase 2 architecture backlog, not new MVP packages.

| Slice | Entry and concrete work | Completion evidence / stop condition |
| --- | --- | --- |
| Baseline and decision | Reconcile current code/dependencies/runtime; trace real callers; measure upload and pipeline stages. Pick one candidate and exact version/license/support window. | Before/after comparison contract, dependency inventory, maintenance owner and explicit removal list. Stop if no meaningful responsibility can be removed. |
| Low-risk pilot | Lock reproducible dependencies, then migrate one boundary codec; instrument one operation. Separate 2–3 implementation-file batches. | Same canonical/wire/error behavior; resolved dependency and image verification; no semantic change; measured maintenance/overhead results. |
| API lifecycle pilot | Route one read-only API through Starlette/AnyIO using the existing protected service and host. | Existing access/disconnect/shutdown tests plus real process behavior; no leaked workers or late success. Roll back route if contract differs. |
| Orchestration proof | Run a bounded LangGraph prototype over recorded outcomes; settle BigQuery checkpoint strategy before persistence changes. | Fewer orchestration responsibilities; exact original-slot recovery; zero extra provider calls during replay; no second authority. Reject if custom persistence dominates savings. |
| Controlled adoption | Expand one accepted component at a time; assess provider/vector adapters, queue or Terraform only when their measured need is established. | Focused tests and relevant native/IAP acceptance; exact dependency/version/config evidence and rollback. No global automatic refactor. |
| Removal and closure | Drain or pin old runs, verify all callers and recovery dependencies, then delete superseded code in small batches. | Net code/branch/dependency changes and latency/cost evidence recorded; old IDs/receipts still recover; operator docs updated; obsolete code actually removed. |

Every implementation proposal must name the files/functions to remove, not merely
add an adapter. Count new glue, validators, persistence adapters and maintenance
dependencies against the claimed reduction. Measure changed lines/modules,
control-flow duplication, build size, cold start, p50/p95 latency, BQ RPC/scanned
bytes and provider calls where relevant. Set numeric targets from the measured
baseline and agreed tolerances; do not invent savings or speed promises now.

Migration verification must include:

1. Golden valid/invalid wire and storage fixtures, exact hashes and original
   SQL/config for existing jobs, Decimal/UTC/null and duplicate-key behavior.
2. Native isolated commit/rollback, unrelated-row preservation, duplicate/competing
   requests, lost response and cold original-job recovery; verify fixture cleanup.
3. Fresh revocation/source drift before dispatch and after response; no approval
   inference from checkpoint, cache, UI state or user-uploaded text.
4. Faults after reservation, after provider dispatch and after commit. Recovery
   must not issue another provider call; unavailable responses remain unavailable.
5. Real protected UI disconnect/reconnect, batch progress and permission denial,
   with exact receipts. Synthetic tests alone do not establish IAP behavior.
6. Differential retrieval and artifact evidence; any changed model/prompt/parser/
   retrieval behavior receives the applicable measured evaluation before release.

Shadow comparison uses a captured response or read-only/pure computation; it must
not dispatch a second model call or dual-write mutable authoritative state.
Version the adapter/graph on every durable run. New runs may use the candidate
behind a server flag; old runs retain their original recovery implementation.
Rollback changes routing for new work and preserves all existing records.
Do not delete legacy readers/builders until original-job recovery and retention
obligations are satisfied. Approved old results do not approve a changed pipeline.

## Verification of this planning batch and next MVP work

This deliverable is documentation only. Static inventory and targeted code review
identify candidates; linked primary documentation supports library capabilities.
No new dependency, runtime refactor, model execution, benchmark score, IAM change,
resource creation, prompt approval or release was performed. Local path/link checks
validate the plan's references; no engineering test run is represented as needed
or sufficient for this document-only change.

Resume existing Packages 1/3 from fresh request/upload evidence: verify completed
protected intake and correct SKU/request binding, assemble the protected planner
runtime with only necessary reads, and execute the authorized real stage once.
Continue applicable retrieval/artifact execution, measured scoring and original
record recovery toward qualifying full-pipeline runs. Exact prompt/policy approval
and the remaining six-package exits continue to control release.
