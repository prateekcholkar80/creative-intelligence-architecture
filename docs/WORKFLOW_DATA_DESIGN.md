# Durable campaign workflow records

Updated: 2026-10-11. Contracts/schema setup, protected reads and Priority 4.1–4.6 are complete within their recorded implementation/verification scope. Protected submission, history and UI reconnect remain 4.7. See [current runtime gates](WORKFLOW_RUNTIME_GATES.md) for management, lifecycle, dispatch and deferred activation boundaries; older sections retain milestone-specific historical evidence.

This document defines the persistence contract for the campaign chat. [workflow_schema.py](../src/application/workflow_schema.py) supplies explicit BigQuery fields and SDK metadata; [setup_workflow_tables.py](../src/ingestion/setup_workflow_tables.py) verifies resources using ADC. Eleven tables were created and verified empty. The opt-in 4.4 management/bootstrap and dispatch paths now implement scoped fencing/audits; no serving bootstrap has run. Seven 4.6 typed workflow/evidence/result writer plans and their default-disabled executor are implemented and isolated-native verified. UI persistence remains pending. Local chat stays synchronous and session-only. Company Workspace setup remains deferred.

## Placement and authority

The `ci_application` dataset was created and verified in `asia-south1` within project `project-661d501a-e709-4276-a26` on 2026-10-10. Application membership, ownership, requests, jobs and audit records belong here. Product, sales, creative and brand-memory datasets retain their current roles and data.

BigQuery remains the authoritative database and vector store. GCS holds original product images and generated artifacts; BigQuery pins their object generations and hashes. JSON manifests and local exports are authoring/audit artifacts, with no serving fallback. An unavailable authoritative membership, job or evidence record must stop the protected operation.

Generated briefs and previews retain `source_system=SYNTHETIC_POC`, `approval_scope=SYNTHETIC_POC_ONLY`, initial review status `NOT_REVIEWED` and `real_business_approved=false`. Memberships and operational events describe actual application activity; do not label real role assignments or token usage as simulated business data. Preserve the original provenance of every evidence source, including BEAUTYFEEDS product facts and synthetic performance.

## Record boundaries

All records require `schema_version=campaign-workflow-v1` and `workspace_id`. The Python definitions are the exact field/type/mode contract; this table describes each record's responsibility. Keys are logical invariants enforced by loaders, not activated database constraints.

| Table | Logical key within workspace | Responsibility | Storage layout |
| --- | --- | --- | --- |
| workspaces | workspace_id | Active workspace and revision used to serialize mutations | Unpartitioned |
| memberships | principal_id | Stable verified identity, additive roles, ACTIVE/SUSPENDED/REVOKED status, revision and assignment origin | Unpartitioned |
| conversations | conversation_id | Immutable owner; current PRIVATE/TEAM visibility, ACTIVE/ARCHIVED status and access revision | Unpartitioned |
| requests | request_id | Submitted preference snapshot, interpretation outcome, scoped retry identity, profile snapshots and request revision | Daily created_at |
| jobs | job_id | Current generation state, deadline, attempt count, active attempt and successful output reference | Unpartitioned |
| job_attempts | attempt_id | Each execution/recovery attempt and its state, worker and dispatch correlation | Daily started_at |
| stage_events | event_id | Append-only ordered stage outcomes and safe errors | Daily occurred_at |
| outputs | output_id | Immutable verified brief, model settings, POC provenance, evidence snapshot and GCS artifact references | Daily created_at |
| evidence_snapshots | evidence_snapshot_id + evidence_ref_id | Append-only source/review/vector/image identity and integrity evidence | Daily created_at |
| model_invocations | invocation_id | Per-call dispatch, resolved settings, returned model, response validation and usage, including clarification calls | Daily started_at |
| audit_events | audit_id | Append-only authorization and mutation outcomes with policy and revisions | Daily occurred_at |

Every table clusters by workspace first, then the IDs used in scoped lookup. Current membership, conversation access and jobs are unpartitioned so access/status checks need no guessed date range. History tables require a partition predicate and bounded query bytes. Use the current job's request/attempt timestamps to find its related partitions; conversation history uses explicit bounded date pages. Partition boundaries and expiration are based on UTC. [BigQuery partition management](https://docs.cloud.google.com/bigquery/docs/managing-partitioned-tables).

Inspect schema definitions locally without credentials or cloud calls:

```sh
.venv/bin/python -m src.application.workflow_schema
```

The offline manifest's DESIGN_ONLY status describes that local definition artifact, not live deployment state. Use the ADC setup command to check actual cloud resources:

```sh
.venv/bin/python -m src.ingestion.setup_workflow_tables --check
.venv/bin/python -m src.ingestion.setup_workflow_tables --apply
```

`--check` is read-only and reports missing/verified resources and metadata row counts. `--apply` checks every existing target before creating any missing dataset/table. Both reject wrong project/region, default dataset expiration, mismatched recursive schema/modes/order, table type, constraints, partition layout/filter and clustering. Schema type aliases and matching duplicate partition-filter metadata returned by the service are normalized; conflicting settings still fail. Compatible concurrent creation is re-read and verified. Interrupted setup can resume without replacing anything; it makes no row writes, schema alterations, deletes, IAM changes or role assignments.

No foreign/primary constraints or automatic expiration are activated. Future staging loads must explicitly copy the destination schema; never infer it from pandas. NUMERIC estimates require Decimal. The generic existing upsert helper is insufficient for revision guards, nested schema comparison and immutable workflow fields; introduce a dedicated workflow repository.

Live verification on 2026-10-10: CLI project/dataset checks succeeded, and Python ADC metadata reads succeeded. Setup created the dataset and all eleven schema-explicit tables (four before the service metadata normalization fix, seven when resumed). Full post-setup verification found no missing tables, correct schema/layout/region and zero metadata rows in each table. A subsequent `--apply` returned VERIFIED, `created_dataset=false` and `created_tables=[]`. This verifies resource setup idempotency, not data-loader idempotency or concurrency fencing. Existing business datasets were outside the setup command's scope. Sixteen setup unit tests and the full 367-test suite pass, including the partition-metadata regression.

## Identity and ownership links

Principal identity comes from the implemented signed IAP adapter; memberships map that principal into a workspace. The protected metadata gateway invokes the adapter per call and resolves current parent-conversation access. No fake personal-account identity, company domain or automatic role assignment is introduced. Shared mode requires verified identity. A future explicit local development mode must have a separate namespace and never become a shared-mode fallback.

Requests, jobs, attempts, stages, outputs, evidence and model calls reference `conversation_id` and `owner_id`. A writer must enforce that each owner/workspace pair agrees with the conversation and that every child references the correct parent. Current visibility lives only on the conversation. Reads join it and current membership, then invoke the [access policy](ACCESS_OWNERSHIP_DESIGN.md). Team sharing allows role-scoped reading/review; it does not transfer ownership or permit peers to refine a draft.

The current session ID may be retained as optional correlation, but cannot authorize anything. Resource IDs, hashes and GCS paths cannot authorize access either. Artifact download first checks the current conversation scope, then the pinned object generation/hash. Audit events are restricted to authorized operational readers, not exposed by ordinary team sharing.

Canonical SKU stays CI-SKU-0001 through CI-SKU-0020. A clarification request may lack a SKU; a READY generation/refinement request, its job, output and product-scoped evidence must have the validated existing canonical SKU. Global brand-rule evidence can still be linked to the SKU for which it was retrieved; do not remap its actual source record identity.

## Request and job lifecycle

Operations are INTERPRET, GENERATE and REFINE. Request states are ACCEPTED, READY, NEEDS_CLARIFICATION, BLOCKED and FAILED. Before interpretation, persist bounded submitted preferences, message hash, pinned interpreter settings and an ACCEPTED request. Raw chat text is not stored in this initial contract; it stays in process for the call. Save the resolved preferences/unsupported-feature classification as a revisioned outcome. If the process loses the text, recovery asks the user to resubmit rather than inventing or replaying content from a hash. Clarification calls have request/trace identity but no generation job.

The `submitted_preferences` JSON field has an explicit submission envelope: `preferences` contains the five canonical preference fields, and `renderer` contains a server-pinned version/settings SHA-256 for GENERATE, or null for INTERPRET. This pins renderer inputs without changing the deployed table schema. Missing interpretation preferences use null, not empty strings; READY generation requires all five values and a normalized audience of at most 160 characters. The current UI's strings will need adaptation when persistence is connected.

Submitted preferences, message hash, principal, owner, operation, retry hash, input hash, profile snapshots and creation time are immutable. Request revision, outcome and resolved preference fields can change through the validated interpretation outcome. Generation is a new submitted READY request after the user reviews complete preferences; existing requests are not repurposed. REFINE and its parent-output link are reserved and rejected by the current validators until that workflow is implemented.

| Job state | Permitted next states | Meaning |
| --- | --- | --- |
| QUEUED | RUNNING, BLOCKED, FAILED | Accepted generation intent awaiting authorized admission |
| RUNNING | SUCCEEDED, BLOCKED, FAILED, RECOVERY_REQUIRED | Claimed attempt with bounded deadline |
| RECOVERY_REQUIRED | SUCCEEDED, BLOCKED, FAILED, QUEUED | Reconcile durable results; QUEUED only if no billable dispatch occurred |
| SUCCEEDED / BLOCKED / FAILED | None | Terminal record; explicit new intent creates a new job |

Attempt states are RUNNING, SUCCEEDED, BLOCKED, FAILED and RECOVERY_REQUIRED. Every transition compares the expected revision and attempt identity, preserves owner and input identity, and emits an audit/stage event. Successful jobs require exactly one committed output from the winning attempt, completed time and no error; BLOCKED/FAILED jobs require a safe error and no output. RECOVERY_REQUIRED exposes uncertainty instead of reporting success or restarting model work.

Existing retrieval/planning/rendering/verification stage names are retained. Durable event outcomes are STARTED, SUCCEEDED, BLOCKED and FAILED; sequence is per attempt and repeated event IDs must match their original content. Events describe progress; the current job row is authoritative status. Guardrails and current evidence/image/review checks must pass before committing success. POC creative review remains a separate later workflow and cannot be synthesized by a successful job.

## Idempotency and concurrency

Define a scoped retry identity as SHA-256 over canonical JSON containing the contract version, verified principal, workspace, conversation, operation and bounded client retry key. Define an independent request hash over the accepted input: submitted preferences, message hash if present, parent output, generation intent, resolved per-stage settings/prompt/schema/policy versions and renderer version. Use UTF-8, sorted keys, explicit nulls and no floats with nonfinite values. Exclude timestamps, random request/job/trace IDs and usage. A duplicate retry identity with the same input returns its original request/job; different input returns IDEMPOTENCY_CONFLICT before billable work.

A server-issued `generation_intent_id` represents one reviewed submission. Reuse it through double-clicks, reruns and reconnects; it must resolve to one job even if a client sends a different retry key. Generate again deliberately creates a new intent and retry key. Changing model/thinking settings or a parent output changes the input hash; it needs a new intent, not an implicit retry. Ordinary transport retries reuse the pinned evidence/settings where still eligible; an explicit new generation performs fresh retrieval.

**Proposed POC mutation coordinator: a BigQuery transaction that updates a pre-existing workspace control row.** Every workflow writer first verifies exactly one control row, expected revision and ACTIVE status, then updates that row and asserts exactly one row changed. It checks current membership/conversation and existing request/intent/job in the same transaction before inserting or conditionally updating records. Commit related request/job/state/audit changes atomically. All writers, including sharing and membership management, must participate; reject duplicate control rows. Controlled one-time bootstrap remains a separate operator operation, not automatic request handling.

This design deliberately forces conflicting mutations through an existing row rather than assuming concurrent insert/MERGE protects uniqueness. BigQuery documents transaction conflicts for UPDATE/DELETE, while append operations can run concurrently. Retry a cancelled metadata transaction only after fresh authoritative reads; never wrap an external model call in that retry. BigQuery does not enforce primary/foreign keys. Table-level contention is acceptable for this small POC; measure it before scaling the coordination design. [BigQuery transactions](https://docs.cloud.google.com/bigquery/docs/transactions), [key constraints](https://docs.cloud.google.com/bigquery/docs/primary-foreign-keys).

Before each billable call, atomically claim one PLANNED invocation as DISPATCHED with worker identity, revision and a unique dispatch receipt ID. The receipt is the committed BigQuery record, not a second database or a local lock file. Only the process that won that claim proceeds. A second worker must not execute a DISPATCHED invocation. Persist model completion as SUCCEEDED/FAILED/BLOCKED, or UNKNOWN on uncertain outcomes; usage may remain unknown. Check authorization again before dispatch and before committing/publishing results.

There is no atomic transaction spanning BigQuery and Vertex. A crash after dispatch is recorded but before/during the external call cannot prove whether it ran. Recovery must not automatically replay that invocation. Reconcile any persisted result/artifact; otherwise leave RECOVERY_REQUIRED and close with a safe outcome after investigation. An authorized explicit new intent can produce another generation and may incur another charge. Claim records protect admission; they do not establish exactly-once execution of the external API. Worker deadlines and disabled automatic model-call replay are required. Generation's internal embedding calls need the later C1 per-call accounting and retry controls too.

For GCS publication, use unique output/artifact paths, pinned generations, integrity hashes and create-if-absent preconditions. Upload artifacts first; transactionally commit outputs/evidence and job success after verifying them and rechecking access. Unreferenced uploads remain nonservable and are cleaned only by scoped recovery/retention rules. A crash after success commit should return the existing output on retry. [GCS request preconditions](https://docs.cloud.google.com/storage/docs/request-preconditions).

Pure request/job/attempt/stage-event/model-invocation/output/artifact validators are implemented separately from the schema module. Evidence row/bundle validators are implemented in [evidence_contract.py](../src/application/evidence_contract.py). Audit row/replay validators are implemented in [audit_contract.py](../src/application/audit_contract.py). Control contracts are implemented in [workspace_contract.py](../src/application/workspace_contract.py), [membership_contract.py](../src/application/membership_contract.py) and [conversation_contract.py](../src/application/conversation_contract.py). The offline sharing coordinator contract is implemented in [sharing_coordinator_contract.py](../src/application/sharing_coordinator_contract.py). Sharing transaction SQL/query contracts and a disabled-by-default trusted executor with sealed intents are implemented and tested offline. Isolated native SQL/SDK/commit and transaction-race verification is complete; deployed host cancellation and two-worker process-crash tests remain pending. See [native evidence](SHARING_INTEGRATION.md). These deployment checks are mandatory before a durable writer is connected to shared generation. Archive write authorization/auditing remains disabled pending an explicit lifecycle action.

## Implemented pure contracts

[workflow_contract.py](../src/application/workflow_contract.py) validates complete request/job/attempt/stage-event/model-invocation/output rows and nested artifact metadata against the deployed explicit field sets, including nullability, profiles, exact integers/booleans/Decimal NUMERIC values, finite numbers and timezone-aware timestamps. It checks the supplied conversation scope and same-owner request identity. This validates consistency of trusted server records; it does not authenticate a caller, resolve membership or check a live product/model catalog.

The implemented functions are:

| Function | Behavior |
| --- | --- |
| validate_profile | Checks stage, regional scope, positive limits and LEVEL/BUDGET/NONE consistency. Structural validation does not enable a model; published capability/role policy is still M2. |
| request_fingerprint | Hashes accepted submission and pinned profile/policy/renderer inputs. Profile ordering is normalized. Runtime IDs, timestamps, revisions and interpretation outcomes are excluded. |
| retry_key_hash | Computes identity from workspace, principal, conversation, operation and bounded retry key. The service must construct it from verified scope and client submission rather than accept a browser-provided digest. |
| validate_request | Checks immutable input digest, request outcome, canonical preferences, unsupported features and conversation ownership. |
| transition_request | Proposes a revision-checked copy finalizing ACCEPTED interpretation as READY, NEEDS_CLARIFICATION, BLOCKED or FAILED without changing accepted input/hash. |
| resolve_retry | Returns NEW/REUSE after a scoped authoritative lookup; conflicts and expired submissions are errors. Same generation intent deduplicates a changed transport retry key. |
| validate_job | Checks parent request/scope/SKU/input identity, counters, time order and active/terminal-state invariants. |
| transition_job | Proposes a copy under expected revision/attempt checks. Claiming requires a live deadline. Recovery can requeue only with typed NOT_DISPATCHED status; DISPATCHED/UNKNOWN cannot replay. |
| validate_attempt | Checks explicit attempt schema, request/job/conversation links, worker/receipt IDs, numbering, timestamps and current versus closed historical lifecycle. |
| validate_stage_event | Checks one event's scope, attempt/trace/policy links, stage/outcome vocabulary, timestamps, duration and safe-error shape. Does not prove sequence completeness. |
| validate_stage_events | Checks a complete sequence-ordered prefix, unique IDs, consecutive numbering, stage pairing/order and recorded durations; success requires all four completed stages. |
| append_stage_event | Proposes an append or identical-ID replay on a copied history; changed content under an existing ID conflicts. Does not write or enforce concurrent uniqueness. |
| validate_model_invocation | Checks interpretation/planner parent scope, pinned profile, bounded single-call policy, dispatch/outcome metadata, nullable usage and exact Decimal estimate fields. |
| validate_model_invocation_change | Checks previous/proposed rows, supplied prior parent snapshots, expected revision, immutable identity/claim, non-replay state changes and later accounting updates. |
| artifact_gcs_uri | Builds the reserved workflow path from validated workspace/conversation/SKU/output/artifact identifiers and a supported kind. |
| validate_artifact | Checks scoped URI, kind/MIME agreement, bounded positive generation, SHA-256 and positive bounded byte count. Metadata validation does not verify GCS objects. |
| validate_output | Validates request/job/conversation links, successful or proposed-success job state, winning attempt, POC provenance, pinned profiles/renderer, versioned brief/guardrail metadata and artifact set. |

Contract errors expose a safe code without row content. Helpers preserve their input records; they do not persist changes, create cloud clients or provide a lock. The future writer must obtain dispatch status from all authoritative invocation/attempt records, verify output/evidence/artifact integrity before success, and commit state plus audit events atomically. Passing a caller-supplied NOT_DISPATCHED enum or output ID is not proof of those facts. A stale/missing/unknown reader result must fail closed.

The current retry window defaults to seven days, including the boundary, and can be passed explicitly by the future server policy. Both newly proposed and existing submissions must be within it; a new transport timestamp cannot renew an old intent. The request hash covers full effective profile fields and their settings hash, but catalog publication/compatibility and prompt/safety resolution still require authoritative readers. Protected downloads invoke request/job/output validation. Attempt/event contracts are not connected to service writers or the local chat yet.

The [37 contract tests](../tests/test_workflow_contract.py) cover all 20 canonical SKUs, changed generation inputs, dictionary/profile order, clarification/blocked outcomes, conflicting/expired retries, duplicate intents, ownership/parent mismatch, malformed settings, stale revisions, terminal-state restrictions and safe recovery. At the validator milestone, the full suite passed 404 tests on 2026-10-10; the subsequent identity/access-reader foundation brings it to 441. These fixture users/profiles are never published. No cloud schema or rows changed in either milestone.

## Implemented attempt and stage-event contracts

`validate_attempt` consumes complete attempt/request/job rows and the supplied conversation scope. Attempts inherit the canonical SKU through the validated job/request rather than adding a new product key. Each attempt needs a worker ID, positive INT64 revision/number and aware, ordered timestamps; claiming must precede the job deadline. Dispatch receipt IDs are optional except for successful generation attempts. RUNNING has no error/completion; RECOVERY_REQUIRED has a safe error and no completion; terminal attempts have completion equal to update time, with errors required for BLOCKED/FAILED.

The current attempt must match the job's active ID, latest number, state, safe error and completion. Closed earlier attempts must be FAILED with no declared receipt, a number below the active attempt (or at most the count when no attempt is active), and completion no later than the parent's latest lifecycle update. This supports closing an undispatched recovery attempt before requeue/claim. Unknown, running or previously dispatched attempts cannot become historical retry candidates. Receipt absence is not proof of nondispatch; future coordinated writers must inspect every authoritative model invocation before requeue and enforce immutable attempt/worker identity, number uniqueness and nonoverlap. The validator supplies no lock or dispatch permission.

`CAMPAIGN_STAGES` is shared with the existing chat adapter, retaining RETRIEVING_EVIDENCE, PLANNING, RENDERING and VERIFYING. Durable event validation does not change the local adapter's existing stage-start envelope. One row checks exact schema/scope/attempt/trace/policy, positive sequence, bounded ID and occurrence within its attempt period. STARTED has null duration/error; completed outcomes have nonnegative integer duration, with safe errors required only for BLOCKED/FAILED.

`validate_stage_events` requires the whole sequence-ordered prefix, not a paginated fragment. The prefix has at most eight rows: each stage starts once and completes once before the next starts. IDs are unique, sequences start at one and are contiguous, occurrence times never decrease, and duration equals the integer millisecond floor of completion minus recorded start. A failed/blocked stage stops progress; closed failed/blocked attempts must agree with a recorded stage failure's outcome/error when present. Empty or unfinished prefixes remain valid for interrupted attempts, including recovery and failures outside a stage. A successful attempt requires all four start/success pairs; progress alone is not authoritative job success, guardrail proof or POC approval.

`append_stage_event` returns a deep copy of the proposed full history. An identical existing event ID/content is a replay; changed content conflicts, and new IDs must occupy the next sequence. It validates the resulting prefix, allowing the final verification completion to be checked with proposed successful parent rows in one future transaction. This is pure idempotency logic; global event-ID/sequence uniqueness and transaction fencing remain pending.

Verification on 2026-10-10: [40 execution-contract tests](../tests/test_execution_contract.py) cover every attempt state, all 20 canonical SKUs, parent/owner/policy mismatch, counters, timestamps, receipt/worker requirements, recovery history, stage order/pairing/durations, interrupted and successful prefixes, duplicate/conflicting retries, input immutability and no cloud client creation. At that execution-contract milestone the full suite passed 568 tests. That slice made no cloud calls, schema/data writes, model invocations or UI persistence changes.

## Implemented model-invocation contracts

`validate_model_invocation` checks the full deployed `model_invocations` schema without cloud calls or input mutation. INTERPRETER calls belong to INTERPRET requests and have null job/attempt links; pending interpretation requires an ACCEPTED request. PLANNER calls require complete, validated GENERATE request/job/attempt parents and their canonical product identity. The call's profile must exactly match its stage's request snapshot, including model, region, thinking, prompt/schema/policy, limits and settings hash. Returned model version is recorded independently when available, never invented or silently equated with the requested model.

Call IDs and revisions are bounded/positive. The current workflow permits call_number=1 only: interpretation resubmission and deliberate generation use new requests/intents. Global uniqueness of invocation IDs and one call per stage still requires coordinated writers. Receipt-bearing planner calls must match their attempt's receipt and worker. A missing receipt cannot coexist with observed response or usage. Call start must fall within its parent period and planner start must precede the job deadline; actual dispatch admission still needs a fresh clock, access and budget check.

| Invocation state | Record requirements |
| --- | --- |
| PLANNED | No receipt, worker, result, error, completion, usage or estimate |
| DISPATCHED | Receipt/worker assigned; no result, error, completion, usage or estimate yet |
| UNKNOWN | Receipt/worker and safe error retained; completion remains null; planner requires recovery or safe failure closure |
| SUCCEEDED | Receipt/worker, completion, response SHA-256, STOP finish, PASSED safety and no error |
| FAILED / BLOCKED | Completion and safe error; preserve observed result/usage when dispatched, or no result/usage when aborted before dispatch |

Successful planner calls may precede rendering failure or workflow reconciliation; model success is not job success or creative approval. A failed/blocked planner call cannot belong to a successful attempt. Closed interpretation/attempt parents bound recorded call completion. UNKNOWN records express uncertainty and cannot establish that a call finished or is free to replay. Safe symbolic codes exclude raw provider messages; profiles do not prove published model eligibility, and recorded safety flags do not perform screening.

Token fields map to prompt, generated candidates, cached prompt content and thinking counts respectively. They are strict, nonnegative INT64 values when reported. NOT_REPORTED requires all four null; PARTIAL requires some reported categories; REPORTED requires all four reported. Unknown categories are never defaulted to zero. Known cached counts cannot exceed known prompt counts. Thinking is a separate category from candidate output; actual reported usage is retained even when it exceeds configured limits.

NOT_ESTIMATED requires null cost/currency/pricing fields. PARTIAL/ESTIMATED require at least some reported usage, a nonnegative Python Decimal estimate, a three-uppercase-letter currency identifier and pricing version. ESTIMATED additionally requires fully reported usage; PARTIAL may represent incomplete token or pricing coverage. NUMERIC validation accepts exact values representable within 29 integer and 9 fractional digits, allowing insignificant trailing zeroes, without rounding or dependence on process decimal precision. Nonfinite, overflowing and overprecise values are rejected. This validates estimate structure only; pricing resolution, per-category coverage, calculation, budgets and billed-cost reconciliation remain pending. Pre-dispatch budget reservations are separate from observed per-call estimates.

`validate_model_invocation_change` validates both rows plus optional separately supplied previous request/job/attempt snapshots when their state/receipt changes in the proposed commit. It never reconstructs historical parents. Updates require the expected old revision and exactly one increment, unchanged call/input/profile/start identity, and an immutable assigned receipt/worker. PLANNED can dispatch or abort before dispatch; DISPATCHED can complete or become UNKNOWN; UNKNOWN can reconcile to a terminal result. No dispatched/unknown/terminal call can return to PLANNED or DISPATCHED. A PLANNED failure cannot skip the dispatch claim and pretend a billable call ran.

Known response identity, finish reason, evaluated PASSED/BLOCKED safety outcomes and token counts cannot be rewritten or erased. NOT_EVALUATED safety may be evaluated during reconciliation. A same-state terminal/UNKNOWN update can fill missing accounting fields while retaining response/completion metadata. Known estimates cannot disappear; changing an amount without newly reported usage requires a new pricing version. Immutable outcomes and receipt records still need authoritative reads and transactional fencing before persistence. These pure checks supply neither dispatch proof nor a distributed lock.

Verification on 2026-10-10: [45 model-invocation tests](../tests/test_invocation_contract.py) cover both stages and all six states, all 20 SKUs, pinned profile changes, parent links, receipt/worker binding, incomplete/blocked responses, UNKNOWN recovery, nullable/billable usage, Decimal bounds and context independence, versioned estimates, stale revisions, immutable claims and response identity, no replay, no cloud clients and input preservation. The full suite passed 613 tests at the invocation milestone; the previous full-suite baseline passed 944 with evidence/audit/workspace/membership/conversation/sharing coordinator contracts. No schema/data writes, provider calls, pricing publication, approvals or UI changes occur in this slice.

## Implemented output and artifact contracts

`validate_output` requires the complete output, parent request and completed (or proposed-success) job rows, plus the server-loaded conversation access record. It invokes the existing request/job validators, binds schema/workspace/owner/conversation/request/job/trace/SKU/policy and winning attempt/output IDs, and checks creation time within the job period. Parent-output refinement remains disabled. Output model profiles must match the request's independently pinned stage settings; renderer version must match its pinned submission. Profile list ordering is immaterial. The validator does not commit success or prove that supplied rows exist in BigQuery.

Outputs retain SYNTHETIC_POC, SYNTHETIC_POC_ONLY, initial NOT_REVIEWED and `real_business_approved=false`. Brief schema version is grounded-campaign-brief-v1. Bounded JSON checks and the known top-level brief fields preserve canonical request/product identity, product/image source provenance, synthetic analytics scope, unique evidence IDs, copy reference resolution and recorded planner settings/finish flags. Recorded temperatures accept equivalent integer/float representations while booleans remain invalid. These are structural and consistency checks; factual correctness, concentration/benefit claims, citation support and current approval eligibility still require fresh authoritative grounding. A recorded PASSED flag is not independent proof of those checks.

Guardrail JSON now has a workflow-only version `workflow-output-guardrails-v1` plus `grounding`, `benefit_claims_authorized`, `product_image_integrity`, `human_review` and `content_screening`. The allowed output state records passed grounding/image checks, no benefit authorization and required human review. Screening can remain honestly NOT_INTEGRATED or record PASSED. This future persistence contract does not alter the existing local chat envelope or implement screening.

Artifact IDs and path scope IDs use 1–128 ASCII letters/digits/underscores/hyphens, beginning with a letter or digit. The canonical SKU is unchanged. The fixed bucket and reserved object namespace are:

```text
gs://creative-brand-memory-project-661d501a-e709-4276-a26/workflow_artifacts/v1/<workspace_id>/<conversation_id>/<canonical_sku>/<output_id>/<artifact_id>.<extension>
```

No objects have been written under this namespace. Exact URI matching rejects different scopes/buckets, path traversal, query strings and original product/registered-creative paths. Generation is a positive decimal string of at most 20 digits; SHA-256 is 64 lowercase hexadecimal characters. Sizes are positive INT64 values with server-configured bounds. `ArtifactLimits` defaults to 32 MiB per artifact and 64 MiB per output bundle; ceilings are 128 MiB and 384 MiB respectively. Limits are not accepted from a browser.

| Kind | MIME type | Extension | Output requirement |
| --- | --- | --- | --- |
| BANNER_PNG | image/png | png | Exactly one |
| BRIEF_JSON | application/json | json | Optional, at most one |
| BRIEF_TEXT | text/plain | txt | Optional, at most one |

Artifact IDs and kinds must be unique within an output. Optional BRIEF_JSON must match the canonical bytes of the stored brief: UTF-8 JSON, sorted keys, compact comma/colon separators, unescaped Unicode and no nonfinite numbers. Its declared hash and size must match those bytes. Brief/guardrail JSON stays bounded to 64 KiB and depth 12. Pure contracts perform no I/O, mutate no inputs and return safe ContractError codes without row content. The protected reader below now verifies actual GCS bytes/format/generation; coordinated writers still need to enforce global path uniqueness.

Verification on 2026-10-10: [28 output/artifact tests](../tests/test_output_contract.py) cover all 20 SKUs, scope/parent/state mismatch, provenance, profile/renderer changes, malformed booleans/JSON, byte bounds, unsafe paths, duplicate artifacts and stale JSON hashes. At that contract milestone the full repository suite passed 491 tests. An offline structural check also accepted the existing saved Gemini 3.5 brief (26,021 canonical JSON bytes) when paired with fixture parent records; that audit file is not serving evidence or an access grant. No cloud calls, data/schema writes, artifact uploads or UI changes occurred in the pure contract milestone.

## Implemented protected download reader

The [service gateway](../src/application/protected_service.py) now exposes `download_artifact` for a verified IAP request and workspace/job/artifact IDs. [BigQueryAccessReader.read_artifact](../src/application/access_reader.py) reads complete job/request/output records, resolves current conversation scope and applies the contracts above. It checks DOWNLOAD_ARTIFACT permission before content reads, then rereads current control records after them. Request partitions span the seven days before job creation; output partitions span job creation through completion, bounded to seven days. These are stored job-relative windows, not artifact retention. Missing, duplicate, out-of-window or malformed records deny without fallback; future admission must enforce the request-to-job delay bound.

[GcsArtifactReader](../src/application/artifact_download.py) reads only the exact reserved URI and recorded generation, with conditional metadata/download calls. It verifies generation, MIME, unencoded object size, bounded raw response length and SHA-256. PNGs must be single-frame, decodable 1200 x 628 images; JSON must equal canonical stored brief bytes; text must be nonblank UTF-8 with safe controls. After GCS access, the service repeats the full authoritative read and refuses changed manifests, briefs, ownership, permissions or expired identity before releasing bytes. Responses omit GCS URLs and caller/owner identity and preserve original POC review/provenance metadata.

At the download milestone the full suite passed 528 tests, including [37 download tests](../tests/test_protected_download.py) using real signatures and fake full BQ/GCS records. The read-only ADC probe verified deployed schemas, empty control tables and five summary/full-download query dry runs with zero estimated bytes. No workflow artifacts, row writers, new memberships, approval changes, HTTP endpoint or UI integration were created. A live artifact download and populated-table costs remain unverified. These separate reads do not supply transactional writer fencing or fresh grounding/claim approval proof. See [access boundaries](ACCESS_OWNERSHIP_DESIGN.md#protected-artifact-downloads).

## Implemented evidence snapshot contracts

[evidence_contract.py](../src/application/evidence_contract.py) provides pure `validate_evidence_snapshot` and `validate_evidence_bundle` functions for the existing evidence_snapshots table. They validate complete destination-schema rows and the parent request/job/attempt through the existing contracts, binding workspace, owner, conversation, canonical SKU and attempt. Validation must occur no earlier than attempt start and no later than row creation, with creation before the job deadline; closed attempts bound creation by completion. These supplied rows are neither access grants nor proof that the source was read from BigQuery.

`evidence_schema_version=workflow-evidence-v1` uses an exact JSON envelope `{reference, record}`. Each record has a source-specific field allowlist defined in the module. Callers must explicitly project fresh reader results, encoding dates/timestamps as ISO strings and NUMERIC values as exact decimal strings. No pandas inference, raw daily-sales arrays, image bytes, embedding vectors, unrestricted extra metadata or local-serving fallback is accepted. A payload is limited to 64 KiB and 12 nesting levels; a complete bundle is limited to 256 rows and 2 MiB of canonical payload bytes.

| Source kind | Authoritative source in the fixed project | Content hash meaning |
| --- | --- | --- |
| PRODUCT | ci_product.product_master | Canonical JSON of the projected record |
| KNOWLEDGE_CHUNK | ci_brand_memory.poc_generation_knowledge_chunks; styling uses poc_visual_reference_chunks | UTF-8 chunk_text; must also match the source record hash |
| POC_RULE | ci_brand_memory.brand_guidelines | Canonical JSON of the projected record |
| POC_CLAIM | ci_brand_memory.poc_approved_product_claims | Canonical JSON of the projected record |
| SALES_SUMMARY | ci_sales.sku_daily_sales, aggregated over the pinned SKU/channel/date range | Canonical JSON of the projected aggregate |
| CAMPAIGN_PERFORMANCE | ci_creative.campaign_daily_performance, aggregated over the pinned campaign/asset/date range | Canonical JSON of the projected aggregate |
| PRODUCT_IMAGE | ci_product.product_assets | Pinned original image bytes, declared hash only |
| CAMPAIGN_IMAGE | ci_creative.campaign_assets | Pinned original image bytes, declared hash only |
| GENERATED_IMAGE | ci_creative.generated_campaign_assets | Pinned original image bytes, declared hash only |

Source tables, record keys and supported provenance are restricted. Product facts/images remain BEAUTYFEEDS; POC policy/claims/campaigns/analytics remain SYNTHETIC_POC; styling remains OFFICIAL_BRAND_WEBSITE and VISUAL_INSPIRATION_ONLY. Eligible chunk types exclude review-only/rejected material. Global policy/styling chunks may have null source SKU while the workflow row remains bound to the request SKU. POC claims must be mock-approved exact PRODUCT_NAME evidence with SYNTHETIC_POC_ONLY scope; the complete bundle compares wording with the product name. The existing fixture citation is retained as recorded provenance, never read as a runtime source. Current mock claim records have no review revision, so their nullable review_revision stays null rather than inventing one.

Every image retains exact bucket/path, positive decimal generation of at most 20 digits, lowercase SHA-256, MIME/dimensions and the existing multimodalembedding@001 profile. Generated references retain a positive review revision, review ID/reference, APPROVED_FOR_POC/POC_VISUAL_REUSE_ONLY scope, the original product-image generation/hash and the visual-only usage restriction. Ranked campaign text pins gemini-embedding-001/768; image dimensions accept the existing model's 128/256/512/1408 profiles. Rank, distance and ranking version must be complete together; cosine distance accepts only the reader's small floating-point tolerance around [0, 2]. Bundle ranks are contiguous and use one profile/version per selection group. These checks do not verify vector values, ranking relevance, current reviews or downloaded bytes.

`evidence_ref_id` identifies a source record supporting a citation: `EVR-` plus 32 hexadecimal hash characters from canonical `[source_table, source_record_id, reference]`. Brief citation IDs remain the existing `EVD-` plus 16 hexadecimal hash characters of the reference. Several source records can support one citation; citation aliases may retain the same record with separate row IDs. Duplicate row IDs and conflicting records/pins for one source key fail. Source revisions are retained when available, not fabricated when absent. Bundle rows share one workflow snapshot ID and one non-null knowledge snapshot across applicable sources.

A complete bundle requires the canonical product, factual product/ingredient chunks, applicable POC rules with their policy chunks, original product image and structured sales. Selected campaign text, original banners and performance summaries must agree; generated references must pin the same original product image. Naked & Thriving styling requires the user selection and all four supported visual sections. With an output supplied, the bundle also validates the output contract, attempt/snapshot links, exact citation-index coverage, referenced claim IDs, creation before the brief and projected product/image/policy/claim/performance/full-period sales values. It does not prove claim support in arbitrary prose or validate every optional field/derived trend in an existing brief.

Verification: [44 evidence contract tests](../tests/test_evidence_contract.py) cover all source kinds and 20 SKUs, invalid parents/types/times, hashes, provenance, Decimal strings, payload limits, policy/claim scope, image/review/vector pins, citation aliases, conflicting/mixed snapshots, campaign selection consistency and output linkage. The full suite passed 657 tests at the evidence milestone. The slice is offline and does not alter deployed schemas, source data, approvals, eligibility, model calls or UI behavior. A reader-to-projection adapter, complete grounded-context capture, fresh source/review/GCS rechecks and transactional append-only publication/replay verification remain required before writers can use these contracts. Historical snapshots are audit evidence, never a substitute for current authoritative eligibility.

## Implemented audit contracts

[audit_contract.py](../src/application/audit_contract.py) provides `validate_audit_event` and `validate_audit_replay` for the existing append-only audit_events schema. The module is pure: it does not authenticate identities/workers, evaluate roles from emails/ADC, read or write BigQuery, invoke a model or authorize a mutation. Inputs must be captured server observations from verified identity/current access checks and separately validated domain operations. A typed observation alone does not establish that those checks occurred.

The validator requires every destination-schema field, including explicit nullable values. It binds schema/workspace/actor/action/reason/policy, target scope and request/job correlation, revisions, mutation outcome and trace ID to the supplied observation. The event time equals the captured `observed_at`, which must be no later than the supplied server `recorded_at`; both are timezone-aware. Historical observations can be validated without reading the current wall clock or making up freshness. Audit IDs, trace IDs and resource IDs are bounded ASCII identifiers using letters/digits/colon/underscore/hyphen; principal and membership-target IDs allow up to 256 characters, other IDs 128. This accepts normalized IAP hashes and worker IDs, and rejects email/JWT/URI/free-text forms. Identifier syntax does not prove origin or detect every possible secret; producers must use server-resolved IDs.

No free-form notes, exception text, prompts, responses, JWTs, emails or metadata bags are admitted as extra fields. Validation errors contain a fixed code, never supplied content. Audit data is operational application activity, not synthetic sales/campaign data, so no fabricated source_system is added. Audit visibility remains restricted to authorized operational readers, separately from conversation sharing.

### Access decisions and target scope

User and anonymous events consume the existing frozen `AccessDecision` with typed Action/Reason and `marketing-access-v1`. USER actor identity and membership revision come from that decision. ANONYMOUS requires the authentication-required denial, null actor identity and null membership revision. Allowed flags and reasons must agree; benefit approval and launch actions remain denied. Captured allowed decisions do not bypass current authorization or enable unimplemented workflows.

`AuditTarget` is an immutable minimal server projection. Workspace targets identify the exact workspace and have no content-owner links; membership targets identify the affected normalized principal, which may differ from the admin actor. Conversation and child workflow targets require owner/conversation scope; jobs, attempts, stages, outputs and evidence require request/job links where applicable. Interpretation model invocations can have a request without a job. Denied/missing/foreign lookups without a trustworthy target use target=None and omit resource/ownership metadata rather than inventing it.

For user read/download/refinement/review/visibility decisions, the audit target is the parent CONVERSATION. Its resource_revision records access_revision, including for child downloads. Known request/job IDs can be retained as correlation on that target. Owner/team reasons must agree with its owner, and its identity/revision must match the authorization decision. This avoids confusing a child job revision with the parent sharing revision. Generation and administration have restricted workspace/member/new-owned-resource targets. These projections still require authoritative parent validation in the future producer.

### Mutation and worker observations

Authorization and mutation outcome are separate. An allowed action may encounter a rejected mutation or an unknown commit; it is not automatically APPLIED. A denied action can only record REJECTED when a mutation observation is supplied. Reads never have a mutation outcome. User mutation records currently support visibility updates, member creation/update, workspace model/limit control updates and creation of owned generation resources. Refinement/review events capture access only; their domain write contracts remain separate pending work. Bootstrap is not inferred or performed by this validator.

| Mutation kind | Target revision before operation | APPLIED new_revision | Other outcomes |
| --- | --- | --- | --- |
| CREATE, versioned resource | null | 1 | REJECTED/UNKNOWN: null; NO_CHANGE is invalid |
| UPDATE, versioned resource | Positive INT64 | Previous + 1 | NO_CHANGE: previous; REJECTED/UNKNOWN: null |
| APPEND, stage/output/evidence | null | null | NO_CHANGE/REJECTED/UNKNOWN: null |

`resource_revision` means the target's pre-mutation revision, not the workspace coordinator's fencing revision. Revision fields reject booleans, non-integers, non-positive values and INT64 overflow. Append-only records have no row revision and do not acquire a fabricated revision through an audit.

SERVICE events consume `WorkflowObservation` with a bounded worker ID, known WorkflowAction/WorkflowReason and `workflow-audit-v1`. Membership revision remains null rather than impersonating the initiating user. Supported actions cover request/job/invocation creation, attempt claims, job/attempt updates, model dispatch/result/accounting observations and stage/evidence/output append. Each action fixes the permitted target type and mutation kind. TRANSITION_VALIDATED pairs with APPLIED/NO_CHANGE, TRANSITION_REJECTED with REJECTED and OUTCOME_UNKNOWN with UNKNOWN. The allowed flag describes the captured worker admission/result category; it is not an application role grant or proof of domain validity.

Audit mutation UNKNOWN denotes uncertainty about the mutation/commit, independently from model-invocation UNKNOWN. Successfully recording a model's unknown outcome is an APPLIED database mutation. No resulting revision is inferred for an uncertain commit, and no audit event authorizes automatic replay of uncertain model work.

### Replay and verification

`validate_audit_replay` checks an existing row against its original captured context, checks the candidate schema and requires the same workspace/audit ID and identical content. Changed actor, timestamp, reason, outcome or revision produces a conflict. Later membership revocation must not rewrite historical decisions; historical validation is never current permission. Reconciled outcomes need a new event rather than modifying append-only history. Actual uniqueness, immutable persistence, audit production on every path, atomic related writes and recovery still require the coordinator/writers.

Verification on 2026-10-10: [43 audit contract tests](../tests/test_audit_contract.py) cover all policy denial reasons, owner/team decisions, anonymous/revoked access, strict schemas and safe identifiers, typed/frozen observations, resource links, revision boundaries, user/service outcome matrices, unknown commits and immutable replay. The full suite passed 700 tests at the audit milestone; the previous full-suite baseline passed 944 with workspace/membership/conversation/sharing coordinator contracts. No cloud data/schema writes, model calls, IAM/approval changes, user bootstrap or UI behavior changes occurred.

## Model, evidence and cost snapshots

Requests and outputs carry separate stage profile snapshots; each invocation carries its own effective profile. Preserve stage, selected model, endpoint, profile/catalog version, thinking control/value, output limit, timeout, seed/temperature when applicable, prompt/schema/safety versions and full settings hash. LEVEL requires a thinking level and null budget; BUDGET requires an integer budget and null level; NONE requires both null. Model-specific supported combinations still need M2 evaluation/publication. The schema does not publish profiles or change the current shared model/fixed interpreter setting.

The returned model identity and response hash belong to each invocation, along with finish/safety outcomes. Usage statuses are NOT_REPORTED, PARTIAL or REPORTED; unavailable token categories stay null, never assumed zero. Cost status is NOT_ESTIMATED, PARTIAL or ESTIMATED. Monetary estimates need currency and pricing version; billed-cost reconciliation is still C3. Clarification and failed calls must be accounted for independently of a completed generation.

Evidence snapshots pin source table/key/provenance, content hash and validation time. Pin source/review revisions, knowledge snapshot, approved evidence references, image generations/hashes and embedding model/dimensions/rank/distance when applicable. A hash alone cannot reconstruct mutable analytics: versioned `evidence_payload` JSON retains the exact allowed grounded values supplied to planning, with their references. Validate it against a source-specific allowlist/schema; for images, store metadata/descriptions and integrity references, not bytes. Durable recovery must recheck current eligibility; a historical snapshot is an audit reference, not permission to reuse revoked evidence. Never embed raw daily sales rows or copy vectors into workflow tables.

Outputs store brief/guardrail JSON only under their versioned validated contracts; images stay in private GCS. Artifact records include ID, kind, URI, generation, SHA-256, content type and byte count. Current registration/review eligibility in ci_creative remains separate. Registration is a later explicit action; adding workflow persistence must not automatically register, approve or overwrite existing banners.

## Retention and validation gates

Proposed initial retention: 90 days for conversations and their request/job/output/evidence/artifact bundles, 90 days for detailed invocation/stage records, and 365 days for access/mutation audits. These are design defaults to agree before deployment, not applied expiration settings or regulatory guarantees. Retain current workspace/membership controls until explicitly decommissioned. Raw messages/JWTs/emails and unrestricted model responses are excluded from initial workflow capture. Audience text, briefs and principal IDs remain access-controlled content.

Do not enable independent partition expiry that leaves jobs referencing missing outputs/evidence. A future cleanup workflow first marks a scoped conversation unavailable, preserves necessary minimal audit/idempotency tombstones, and then purges only its related records and pinned artifact generations under agreed policy. Remove or anonymize personal content according to the approved retention policy; keep retry identities for at least the accepted retry window. Proposed retry window is seven days, with older submissions rejected or explicitly creating a new intent. GCS lifecycle cleanup must protect original product images, existing registered/reviewed creative assets and artifacts referenced by other retained records. Default dataset expiration must not silently remove membership/control rows.

Before loaders/service integration, implement and verify:

1. Recursive schema/layout comparison, row validation, typed enums and timestamp/numeric handling; preserve immutable fields on updates.
2. Unique logical keys, scope/owner/parent consistency, same-SKU job/output/evidence links, positive revisions and valid state transitions.
3. Profile/thinking consistency, input hashing and same-key conflict behavior; missing/disabled authoritative profiles fail clearly.
4. Transaction control-row fencing, two-worker intent/dispatch races, unknown commits, lost external responses and success-before-reconnect recovery.
5. Current access checks for status/history/downloads/reviews, removed memberships and visibility changes; bounded partitioned queries and safe denial errors.
6. Output/evidence/artifact publication integrity, unknown usage/cost, and scoped retention without deleting unrelated data.

The schema tests check structural boundaries locally, including nested profile/artifact fields, nullable usage, partition layout and no cloud client calls. Identity/gateway/download tests verify signatures, parent-resource access and artifact integrity with fake database/object rows; live dry runs validate summary and complete job/request/output query projections and partition predicates. Populated-table execution, live artifact downloads, loader idempotency, transaction fencing and recovery remain subsequent milestones.

## Implemented workspace control contracts

[workspace_contract.py](../src/application/workspace_contract.py) validates the complete destination schema for `ci_application.workspaces`. Only `ACTIVE` and `DISABLED` statuses are supported. Revisions are positive BigQuery INT64 values; identifiers and schema versions are scoped explicitly. Timestamps must be timezone-aware, representable in UTC and ordered.

`validate_workspace_creation` checks an ACTIVE revision-one initial-row proposal with creation/update times equal to the supplied server observation. It does not provision a workspace or assign users. Controlled bootstrap and uniqueness checks remain separate operator/coordinator responsibilities.

`validate_workspace_change` checks immutable workspace identity/schema/creation time, the expected prior revision, exactly one revision increment and a nondecreasing update time equal to the supplied server observation. Typed `FENCE` proposals require an ACTIVE workspace and unchanged status. Typed `SET_STATUS` proposals require a real ACTIVE/DISABLED transition; disable/reactivate proposals need a separately authorized control path. Ordinary workflow writes cannot fence a disabled workspace. Reactivation is not an exception that ordinary workflow callers can request. That operator/control transaction and its authorization/audit action remain to be designed before status writes are exposed.

`propose_workspace_fence` returns an independent copy without modifying the stored snapshot. Same-clock timestamps still consume a revision; exhausted revisions cannot wrap or reset. These checks neither prove that the supplied clock is trusted nor acquire a distributed lock. The future coordinator must select exactly one current control row and revalidate/update it atomically with related records. The existing access reader now uses the complete workspace row validator; duplicates, missing records and disabled workspaces still deny access.

Verification on 2026-10-10: [34 workspace tests](../tests/test_workspace_contract.py) cover creation, status/fence distinctions, stale and exhausted revisions, timestamp boundaries, immutable fields, safe errors, no cloud calls and input preservation. All 21 access-reader tests pass with additional malformed-control cases. The full suite passed 734 tests at the workspace milestone; the previous full-suite baseline passed 944 with membership/conversation/sharing coordinator/query contracts. No cloud data/schema writes, model calls, bootstrap, IAM changes or UI integration occurred.

## Implemented membership contracts

[membership_contract.py](../src/application/membership_contract.py) checks the complete `ci_application.memberships` destination schema. `validate_membership` binds the stored principal ID to the existing IAP issuer/subject hash and returns immutable, typed access metadata. It accepts only known unique roles, ACTIVE/SUSPENDED/REVOKED status and ADMIN/SYNC/BOOTSTRAP provenance. Empty roles remain valid but grant no actions. Revisions are positive BigQuery INT64 values; timestamps must be timezone-aware, representable in UTC and ordered. The access reader shares this validator and retains the exact verified-identity comparison and fail-closed behavior.

Creation proposals require revision one and creation/update times equal to the supplied observation. Initial inactive records are representable but grant no access. Change proposals bind the updater and typed origin, preserve workspace/principal/issuer/subject/creation time, require the expected prior revision and exactly one increment, and reject exhausted revisions or backward/mismatched update times. A real role-set or status change is required: role order, updater metadata or timestamps alone cannot consume a revision. All six explicit status transitions are representable; revoked targets can be reactivated only through an authorized management path. BOOTSTRAP cannot update an existing row.

`validate_admin_membership_mutation` evaluates supplied pre-write snapshots with the existing MANAGE_MEMBERS policy. It requires an ACTIVE workspace with the expected usable control revision and an ACTIVE ADMIN actor membership bound to the actor's principal. The candidate's updater is that actor and its origin must be ADMIN. The user-admin path cannot claim SYNC or BOOTSTRAP provenance. Self-mutations require the target's previous row to equal the actor snapshot; the returned access decision captures the actor's pre-mutation revision for audit. Explicit self-demotion/revocation is representable. Preventing loss of the workspace's last usable admin requires additional authoritative reads and remains a future management-writer safeguard.

`propose_admin_membership_change` returns an independent copy with deterministically sorted roles. It does not persist assignments or implement retry idempotency. Identical no-op updates are rejected by the change contract; the future writer must resolve a repeated mutation's committed identity before proposing another revision.

These checks do not authenticate subjects, verify snapshot freshness or prove a commit. The future service must verify sign-in and the coordinator must reread current workspace/actor/target rows, reject duplicates and fence the mutation with its audit atomically. Stored SYNC/BOOTSTRAP provenance and structural validation do not enable those management paths. Controlled bootstrap, synchronization identity/mapping, management endpoints, last-admin protection and workspace lifecycle authorization/auditing remain pending. No role is inferred from email, prompts or ADC.

Verification on 2026-10-10: [47 membership tests](../tests/test_membership_contract.py) cover identity binding, schemas, role/status/origin rules, revisions/times, creation and all status transitions, revoked/non-admin actors, cross-workspace access, self-revocation, deterministic proposals, safe errors and no authentication/cloud calls. All 21 access-reader and 59 protected gateway/download tests pass. The full suite passed 781 tests at the membership milestone; the previous full-suite baseline passed 944 with conversation/sharing coordinator/query contracts. No cloud data/schema writes, IAM changes, user bootstrap, model calls or UI behavior changes occurred.

## Implemented conversation contracts

[conversation_contract.py](../src/application/conversation_contract.py) validates the complete `ci_application.conversations` destination schema and returns immutable access metadata. Rows require the exact workspace/conversation identity, bounded owner ID, ACTIVE/ARCHIVED status, PRIVATE/TEAM visibility, positive BigQuery INT64 access revision and ordered timezone-aware timestamps representable in UTC. The access reader shares these checks and preserves archived read/download-only behavior. It still denies duplicate/missing/malformed records.

Creation requires ACTIVE/private revision one, a supplied expected owner and creation/update times equal to the server observation. `propose_conversation_creation` derives ownership from the acting principal after applying GENERATE_DRAFT to the supplied workspace/membership snapshots. It accepts no custom owner or initial visibility. This is a proposal for a new identity, not proof that the ID is unused; the coordinator must reject an existing conversation rather than replace it.

Sharing preserves workspace/conversation identity, owner and creation time; it requires the expected prior access revision, exactly one increment and a nondecreasing update time equal to the observation. A typed SET_VISIBILITY change must actually switch PRIVATE/TEAM and cannot change lifecycle status. The creator mutation gate applies the existing creator/owner policy against supplied pre-write records, checks an ACTIVE workspace with the expected usable control revision, and returns the pre-mutation access decision for audit. Revoked/suspended actors and nonowners cannot share; ADMIN alone supplies no creator bypass. Proposals are independent copies. No-op sharing is rejected; committed retry identity resolution remains the writer's responsibility.

Archival has an offline structural contract: ACTIVE to ARCHIVED, one access revision increment and unchanged visibility/ownership. Archival cannot silently reshare. Archived rows cannot be updated, restored or shared through these contracts. The creator mutation gate explicitly rejects ARCHIVE with `CONVERSATION_LIFECYCLE_WRITE_DISABLED`; it does not reuse sharing permission or fabricate an audit action. An explicit lifecycle permission, versioned policy/audit contract and in-flight job behavior must be defined before an archival writer is exposed. Restoration and ownership transfer remain outside this contract.

These checks do not authenticate actors, establish current snapshot freshness, acquire a lock or write rows/audits. The future service verifies identity; the coordinator rereads current workspace/membership/conversation records, rejects duplicates and atomically fences related mutations and audits. Actual owner identity binding comes from that trusted creation path, not an arbitrary stored label. The legacy metadata-only visibility helper is not sufficient to enforce conversation lifecycle at a durable write boundary.

Verification on 2026-10-10: [43 conversation tests](../tests/test_conversation_contract.py) cover row schemas, private creation/owner binding, revision/timestamp boundaries, sharing and structural archival, revoked/nonowner actors, disabled lifecycle writes, audit compatibility, safe errors, independent proposals and no cloud/authentication calls. All 21 access-reader and 59 protected gateway/download tests pass. The full suite passed 824 tests at the conversation milestone; the previous full-suite baseline passed 944 with sharing coordinator/query contracts. No cloud data/schema writes, role/IAM changes, model calls, bootstrap or UI behavior changes occurred.

## Implemented sharing coordinator contract

[sharing_coordinator_contract.py](../src/application/sharing_coordinator_contract.py) implements one offline, nonbillable SET_VISIBILITY coordinator contract. It does not execute SQL, verify sign-in, read BigQuery, persist rows or prove a commit. It leaves the eleven deployed schemas and local UI unchanged.

`SharingCommand` is frozen and bounds workspace/conversation/principal identity, typed target visibility, original membership/conversation revision pins, hashed retry key, server-issued acceptance time and intent trace. Acceptance time and trace are immutable across retries; a new transport attempt does not regenerate them. The retry window is the existing seven-day policy, ending exclusively at `deadline_at`. The operational workspace fence revision is separate from the command and may be explicitly refreshed after a positively known noncommit without changing the accepted domain intent. Membership/conversation pins cannot be silently rebased.

The reserved audit ID format is `share-v1:<scoped_retry_digest>:<command_digest>`. Both are full SHA-256 digests encoded as unpadded base64url; the 96-character ID fits the existing audit identifier bound. The scoped retry digest binds contract/action/workspace/principal/conversation and the hashed retry key. The command digest also binds target visibility, membership/conversation revisions, policy and original acceptance time/trace. Unlike generation's logical request hash, this digest pins the entire accepted sharing envelope. Same retry scope with changed envelope retains the same lookup prefix but conflicts on the suffix. No raw retry key or new schema column is persisted.

Retry guarantees are bounded to the original valid intent window. The future service must resolve the original server-issued envelope or verify a sealed one; browser input must not be allowed to replace its acceptance time/trace and move the receipt lookup window. An explicit new intent uses a new retry key. This offline contract does not establish permanent key uniqueness across expired windows or implement an intent issuer/retention registry.

`prepare_sharing` requires bounded scoped row lists (at most two lookup results), rejects missing/duplicate control rows and duplicate receipt prefixes, validates complete schemas, and applies current supplied creator/owner policy before receipt reuse. Future reads must resolve receipts by exact workspace, `STARTS_WITH(audit_id, @audit_prefix)` and the original acceptance/deadline partition bounds, with LIMIT 2. Equality on the full audit ID alone is insufficient: it would miss a changed-command conflict under the same retry key. These are read-contract requirements, not implemented SQL or a measured query plan.

For a new write, expected membership/conversation/fence revisions must match. The PREPARED result holds independent, immutable workspace-before/after, conversation-before/after and audit candidate rows. Both control revisions increment exactly once and all proposed update/audit times match the supplied observation. Full workspace/conversation/audit validators bind their scope. The staged audit uses APPLIED to describe the proposed successful commit; PREPARED is not an actual success and exposes no resulting revision.

A matching supplied committed receipt produces REUSED with the original conversation revision and no write proposal. Audit fields must match the original command, APPLIED outcome, actor/owner and revision pair; time must be within the original window. Current access must still allow sharing. Current membership/workspace revisions can have advanced. Current conversation revision/time must be consistent with the receipt; at the receipt's exact resulting revision the visibility must match. Later legitimate visibility changes are preserved, and REUSED does not claim the old visibility is current. Historical audit validation never replaces current authorization.

`SharingCommitObservation` binds the command's audit ID to typed NOT_SUBMITTED/COMMITTED/NOT_COMMITTED/UNKNOWN status. Every submitted observation carries a preallocated query job ID for future reconciliation. NOT_COMMITTED must come from a positively verified aborted/not-executed job; a timeout, exception string or missing audit is not proof. UNKNOWN or declared COMMITTED without a matching receipt returns UNKNOWN with no plan. A matching receipt can resolve uncertainty; a receipt contradicting NOT_COMMITTED fails closed. These are consistency checks on supplied observations, not provider/job verification.

Only an atomically committed APPLIED sharing audit may use the reserved retry prefix. Denial, no-op and uncertainty diagnostics must use separate audit identities and must not occupy or rewrite that receipt. Future SQL must atomically reread/fence the unique workspace and current actor/conversation, check the entire receipt prefix and conditionally update both control rows plus append the audit. All competing writers must participate. Missing receipts are not replay authorization; no model call belongs in this transaction/retry path. The executor and isolated native probe now verify original-job correlation, commit classification and fixture transaction races. The authoritative read-only receipt path is now implemented below. Controlled fresh attempts and deployed cancellation/crash recovery remain pending; see [native evidence](SHARING_INTEGRATION.md).

Verification on 2026-10-10: [41 coordinator tests](../tests/test_sharing_coordinator_contract.py) cover full digest identities, changed-envelope conflicts, immutable proposals, duplicate/missing/scope failures, current revocation/ownership, revision/window bounds, audit binding, later state preservation, contradictory/unknown commit observations and no cloud/authentication calls. The full suite passed 865 tests at the coordinator milestone; the previous full-suite baseline passed 944 with sharing query/executor foundations. No cloud data/schema writes, role/IAM/approval changes, bootstrap, model calls or UI behavior changes occurred.

## Implemented sharing transaction query contract

[sharing_query.py](../src/application/sharing_query.py) renders
[share_conversation.sql](../sql/share_conversation.sql) from fixed project/table
identifiers and the existing destination-schema metadata. All command and attempt
values use explicitly typed scalar BigQuery parameters. No client is constructed,
query submitted, row written or identity authenticated by this module.

The script checks the original seven-day intent window using the transaction's
server timestamp, reads at most two scoped workspace/membership/conversation and
receipt rows, and rejects missing or duplicate control records. Receipt lookup
uses the complete retry prefix and an explicit `occurred_at` partition interval,
so a changed accepted command conflicts instead of evading exact-ID lookup.
Complete control metadata, IAP principal hashing, unique known roles, active
creator membership and active conversation ownership are checked before either
writing or reusing a historical receipt. ADMIN alone grants no sharing bypass.

With no receipt and a NOT_SUBMITTED or positively established NOT_COMMITTED
observation, the script compares the original membership/conversation pins and
the operational workspace revision. It rejects stale revisions, exhausted fences
and visibility no-ops. Scoped conditional updates consume one workspace and
conversation revision; a fully specified APPLIED audit is inserted in the same
transaction. Each mutation must affect exactly one row. An error rolls back and
is rethrown; the final result statement runs after commit. No unrelated deletion,
MERGE, table replacement or inferred schema is used.

A matching successful receipt returns REUSED without updating the fence or
visibility. It validates all historical audit fields and their causal relationship
to current conversation state; later legitimate changes remain intact. A missing
receipt with UNKNOWN/COMMITTED input returns UNKNOWN without any write. A receipt
contradicting a NOT_COMMITTED observation fails. Failure/denial diagnostics remain
outside the reserved successful-receipt namespace.

Frozen `SharingQuerySettings` defaults to 64 MiB maximum bytes billed, a 60-second
job timeout and a 20-second statement timeout, with server-configurable bounded
values. Every configuration call creates independent SDK objects, disables cache
and session mode and pins `asia-south1`. A timeout is not evidence of noncommit;
the future executor must disable automatic job retries and reconcile the original
preallocated job ID. These limits have not been measured against populated data.

`validate_sharing_result` accepts exactly one server-internal result row, reuses
the complete control/audit validators, checks APPLIED revision/timestamp bindings
and preserves UNKNOWN. Its immutable response contains only outcome, audit ID,
observation time and original resulting conversation revision. Full identity
records must not reach the browser or logs. Result validation cannot establish
that arbitrary supplied rows came from a committed, correlated BigQuery job.

Google documents snapshot isolation, atomic commit and cancellation of conflicting
mutating transactions. Every access-changing management/lifecycle participant
must update the same existing workspace fence; unenforced keys and append-only
audits cannot supply this coordination alone. This design does not claim per-row
locking, a throughput guarantee or protection from uncontrolled external writes.
See [BigQuery transactions](https://docs.cloud.google.com/bigquery/docs/transactions).
Script dry runs can skip DDL/control-flow bodies and cannot establish runtime
correctness; see [multi-statement query dry runs](https://docs.cloud.google.com/bigquery/docs/multi-statement-queries).

Verification on 2026-10-10: [31 query-contract tests](../tests/test_sharing_query.py)
cover parameter types/window stability, fixed identifiers/schema declarations,
bounded independent SDK settings, transaction/receipt/mutation boundaries,
malformed results, exact post-state bindings, current revocation/ownership,
changed-command conflicts, historical state preservation and unknown outcomes.
The 72 combined sharing tests and full 896-test suite passed offline at the query milestone; the previous full-suite baseline passed 944 with the executor tests. No native SQL
execution, cloud write, role/approval/bootstrap change, model call or UI wiring
occurred at the query-contract milestone. The subsequent [isolated native probe](SHARING_INTEGRATION.md) verifies fixture execution, races and original-job recovery; deployed recovery remains pending.

## Implemented sharing executor and sealed intents

[sharing_executor.py](../src/application/sharing_executor.py) adds a server-only
`ProtectedSharingExecutor` with separate intent acceptance, submission and
recovery methods. Execution is disabled by default. Trusted runtime composition
must supply the existing IAP verifier, the same project-scoped BigQuery client
and access reader, an explicitly configured intent sealer and bounded settings.
Client default query configuration is refused to prevent hidden write/session
overrides. No HTTP endpoint or local chat integration is added.

Every call authenticates observed headers anew. Intent acceptance derives the
principal from verified identity and time/trace from the server, then checks
current creator/owner permission. Revision inputs are optimistic expectations,
not permission claims. Submission/recovery resolve the sealed original envelope,
check current membership/workspace/conversation access and check access again
before returning historical results. Revocation during a result read denies the
response; a previously successful job never supplies a current access grant.
No email/ADC-derived roles or fixture bootstrap is provided.

[sharing_intent.py](../src/application/sharing_intent.py) uses a domain-separated
HMAC-SHA256 envelope containing the accepted command, original fence expectation
and exact query/configuration fingerprint. Canonical JSON, bounded canonical
base64url, strict fields/version, signature, principal and original seven-day
window are validated. The fingerprint derives a stable full-digest native job ID
before dispatch and binds project/region, SQL and requested SDK configuration.
Browser changes to time, trace, revisions or execution settings cannot resolve
through an unchanged seal. SQL/settings changes fail closed on old envelopes;
versioned migration compatibility remains future work.

The seal is integrity protection, not encryption, persisted business knowledge or
an authorization grant. It retains opaque IDs/revisions and hashes, never raw
assertions/identity subjects or the raw retry key. BigQuery remains authoritative;
there is no local intent/data registry or serving fallback. The 32–64-byte signing
key has no default and is supplied at runtime, never written to disk by the code.
Deployment must keep managed key/configuration consistent across replicas and
restarts for the intent lifetime. Managed-secret provisioning, key rotation and
retained-version support are not implemented. Transport retries retain the token;
calling acceptance again creates a different accepted envelope and is not recovery.

Before submission the adapter verifies dataset and all four destination schemas
and layouts with the existing setup validators. It makes one explicit
`jobs.insert` call per submit, with the stable ID, project/region, bounded timeout
and both RPC/job retries disabled. Repeated submits use the same ID and reconcile
collisions/lost responses. `recover` performs only reads of that original job.
It checks native parent identity, SQL, parameters, cache/session settings and
requested timeout/byte bounds before trusting results. Nested SDK Rows are
converted with bounded indexing, and exactly one final result passes through
the existing result validator. The returned projection excludes full identity
records. Recovery returns the original script outcome; it does not claim another
mutation just occurred.

Missing/inaccessible/pending job metadata, timeouts, invalid result rows and
unverifiable correlations retain UNKNOWN. A job error alone is insufficient:
a script can commit and then fail its final result statement. For a failed,
correlated terminal parent, the adapter examines a bounded list of terminal
child jobs with matching parent/project/region. One successful native rollback
and no successful commit establishes NOT_COMMITTED. One successful commit and
no successful rollback establishes COMMITTED without inventing a lost result.
Empty, truncated, conflicting or incomplete child evidence remains UNKNOWN.
Nonterminal errors in `errors[]` do not override a successful final result.
See [BigQuery job status](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/Job)
and [job management](https://docs.cloud.google.com/bigquery/docs/managing-jobs).

This slice handles the original sealed attempt only. It never remints a job after
unknown or proven rollback. Controlled fresh-fence retry after positive noncommit,
intent version/key migration and endpoint response/audit policies remain pending.
Separate authoritative receipt recovery is implemented in the subsequent slice below.
Native job retention and least-privilege deployment must preserve the recovery
window. Per-RPC/result timeouts are bounded; a deployed host's overall request
deadline and cancellation behavior still need integration verification.

Verification on 2026-10-10: [33 executor tests](../tests/test_sharing_executor.py)
cover real ES256 sign-in, seal tampering/identity/window/configuration binding,
default disablement, schema preflight, current revocation/ownership, stable IDs,
disabled retries, duplicate submission and lost-response recovery, native job
scope/configuration mismatches, nested SDK Rows, bounded results and positive
commit/rollback classification. At the executor milestone, the 105 focused sharing tests and full 929-test
suite passed offline. That slice added no live query, cloud data/schema write, key provisioning,
role/bootstrap/approval change, model call or UI integration occurred. Offline
fixtures do not prove native SQL execution or transaction races.

## Implemented isolated native sharing verification

[sharing_integration.py](../src/validation/sharing_integration.py) is an opt-in
ADC harness using four explicitly schematized, expiring tables in a fresh
`ci_sharing_it_<uuid>` dataset. It redirects only the existing template's four
exact table paths; production SQL scope and default-disabled execution are
unchanged. Native correlation and commit classification now live in
[sharing_jobs.py](../src/application/sharing_jobs.py), shared by the executor
and the probe. Cleanup requires verified dataset ownership, no unexpected tables
and terminal native jobs. No serving users or business records are created.

The native acceptance cases, repeatable command, fixture isolation, verification
report and limitations are recorded in [SHARING_INTEGRATION.md](SHARING_INTEGRATION.md).
At the transaction-probe milestone, fifteen offline probe tests and the full 944-test suite passed. This slice verifies
fixture transaction behavior; deployed SSO, host cancellation, process crashes,
management endpoints and populated production performance remain separate gates.

## Implemented authoritative sharing receipt recovery

[sharing_receipt.py](../src/application/sharing_receipt.py) builds a bounded
single SELECT for the complete control records and receipt prefix in the original
intent window. The separate `recover_receipt` method in the server-only executor
resolves the original seal, checks current identity/access and schemas, verifies
native read-job correlation, validates the receipt through existing contracts and
rechecks access before release. REUSED preserves the original resulting revision
and later visibility changes; receipt absence stays UNKNOWN. The result proves
the business intent committed, not a specific mutation job's outcome. It never
submits a mutation, advances a fence or issues a new intent.

The 181 impacted tests and six focused native fixture cases passed. The owned
dataset was deleted; no serving users, business data, approvals or UI behavior
changed. The previous full-suite baseline remains 944 tests; unrelated suites were
not rerun for this small slice. See [receipt behavior and evidence](SHARING_RECEIPT_RECOVERY.md).
Execution is still disabled by default; endpoint integration and controlled new
attempts remain pending.

## Implemented controlled-retry contract

The offline [sharing retry contract](SHARING_RETRY_CONTRACT.md) now preserves the
original accepted command, business pins, receipt and seven-day window while
allowing explicit nonregressing operational fence refresh. It requires supplied
positive noncommit bound to the immediate predecessor, limits the chain to three
retries and creates deterministic domain-separated job IDs and runtime seals.
The new job starts NOT_SUBMITTED, so an existing original receipt can be reused
without assigning the predecessor's rollback status to the new job. Thirty new
tests and all 211 affected tests passed. No cloud calls occurred in this slice.
Typed observations and seals do not prove native rollback or current access;
the trusted executor now connects those gates as recorded below. Native retry
races remain pending.

## Implemented trusted retry executor

The [trusted retry adapter](SHARING_RETRY_EXECUTOR.md) now issues/submits/recovers
sealed retries and accepts retry tokens for read-only receipt recovery. New
dispatch requires fresh correlated positive rollback for every predecessor,
deployed schema checks, current scoped BigQuery controls/receipts and current
creator/owner access. Issuance derives the operational fence from the snapshot;
submission preserves the sealed fence and original business pins. Uncertain
predecessors cannot issue a new retry. Existing native jobs recover without
resubmission, and a receipt can return REUSED while the unresolved retry job
observation stays UNKNOWN. No model call or automatic retry is introduced.
All 252 affected offline tests passed, including 41 new adapter tests. No cloud
calls occurred in that adapter slice. Execution remains disabled. The separate
native retry evidence is recorded below; managed-key/deployment/HTTP/UI integration
remains pending.

## Implemented isolated native retry verification

The [retry-only probe](SHARING_RETRY_INTEGRATION.md) passed ten native cases in
asia-south1 using an owned temporary dataset with four expiring schema-matched
tables and nine synthetic workspaces. It verifies actual sealed attempt IDs,
positive native rollback, refreshed fences, duplicate/discarded responses,
overlapping competing retries with one applicable mutation/audit, another
attempt's receipt, stale fences, fenced revocation, post-commit recovery and
preservation of later visibility/unrelated rows. All 266 affected offline tests
passed, including 14 new isolation/mode/proof/cleanup guards. The owned dataset
was deleted; serving writes were zero and execution was not activated. The
isolated SQL digest and contract fingerprint are recorded separately; serving
correlation rejects relocated fixture jobs. Actual IAP/HTTP executor invocation,
host cancellation/process crashes and production throughput remain unverified.

## Implemented signing-key and version retention contract

The pure [signing policy and opt-in retained sealer](SHARING_SIGNING_RETENTION.md)
now preserve original key/profile, seven-day intent windows and stable job IDs
through planned rotation and nested retries. Initial issuance intervals cannot
overlap, retained material covers the full original lifetime, and early key
removal/rebinding or revocation reversal is rejected. Retired keys can sign
bounded retries for their original cohort. Different settings profiles can
coexist; unsupported installed SQL/schema/handler drift fails closed. All 304
affected offline tests passed, including 38 new tests and five executor checks.
No cloud calls or execution-default changes occurred in that contract slice.
Pinned Secret Manager loading is now implemented as recorded below. Controlled
policy publication, deployed revocation/replica consistency and future
incompatible handler migrations remain unimplemented.

## Implemented pinned secret-loader foundation

The opt-in [Secret Manager loader](SHARING_SECRET_LOADER.md) now composes retained
sealers from trusted regional numeric version pins, with ADC client construction,
exact SDK response identity/CRC32C checks, bounded retry-disabled RPCs and no
partial/cache fallback. Revoked keys are not fetched; their pins remain in
metadata for transition checks. All 335 affected offline tests pass, including
31 new tests, and dependency checks pass. No live secret access/provisioning or
execution activation occurred. Trusted project-number association, live resources,
authoritative publication/refresh and managed rollout remain deployment gates.

## Implemented publication and replica refresh contract

The pure [publication/refresh contract](SHARING_PUBLICATION_REFRESH.md) now binds
parent-linked increasing revisions, complete immutable key/version/profile history,
policy-wide stop and bounded replica readiness. Retired identities/resources cannot
be reused; STOPPED marks the active key revoked in history and creates no load ticket.
Refresh invalidates readiness before reads, pins valid higher heads before loading,
and rejects stale generation/instance callbacks against supplied current state.
All 379 affected offline tests pass, including 44 new tests. No cloud calls or
activation occurred. Actual BigQuery resource setup/readers/writers, key swaps, durable
restart floors and instantaneous/global revocation remain unimplemented.

## Implemented publication storage and read design

**4.1 — High / Must have — shared pilot** is complete offline. Four separate
[signing control schemas](../src/application/sharing_publication_schema.py) cover
publications, a unique head, complete per-revision key history and durable consumer
checkpoint floors. The [pure read contract](../src/application/sharing_publication_read_contract.py)
validates supplied counts/projections, unique heads, bounded consecutive chains,
exact history/hash pins, checkpoint/known-anchor boundaries, freshness and read-ticket
scope. A running process cannot jump to a checkpoint over missing revisions.
Read acceptance never grants readiness or proves I/O. Unheaded publications deny.

[The storage/read design](SHARING_PUBLICATION_STORAGE.md) defines strict future
codec/snapshot requirements, logical keys, layout, indefinite metadata retention,
resource permissions and failure behavior. The eleven workflow schemas remain
unchanged. All 443 affected offline tests passed for 4.1, including 33 new tests.
The subsequent [4.2 resource/read slice](SHARING_PUBLICATION_READER.md) now
provisions/verifies four empty control tables, implements exact codecs and fresh
bounded SQL/SDK reads, and passes 482 affected offline tests including 39 new tests.
The [4.2 writer slice](SHARING_PUBLICATION_WRITER.md) now implements explicit IAP
global grants, fenced publication/checkpoint persistence and original-job recovery;
519 affected offline tests and 13 isolated native writer cases pass with owned
cleanup. [Controlled genesis bootstrap](SHARING_SIGNING_BOOTSTRAP.md) now completes
4.2's implementation/native fixture scope: 546 affected offline tests and 13
bootstrap native cases pass with owned cleanup. Runtime integration is next in 4.3.
No serving signing-policy rows, IAM or runtime activation changed.

## Next small steps

The authoritative remaining sequence is the [fixed Priority 4 baseline](IMPLEMENTATION_PLAN.md#fixed-remaining-priority-4-milestones).
The already completed contracts, table setup and protected read/download
foundations retain their documented scope. Use these existing IDs in progress
reports; tasks and verification details remain under their parent milestone.

1. (High; Must have — shared pilot) **4.1 — Complete: BigQuery publication schema and authoritative read design.** [Four control schemas and supplied-snapshot contract](SHARING_PUBLICATION_STORAGE.md) define retention, unique-head and complete bounded chain/hash checks. 443 affected tests pass; no provisioning or actual reader in this step.
2. (Critical; Must have — shared pilot) **4.2 — Complete within implementation/native fixture scope: Publication persistence and authoritative reader.** [Resource setup, strict codec and scoped reader](SHARING_PUBLICATION_READER.md), the [authorized fenced writer](SHARING_PUBLICATION_WRITER.md) and [controlled genesis bootstrap](SHARING_SIGNING_BOOTSTRAP.md) are implemented and verified. Real serving bootstrap remains an explicit deployment action; shared execution is disabled.
3. (Critical; Must have — shared pilot) **4.3 — Complete within implementation/native fixture scope: Runtime signing refresh integration.** Fresh authoritative refresh, resource bindings, restart floors, rotation and revocation are verified; serving activation remains deferred.
4. (Critical; Must have — shared pilot) **4.4 — Complete within implementation/isolated verification scope: Runtime recovery and protected API gates.** See [runtime gates](WORKFLOW_RUNTIME_GATES.md): budgets/owned workers, protected read/download/management HTTP and audit correlation, controlled management safeguards and existing-invocation dispatch. Company identity activation remains deferred.
5. (Critical; Must have — shared pilot) **4.5 — Complete within implementation/affected-test scope: Fresh evidence and audit producers.** Native BigQuery read identities, current eligibility, opted-in styling context, pinned GCS bytes, complete deterministic planning bundles and bounded-fresh access/service audit envelopes are validated. 246 affected tests pass; no persistence or live cloud calls occurred.
6. (Critical; Must have — shared pilot) **4.6 — Complete within implementation/native fixture scope: Durable workflow and artifact writers.** Seven explicit-schema atomic write plans and a default-disabled executor are implemented. Six isolated native duplicate/conflict/rollback/post-commit/original-job recovery cases passed with owned cleanup and zero serving-row change. See [native writer evidence](WORKFLOW_WRITER_INTEGRATION.md).
7. (High; Must have — shared pilot) **4.7 — Protected submission, history and UI reconnect.** Connect authorized submission, paginated history, restored conversations/jobs and protected downloads; verify access and recovery. Refinement/review and shared deployment retain priorities 9 and 12.

Continue at **4.7 — High / Must have (shared pilot)**. Fixed Priority 4 progress is **6/7 complete, 1/7 remaining**. This list expands the existing remaining scope into stable IDs;
it does not create new priorities. See the plan for binding exit criteria and
scope-change rules.

Company SSO activation stays deferred. Full U1/F1/U3, M2 profile publication, accounting collection/pricing/budgets, audit retention and Workspace deployment remain open. See [access verification and trust boundaries](ACCESS_OWNERSHIP_DESIGN.md).
