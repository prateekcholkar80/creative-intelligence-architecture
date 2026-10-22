# Local campaign chat guide

The Streamlit interface accepts a campaign request, helps complete its preferences, and generates a grounded brief and banner preview using the existing BigQuery and GCS pipeline. Gemini 3.5 Flash interprets the request and plans the campaign in asia-south1; Pillow composes the banner from the verified original product image.

## Start the interface

Current use is a local POC accessed with the user's personal Google account. A company Workspace account is not needed for this existing local flow. The implemented backend permissions policy and future company SSO design are retained; actual company domain/group setup is deferred. Personal-account cloud credentials do not provide an application login or role assignment.

Run from the repository root with the existing virtual environment and Google Application Default Credentials:

```sh
.venv/bin/python -m streamlit run app.py
```

Open http://127.0.0.1:8501. The server binds to the local machine. If dependencies need installing, use `.venv/bin/python -m pip install -r requirements.txt`.

The application reads the 20 canonical products from BigQuery. If authentication or the catalog is unavailable, restore the cloud connection and click **Refresh product catalog**. Local files do not substitute for product evidence.

## Generate a campaign

1. Enter a request in the chat box, for example: “Create a conversion campaign for CI-SKU-0001 on paid social for skincare shoppers, using Naked & Thriving styling.”
2. Review the interpreted product, objective, audience, channel and visual inspiration. Complete missing details using the campaign form or a follow-up chat message.
3. Click **Generate preview**. Progress shows evidence retrieval, campaign planning, banner composition and final evidence checks.
4. Review the banner, expand **Campaign brief and evidence**, and download the image, brief or metadata.

Follow-up messages can change the five supported preferences and generate another preview from fresh evidence. Requests for benefit claims, discounts, custom copy, exact layout/font/color directions, other dimensions, approval or publication are flagged as outside this first slice. New previews remain unreviewed POC drafts.

## Grounding and session behavior

Product metadata, current claim/rule eligibility, creative references and structured analytics come from BigQuery. Images come from GCS and pass the existing integrity checks. Website research supplies styling inspiration only; sales and campaign performance are simulations. The renderer preserves the entire original product image and validated copy.

Conversation history and generated results are held in server-side memory associated with the browser session. Reloading, disconnecting or restarting the server can lose that history. **New conversation** clears the current session. These results are review artifacts, not registered assets or authoritative approvals. The interface performs no product ingestion, cloud asset registration, approval writes or campaign launch.

Generation occurs only on form submission. Session request IDs prevent a completed submission from generating again during ordinary page reruns or downloads. Durable idempotency across reconnects and multiple workers requires the planned BigQuery job records.

## Usage and limitations

Request interpretation has a bounded message length, a 1,024-token output limit and MINIMAL thinking. Planning retains the existing 4,096-token output limit and uses LOW thinking. Both stages use explicit safety settings and require a complete, unblocked response. The application model client has a 120-second timeout and one configured HTTP attempt; existing embedding modules retain their own bounded retries. Catalog queries have an explicit BigQuery byte limit.

Today the model is shared by both stages through backend configuration, planner thinking is configurable, and interpreter thinking is fixed to MINIMAL in code. Independent stage model/thinking profiles and authorized campaign-form controls are planned in [priority 8](IMPLEMENTATION_PLAN.md#independent-stage-profiles-and-thinking-controls).

Generation details show available interpreter/planner token usage. Earlier clarification turns and embedding calls are not included in a complete cost ledger. Estimated monetary cost is unset, and centralized tracing, billing reconciliation and application spending controls remain planned.

Serving activation of durable conversation/job persistence, reviewer actions, arbitrary creative refinement, shared-user authentication and broader Responsible AI evaluation remain in [the implementation plan](IMPLEMENTATION_PLAN.md). The current interface is a local POC.

Google Workspace is the confirmed team sign-in provider. The planned shared application uses Cloud Run with IAP plus server-enforced roles and ownership; local ADC is cloud-client authentication and does not provide marketing-user login. A campaign-planner model selector is also planned, using evaluated, authorized choices with model-specific limits. Neither feature is implemented in the current interface. See the plan's [team access milestones](IMPLEMENTATION_PLAN.md#marketing-team-authentication-and-access) and [model selection milestones](IMPLEMENTATION_PLAN.md#user-selectable-campaign-models).

Historical campaign references already use eligible same-SKU cosine ranking. Advanced reranking and calibrated relevance thresholds are pending. Generated-banner cosine retrieval exists in the backend but is not enabled in this chat generation path.

The backend [access foundation](ACCESS_OWNERSHIP_DESIGN.md) now includes roles, private/team sharing, a signed IAP identity adapter, fresh BigQuery access checks and protected job status/output summaries/downloads. A default-disabled server-only Streamlit bridge can forward the single IAP assertion to prestarted submission/history/read hosts, restore and page authoritative history, submit reviewed preferences, refresh job status, show safe output/download descriptors and defer verified artifact reads until a download click. The current app entry point installs no such runtime, so the personal-account local flow is unchanged. Shared sign-in and serving bootstrap remain later integration steps.

The [workflow dataset and eleven tables](WORKFLOW_DATA_DESIGN.md) are created and verified in BigQuery. Default local conversation history and previews still use session memory. Durable submission/history services and their UI bridge are implemented behind disabled defaults, with no runtime installed or shared writes activated.

Pure request/job/idempotency validators are now implemented for the future persistence layer. They are not yet invoked by the chat service, so they do not change the current UI or provide durable deduplication or authorization.

Output/artifact validators now protect the separate server download method. It resolves complete BigQuery parents/output, verifies pinned GCS bytes and rechecks current access and manifest before returning a file. It is not connected to the chat; the current session-memory downloads and local flow remain unchanged.

Pure attempt/stage-event validators now check durable progress and duplicate-event retries. They share the existing four stage names with the local adapter; they are not connected to chat persistence or writers. Visible progress remains session-only.

Model-invocation validators now check pinned profiles, dispatch/outcome metadata and nullable usage/Decimal estimates for future persistence. They do not invoke models or collect costs; the chat remains connected to the current synchronous adapter and session memory.

Evidence snapshot validators now check source projections, provenance/hashes, pinned image/review/vector metadata and output citations. They remain an offline contract foundation; fresh-reader projection, current eligibility/GCS checks and durable publication are pending. No snapshot rows are written and local chat behavior is unchanged. Audit row/replay validators are also implemented offline; no audit events are written by the chat. Workspace and membership row/mutation validators and access-reader integration are implemented offline. ADMIN membership proposal checks use captured snapshots and do not assign users or prove current authorization. Conversation row/creation/sharing and structural archival contracts are implemented offline and used by the reader. Archive write authorization remains disabled pending an explicit lifecycle/audit action. The sharing coordinator contract now validates accepted commands, fence/conversation/audit proposals and receipt/unknown outcomes offline; it neither executes queries nor changes visibility. The parameterized sharing SQL/query contract is also implemented offline with 31 tests; it constructs SDK settings and validates returned records without submitting queries. The server-only sharing executor and runtime intent sealer now connect verified headers/current access and original-job recovery with 33 offline tests. Execution is disabled by default; the local chat does not call it. The separate [native sharing probe](SHARING_INTEGRATION.md) passed 11 fixture cases and removed its owned dataset. The separate [read-only receipt recovery method](SHARING_RECEIPT_RECOVERY.md) now passes 181 impacted tests and six focused native cases; it is not called by the chat. The [trusted retry adapter](SHARING_RETRY_EXECUTOR.md) now passes 252 affected offline tests; it is not called by the chat. The separate [native retry probe](SHARING_RETRY_INTEGRATION.md) now passes ten cases and 266 affected offline tests with owned cleanup. The [key/version retention contract](SHARING_SIGNING_RETENTION.md) now passes 304 affected tests. The [pinned loader](SHARING_SECRET_LOADER.md) now passes 335 affected offline tests. The [publication/refresh contract](SHARING_PUBLICATION_REFRESH.md) now passes 379 affected tests. [4.1 storage/read design](SHARING_PUBLICATION_STORAGE.md) is complete offline with 443 affected tests. 4.2 (Critical / Must have) is complete within implementation/native fixture scope: [resource setup, codecs and authoritative reads](SHARING_PUBLICATION_READER.md), the [fenced writer](SHARING_PUBLICATION_WRITER.md) and [controlled bootstrap](SHARING_SIGNING_BOOTSTRAP.md) are implemented and verified; 546 affected offline tests and 13 native bootstrap cases pass, and real serving bootstrap/shared activation remain deferred; runtime integration/writers follow under their fixed IDs before UI persistence. Passing fixture tests does not activate sharing or SSO. No local or serving users were bootstrapped.

## Verification

The UI uses Streamlit AppTest for connected and unavailable catalogs, clarification, unsupported requests, successful previews, safe failures, new conversations and generation deduplication on reruns. Run the full suite with:

```sh
.venv/bin/python -m unittest discover -s tests
```

A real cloud workflow can be verified using the application adapter:

```sh
.venv/bin/python -m src.application.campaign_chat --smoke-test
```

This smoke test reads cloud evidence and invokes the configured models to create one in-memory preview. An optional `--output-dir` writes a unique local review artifact; it does not load or approve cloud data.

The 2026-10-10 model-continuity validation passed five interpretation cases and a verified original-image preview. See [MODEL_CONTINUITY.md](MODEL_CONTINUITY.md) for the checks, repeatable command and remaining evaluation limits.

### Protected initial-turn intake (2026-10-16)

Durable mode now routes chat through `WorkflowUiRuntime.interpret` and the owned
submission worker. It never falls through to the local interpreter/generator.
Select a current product, market (US/CA/GB/AU), platform
(META/KLAVIYO_EMAIL/TIKTOK/PINTEREST) and audience explicitly. These are user
selections, not evidence of commercial or scientific approval. The transaction
checks active product identity/source and atomically stores request, planned
invocation, raw user text, selection authority and audits under the workspace fence.
After a confirmed commit, the protected service independently rereads execution
authority (including catalog and transcript checks) before returning its receipt.
Raw text retains the existing 30-day partition expiry.

This adapter supports an initial turn only. Existing conversation requests,
invocations, jobs, attempts, raw bodies or authority records block a new initial
capture. It does not invent missing history or certify a complete multi-turn flow.
The UI retains the exact message/selection/retry key for checking the original
submission; it does not automatically remint a key or retry model execution.
A committed receipt confirms storage, not generation, evaluation or approval.

`app.py` now calls the opt-in serving startup. To install intake/read hosts,
the deployment must supply `CI_DURABLE_WORKFLOW_UI_ENABLED=true`, an absolute
server-owned JSON manifest path in `CI_CAMPAIGN_SERVING_CONFIG`, and a stable
base64-encoded cursor signing key (at least 32 decoded bytes) through the trusted
secret environment variable `CI_CAMPAIGN_HISTORY_CURSOR_KEY_B64`.
Do not put the cursor key in source control or browser form inputs.
The manifest has exactly `iap_audience`, `workspace_id`, `conversation_id` and
`submission`; `submission` follows `SubmissionSettings` with explicit validated
interpreter/planner profiles, renderer pins and operation/query limits. These pins
are configuration, not prompt activation or model eligibility. Use the actual
service audience and an existing authorized private conversation; startup does not
create membership/conversations or infer roles from ADC.

Startup prestarts all three hosts before installing the UI runtime, closes them on
failure, registers shutdown cleanup, rejects configuration/key changes in the same
process and requires explicit restart after retirement. It installs intake/history/
read dependencies only. The registered model worker, evidence service, cold-process
submission recovery and actual Cloud Run deployment remain unconnected by this
startup path. Generated questions/campaigns remain withheld. Existing job history
reconnect is separate from recovery of a retained initial-turn submission.

### Multi-turn persistence update (2026-10-16)

The protected submission worker now captures prior messages/responses from BigQuery.
Only terminal, complete, unexpired interpreter history is accepted. Each user message
and provider response keeps its original partition age; the next authority stores
ordered hashes rather than copying older raw text. Missing or uncertain prior outcomes
block a new turn. After a confirmed save, Continue conversation clears the UI's retained
submission input; the backend still checks history before accepting another request.
Provider responses remain private and are not displayed by this persistence path.

The server manifest may additionally contain interpreter_runtime with exactly enabled,
policy and targets. policy has policy_id/version/policy_sha256. targets lists exact
[prompt_key, Vertex prompt ID, version ID] entries for campaign-evidence-intent and
campaign-evidence-questions, in that order. This is deployment-owned configuration,
never browser input or approval. With it, the submission worker composes the registered
interpreter processor after persistence. DRAFT/unverified policy or ineligible prompts
withhold execution. Free-form audiences outside the registered NEW/RETURNING enum
also hold before a model call. No automatic audience conversion or prompt mutation.

This update does not deploy the marketing app, authenticate live browser access,
activate prompts, verify independent policy approval or release generated questions.
Same-worker recovery retains the original operation; cold process reconstruction and
fully released conversational question flow remain incomplete.

Cold reconnect update: a newly composed registered processor may verify a stored
terminal outcome (including exact retained successful response body) and reread current
execution authority, without another provider call. This reports stored-state evidence,
not a recovered native transaction receipt or question-release approval. UNKNOWN and
DISPATCHED outcomes stay held. Live browser/IAP reconnect verification is still pending.
