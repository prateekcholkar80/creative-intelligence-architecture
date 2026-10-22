# Planner prompt registration

Batch 13 — **High / Must have / shared pilot — COMPLETE on 2026-10-13**.
The real IAP-authenticated ADMIN registered the exact Vertex planner version as
DRAFT. Independent verification matched the full BigQuery current/history state,
canonical Vertex snapshot/parity and successful original native job SQL/config.
Priority 5 is **16/16 implementation milestones complete within documented scope**;
existing Batches 14–16 native verification and reviewed quality gates still remain
before activation. Historical checkpoints below retain their original scope/status.

## Verified live registration — 2026-10-13

- Vertex prompt `8077808464061005824`, exact version `1`, in `asia-south1`.
- `ci_application.prompt_registry`: DRAFT, revision one; append-only history: one row.
- Snapshot: `78b7c867f499c0aab130fff55914ecce89667ac9e7190026dd9399183108340c`.
- Record digest: `02959bbc5feff4ff1f2882b0edec4d2d08fb5e1e107781e432f763c0ad6cb947`.
- Original job: `prompt_register_de372de65e1778fc05a6a1e3d47aa42de4b4c3d6afe726c201a549ad3bdb1ff0`,
  DONE with no error, created by the expected Cloud Run runtime service account.
- The runtime returned an APPLIED receipt after fresh authorized current/history
  verification. Independent native SQL/config and complete authoritative contract
  checks matched that receipt; no second mutation was submitted. Local ADC cannot
  access the runtime's anonymous result dataset, so independent checks used the
  successful exact transaction and full authoritative rows rather than claiming
  a local executor receipt recovery.
- No ACTIVE planner, promotion, model invocation or campaign generation occurred.

See [independent verification](../data/generated/prompt_registry_diagnostics/batch13_draft_registration_verified_20261013.json).
After verification, configuration-only Ready revision `ci-prompt-registrar-00007-gcc`
closed `CI_PROMPT_REGISTRATION_WRITES_ENABLED`. Bootstrap remains disabled.
Native comparison verified unchanged workspace, immutable image, IAP, runtime,
maximum one instance and concurrency 16. Existing IAM/membership rows are retained;
no registry row or prompt lifecycle was changed by this configuration update. See
[completed read-only console pins](../data/generated/prompt_registry_diagnostics/batch13_completed_console_service_20261013.json).
The last code checkpoint passed 105 affected tests; this completion changed only
operational configuration, verification evidence and documentation. No code tests
were repeated for documentation-only changes.

The fixed next work is **Critical / Must have** native transaction/race/recovery
verification within existing Batches 14–16, in 2–3 file groups with owned fixtures
and verified cleanup. Then **Priority 6 — High / Must have** establishes the labeled
benchmark, measured grounding/retrieval/schema/banner quality, reviewed policies
and agreed promotion thresholds. DRAFT registration alone grants no activation.

## Read-only review

Run from the repository root:

```sh
.venv/bin/python -m src.application.planner_prompt_registration
```

The default selection is Vertex prompt `8077808464061005824`, version `1`, in
`asia-south1`. `--prompt-id` and `--version-id` require explicit numeric identities;
there is no latest/current discovery. The command reads the exact Vertex version,
checks parity against the existing planner contract, and reads exact/ACTIVE
BigQuery registry state. It prints the complete canonical template, settings,
safety controls, output schema, content hashes and nine-field diff. A passed parity
check establishes migration compatibility, not business approval or authorization.

An optional `--review-file PATH` writes an inspection export. The command never
reads that file back. It cannot supply runtime context, authoritative eligibility,
an approved command or a recovery receipt. An unauthenticated review has
`registration_authorized: false` and no proposed lifecycle status.

## Authenticated DRAFT import

Prerequisites are a real protected service with an independently verified
`CI_IAP_EXPECTED_AUDIENCE`, an ACTIVE BigQuery workspace, and a current ACTIVE
ADMIN membership bound to the registrar's verified IAP issuer/subject. The audience
is trusted operator configuration, never browser-supplied input. Deployment,
IAM and controlled serving bootstrap require their existing explicit authorization.
Personal ADC supplies cloud credentials only. Do not create a service-account key,
fabricate an assertion, infer a role from email, or manually insert membership rows.

After those prerequisites are established, run:

```sh
.venv/bin/python -m src.application.planner_prompt_registration \
  --apply --workspace-id VERIFIED_WORKSPACE_ID
```

The operator enters a real assertion through hidden terminal input. The command
verifies it, reads current MANAGE_MODELS access before and after preview preparation,
and derives the proposal actor from that identity. Review the printed canonical
content and enter its exact `preview_sha256`. A separate fresh assertion is then
verified for execution. Do not paste assertions into chat, command arguments,
source files or review exports. Review time is outside the provider deadline;
each preparation/execution operation has its own bounded budget (default 120s).

Execution reissues the preview, checks hash/actor/state, and uses the existing
deterministic native-job executor. The initial transaction atomically inserts
revision-one DRAFT into `ci_application.prompt_registry` and its append-only
`prompt_registry_history`; it does not update or delete unrelated rows. Success
requires an exact APPLIED/REUSED native receipt and a fresh decoded BigQuery
current/history read matching the approved proposal. This does not activate a
prompt, authorize claims or invoke a model.

Missing configuration, denied/revoked access, hash/parity drift, partial/conflicting
state and UNKNOWN outcomes fail closed. The command prints bounded safe error codes
without credentials or raw provider error bodies. It never automatically retries
an uncertain mutation. The underlying service can recover using the same server-held
approved command and original job. This CLI does not persist that command across
restart. A fresh inspection can show stored state, but is not a native receipt or
permission to recreate a proposal after uncertainty.

## Verification on 2026-10-12

- Exact live Vertex inspection passed parity: model `gemini-3.5-flash`, snapshot
  `78b7c867f499c0aab130fff55914ecce89667ac9e7190026dd9399183108340c`.
- Fresh BigQuery exact/ACTIVE reads returned UNREGISTERED/no ACTIVE planner.
- Prerequisite query `2b39f9da-9d2e-45f6-a965-a2d290f67c0d` returned zero
  workspaces, memberships and registry rows, and zero history rows in the
  changed_at >= 1970-01-01 range. The first count query omitted the mandatory
  partition filter and failed; the corrected read passed.
- IAP audience is absent. Apply returns `PROMPT_REGISTRATION_IAP_NOT_CONFIGURED`
  before cloud composition or assertion input. Live registration cannot pass the
  current controls, so no DRAFT write was attempted.
- All 338 affected prompt, identity/access, generation, UI/workflow tests pass,
  including 17 new operator tests. Python syntax checks pass.
- No serving rows, IAM, approvals, Vertex resources or default activation changed;
  no model invocation occurred.

The review and verification exports are under
`data/generated/prompt_registry_diagnostics/batch13_*_20261012.json` and remain
inspection evidence only.

## Historical resumption and setup checkpoints

On resuming Batch 13 on 2026-10-12, the exact live review again passed parity and
returned `UNREGISTERED`, no current revision and no ACTIVE planner. The snapshot
hash is unchanged. All **28 focused operator/registration-command tests passed**.
The review export is
`data/generated/prompt_registry_diagnostics/batch13_review_20261012_resume.json`.
`CI_IAP_EXPECTED_AUDIENCE` is still absent. The Cloud Run service inventory request
returned `SERVICE_DISABLED` for `run.googleapis.com`; this does not prove that
the service inventory is empty. No browser is available to the assistant for
the one-time Console setup. No live registration, bootstrap, IAM or API enabling
was performed during this inspection.

The user selected guided Console setup for the personal Gmail pilot. This is
within the existing Batch 13 dependency and Priority 12 SSO plan; it adds no batch.

### Guided setup progress

The user confirmed OAuth branding, **External** audience and saving the custom
Web application client's IAP redirect URI. Its public client ID is
`1042059373096-3g5hk97mvb07t1boq82fm7s26lc6qs61.apps.googleusercontent.com`.
The shown branding screen has **Testing** publishing status. These are user/UI
observations, not proof of IAP configuration or successful sign-in.

On 2026-10-12, `gcloud services enable run.googleapis.com iap.googleapis.com`
succeeded for the fixed project. A subsequent enabled-services read verified
both APIs. The native `asia-south1` Cloud Run service inventory then completed
successfully and returned no services. No container/service, IAM grants, workspace
bootstrap, registration or activation has been created by this API-enabling step.
The subsequent deployment checkpoint below supersedes the empty-service state.
No client secret or IAP assertion belongs in repository files, logs or chat.

### Protected console deployment — 2026-10-13

The empty IAP resource screen showed that OAuth attachment was requested too
early. The new single-operator console in `src/ui/prompt_registration_console.py`
now reuses the existing Batch 13 command. It verifies signed IAP identity and exact
HTTPS origin, binds one server-held review to principal/workspace for five minutes,
requires fresh ADMIN access and exact hash confirmation, and retains the same
command after uncertain submission. It never displays/exports assertions, creates
memberships, promotes prompts or generates campaigns. Its thin Streamlit entry
point preserves command classes across reruns. Writes are disabled by default.

All **63 affected tests passed**, including **17 console tests**, actual ES256
signature/tampering and Streamlit rerun/default-denial tests. Syntax, dependency
and isolated-source import checks passed. Initial fixture/path errors were corrected
before packaging. The unsupported upload-inspection CLI flag was replaced with an
isolated source directory; SQL assets were added before the only submitted build.
No source/business exports, credentials or local app state were uploaded.

Cloud Build and Artifact Registry APIs were enabled. Created resources:

- `ci-operator-images`: regional Docker repository.
- `ci-prompt-builds-1042059373096`: regional source-only bucket, with verified
  uniform access and public access prevention.
- `ci-prompt-console-build`: dedicated builder, with repository writer access,
  source-bucket object viewer access and project log-writer permission only.
- `ci-prompt-registrar`: dedicated keyless runtime account with no project roles.
- Cloud Run `ci-prompt-registrar`, in `asia-south1`, deployed with IAP.

Native build `c9397d39-a72f-4d6e-a86b-ad51b4bef21c` succeeded. The source manifest
hash is `a8a4e18fad679141476e819d1e42b8a1dec8021baf9e039003cd78eb86816d07`.
The deployed immutable image digest is
`sha256:7d7d992529812773d7f4a0fcc6e77ff1f0a04c369d423c322b8d23a4947d2479`.
Ready revision: `ci-prompt-registrar-00001-d4k`.
Canonical URL: `https://ci-prompt-registrar-1042059373096.asia-south1.run.app`.

Native metadata verified IAP enabled, service/revision maximum one instance,
zero minimum instances, concurrency one, 1 CPU, 1 GiB memory and 300-second timeout.
The only service-level invoker is the Google IAP service agent; there is no public
invoker binding. A credential-free request returned HTTP 302 to sign-in. The
explicit trusted IAP audience is
`/projects/1042059373096/locations/asia-south1/services/ci-prompt-registrar`.
The canonical origin is one of the service's verified URL aliases, even though
`status.url` retains the older hashed alias.

This deployment is **identity-only**: no workspace setting, registration writes
false and no runtime data permissions. All **53 existing BigQuery table/view
metadata records and five datasets** matched the prior physical-table checkpoint.
No business rows, product data, approvals, prompt lifecycle or models changed.
New build/deployment resources and their scoped IAM grants did change; no account
keys or OAuth secrets were created/stored by the assistant.

Gmail sign-in, Google-signed deployed assertion/headers, WebSocket/reconnect behavior,
controlled ADMIN bootstrap, runtime registration permissions and live DRAFT write
remain unverified/pending. Browser access is unavailable to the assistant, so the
user must configure the secret directly in the Console. The console provides
cooperative provider budgets, not the Priority 4 hard-worker runtime guarantee.
It has no durable review recovery across lost sessions/processes and does not
complete general marketing UI SSO or activate shared workflows.

See [console deployment notes](../deploy/prompt-registration/README.md),
[deployment verification](../data/generated/prompt_registry_diagnostics/batch13_console_deployment_20261013.json),
[source manifest](../data/generated/prompt_registry_diagnostics/batch13_console_source_20261013_v2.json),
[native service metadata](../data/generated/prompt_registry_diagnostics/batch13_console_service_20261013.json)
and [serving metadata](../data/generated/prompt_registry_diagnostics/batch13_console_serving_metadata_20261013.json).

On 2026-10-13, after the user reported OAuth attachment, a credential-free request
returned HTTP 302 to `accounts.google.com` with the exact custom client ID
`1042059373096-3g5hk97mvb07t1boq82fm7s26lc6qs61.apps.googleusercontent.com`.
The service-scoped IAP policy was updated and reread: its single explicit accessor
is `user:Prateek.Cholkar@gmail.com` with `roles/iap.httpsResourceAccessor`.
This grants entry through IAP, not an application ADMIN role. Native service
configuration still has IAP enabled, registration writes false and no workspace
setting. OAuth secrets were not displayed or stored. Real browser sign-in and
the application's Google-signed identity verification were still pending at that
checkpoint.

The user's subsequent browser screenshot showed `Google identity verified` and
the expected bootstrap-pending notice, verifying the deployed application identity
check through Streamlit. The JSON identity display failed because its dynamic
asset request returned HTTP 429 (native log timestamp
`2026-10-13T07:41:35.202559Z`). The exact `Json.D1YSMpgY.js` asset exists in the
pinned Streamlit package. One instance/concurrency one leaves no request slot
for assets while Streamlit's WebSocket is active. This diagnosis follows the
[Cloud Run WebSocket request model](https://docs.cloud.google.com/run/docs/triggering/websockets).
The configuration-only fix deployed Ready revision `ci-prompt-registrar-00002-t7k`
with concurrency **16** and the same image digest. Native configuration/IAM checks
verified 100% traffic, maximum one instance, IAP enabled, only IAP-agent invocation,
writes false and no workspace setting. No code, business rows, roles or prompts
changed in this fix. Post-fix authenticated asset rendering and reconnect remain
to be checked; a successful initial identity check does not prove reconnect or
full shared SSO behavior.

1. **High / Must have — Batch 13:** refresh the canonical service URL and verify
   the identity details render without the dynamic-import error. Custom OAuth,
   the selected Gmail's service-level IAP grant and initial application identity
   check are verified; post-fix rendering/reconnect still require verification.
   Google documents the required one-time setup; see
   [Cloud Run IAP configuration](https://docs.cloud.google.com/run/docs/securing/identity-aware-proxy-cloud-run).
2. **High / Must have — Batch 13:** use the existing controlled bootstrap with
   an explicitly approved workspace/owner/ADMIN manifest bound to the real verified
   IAP subject. Prepare the authenticated exact-version review, confirm its hash,
   and verify the live DRAFT current/history and original-job receipt.
3. **Critical / Must have — existing Batches 14–16:** complete isolated native
   transaction/race/expiry/rollback/recovery verification. Implementation and all
   seven physical tables are complete; see [resource evidence](PROMPT_PHYSICAL_TABLES.md).
4. **High / Must have — Priority 6 and existing release gates:** establish reviewed
   benchmark/policy inputs and measured evaluation before promotion. No activation
   is implied by identity setup or DRAFT registration.

See [the implementation plan](IMPLEMENTATION_PLAN.md) for the unchanged overall scope.

### Controlled bootstrap form — 2026-10-13

The user confirmed identity JSON renders after the concurrency fix and selected
`ci-poc-marketing`. The reported subject's deterministic principal matches the
displayed principal; this is a consistency check, not authentication. The form
always rechecks actual signed request identity.

In one three-code-file group, added `workspace_bootstrap_console.py`, focused tests
and optional integration in `prompt_registration_console.py`. The form is disabled
by default. An installed exact-owner ADMIN-only manifest, at most five minutes
long, gates explicit workspace confirmation. It reuses the existing atomic
workspace/membership/audit bootstrap and stable slot-one job. A consumed entry
remains recovery-only, including after provider errors. Recovery reconstructs the
original job without registering an issued write plan or submitting. No automatic
renewal, retry-slot escalation or cloud-derived ADMIN fallback exists.

All **102 affected offline tests passed**, including **10 new bootstrap-console
tests**; syntax/dependencies pass. Live read-only schema preflight verified all
eleven workflow tables empty. Native Cloud Build succeeded using the existing
dedicated builder and isolated allowlisted source; see
[bootstrap deployment notes](../deploy/prompt-registration/README.md).
Runtime IAM now permits query jobs, ci_application reads and data updates on only
the three bootstrap tables. Existing dataset grants were preserved. A dataset IAM
command required allowlisting and was replaced with the supported etag-bound
access-entry update. The unused reader custom-role definition remains unbound.
Native verification found the exact three writer bindings and all eleven tables
still empty. No business-row, product, prompt or model writes occurred in setup.
Ready revision `ci-prompt-registrar-00003-kxz` now serves the tested immutable
image with the exact-owner ADMIN-only manifest for `ci-poc-marketing`, valid from
`2026-10-13T08:10:02.336067+00:00` to `2026-10-13T08:15:02.336067+00:00`
(expires 13:45:02 IST). Native configuration verified this manifest, IAP enabled,
concurrency 16, maximum one instance, no registration workspace and writes false.
The real signed browser confirmation and authoritative row/audit verification
remain pending. Expiry allows only read recovery, not fresh bootstrap. DRAFT
registration remains pending. See
[native service pins](../data/generated/prompt_registry_diagnostics/batch13_bootstrap_console_service_20261013.json)
and [runtime permission verification](../data/generated/prompt_registry_diagnostics/batch13_bootstrap_runtime_permissions_20261013.json).

The first window expired without submission. On the user's disabled-button report,
a fresh native check returned NotFound for all three reserved bootstrap job IDs
and zero workspace-scoped rows across all eleven workflow tables. Only after that
positive absence check, installed a second exact-owner ADMIN-only manifest with
the same workspace, command and slot-one job identity. Window two is valid from
`2026-10-13T10:46:47.149145+00:00` to `2026-10-13T10:51:47.149145+00:00`
(16:16:47–16:21:47 IST). Ready revision `ci-prompt-registrar-00004-qmg` serves 100%.
Native comparison verified the exact new manifest and unchanged image, runtime
identity, IAP, instance cap, concurrency and other environment settings; registration
writes remain false. No original mutation was resubmitted and no business rows
were written by this window update. Browser confirmation and authoritative receipt
verification remain pending. This was an explicit operator configuration step,
not automatic renewal or recovery proof from a missing receipt. See
[absence verification](../data/generated/prompt_registry_diagnostics/batch13_bootstrap_window_precheck_20261013.json)
and [window-two native pins](../data/generated/prompt_registry_diagnostics/batch13_bootstrap_console_service_20261013_window2.json).

### Batch 13 bootstrap feedback — High / Must have — 2026-10-13

After the user reported no message following a click, a fresh native check at
10:55:36 UTC found all three reserved jobs absent and no workspace, owner membership
or BOOTSTRAP_WORKSPACE audit. The second window had expired at 16:21:47 IST.
The screenshot proves the disabled button; it does not prove that an earlier click
reached the server. The bounded error-log lookup found no matching provider/HTTP
failure, so the earlier silent-click cause remains unproven. See
[authoritative absence check](../data/generated/prompt_registry_diagnostics/batch13_bootstrap_click_check_20261013.json).

A two-code-file fix now explains expired/not-yet-open windows and consumed entries,
shows progress during explicit actions, and retains minimal validated receipts or
safe error codes across reruns. Receipt display follows fresh identity/owner/config
checks and is labeled historical, not current access. Safe action/phase/status logs
omit assertions, credentials and exception bodies. No automatic renewal, retry,
new write path or role assignment is added. All **105 affected tests pass**,
including expiry feedback, uncertainty/error retention, committed receipt reruns,
owner-change denial and private-body log exclusion. Compilation/dependencies pass.
Native build `2cc78e65-0e3d-4618-85f7-c112d9ae15bf` succeeded with image digest
`sha256:81de71c9af428d67b3aeb523fbdbd604bc1a932dd54967f14d819e4740b9b75b`.
Ready revision `ci-prompt-registrar-00005-8vs` serves 100%. Immediately before
deployment, all three reserved jobs were absent and all eleven workspace-scoped
table counts were zero. The user confirmed availability, and window three was
installed for 17:49:56–17:54:56 IST on 2026-10-13, keeping the same workspace,
owner, ADMIN role, command and original job identity. Native comparison verified
the exact image/manifest, unchanged other environment/runtime identity, IAP,
concurrency 16 and maximum one instance. Registration remains disabled. See
[source pins](../data/generated/prompt_registry_diagnostics/batch13_bootstrap_feedback_source_20261013.json),
[immediate absence check](../data/generated/prompt_registry_diagnostics/batch13_feedback_window_precheck_20261013.json)
and [native deployment](../data/generated/prompt_registry_diagnostics/batch13_bootstrap_feedback_service_20261013.json).
Signed confirmation and authoritative commit verification remain pending.
Batch 13 remains open;
Priority 5 stays 15/16. Next is coordinated signed confirmation in a valid window,
authoritative workspace/membership/audit verification, then exact-version DRAFT
registration and native receipt/current-history verification.

### Batch 13 bootstrap committed — High / Must have — 2026-10-13

The signed browser action returned COMMITTED/APPLIED. Independent read-only
verification matched the original slot-one native job's project, region, runtime
creator, exact SQL and all configuration/manifest parameters. The parent is DONE
with no error; that exact successful script necessarily reaches COMMIT. A fresh
bounded transaction matched the full workspace, ADMIN membership and audit rows
to the expected manifest, with one row in each and zero rows in the other eight
workflow tables. Workspace/membership are ACTIVE revision one; role is ADMIN only
with BOOTSTRAP provenance. See
[authoritative bootstrap verification](../data/generated/prompt_registry_diagnostics/batch13_workspace_bootstrap_verified_20261013.json).

The local operator's receipt-result read returned 403 because BigQuery denies
access to another user's anonymous result dataset. This did not establish rollback;
the runtime UI had verified its own receipt. An attempted diagnostic child listing
also rejected combining allUsers and parentJobId. No mutation was replayed or IAM
widened for either read. Independent verification used exact successful transaction
metadata and full authoritative rows, not the conservative executor's UNKNOWN
receipt outcome. Initial diagnostic assertions/import errors were corrected locally.

The remaining existing Batch 13 path is scoped runtime permissions, disabling the
completed bootstrap form, fresh IAP/ADMIN exact-version review, explicit confirmation
of that review's preview hash, DRAFT registration and original-job/current/history
verification. The read-only exact version review passed parity again with unchanged
snapshot hash `78b7c867f499c0aab130fff55914ecce89667ac9e7190026dd9399183108340c`,
UNREGISTERED and no ACTIVE planner. No model call or prompt registration occurred.

The dedicated runtime now has updateData-only table bindings on `prompt_registry`
and `prompt_registry_history`. Custom role `ciOperatorPromptVersionReader` contains
only `aiplatform.datasetVersions.get`; its verified project binding permits
version reads, without model invocation or Vertex writes. Vertex IAM scope is the
project; exact prompt/version restriction is enforced by the application. The
[Google API reference](https://docs.cloud.google.com/vertex-ai/docs/reference/rpc/google.cloud.aiplatform.v1)
documents this read permission. An initial binding raced pending role creation
and returned "does not exist in the resource's hierarchy"; after creation completed,
the binding succeeded and native readback verified the final grants. See
[runtime permissions](../data/generated/prompt_registry_diagnostics/batch13_registration_runtime_permissions_20261013.json).

Configuration-only Ready revision `ci-prompt-registrar-00006-4c9` serves 100% with
the same immutable feedback image. Native comparison verified bootstrap disabled,
bootstrap manifest removed, `ci-poc-marketing` installed and initial DRAFT writes
enabled, with other environment/IAP/runtime/instance/concurrency pins unchanged.
See [DRAFT console native pins](../data/generated/prompt_registry_diagnostics/batch13_draft_console_service_20261013.json).
No fresh review is preapproved by this switch. The signed ADMIN must prepare an
exact-version review, inspect it and enter its `preview_sha256` within five minutes.
Snapshot hash is not the confirmation field. Current/history/original-job
verification is required after registration; Batch 13 is still incomplete.

The user subsequently obtained the real authenticated runtime review: authorized,
UNREGISTERED, expected exact snapshot/model/profile and proposed DRAFT, with
mutation_performed false. This verifies the deployed IAP/ADMIN/Vertex/BigQuery
review composition. The first confirmation returned CONSOLE_REVIEW_NOT_CONFIRMED,
which rejects a nonmatching preview hash before operator registration/cloud write.
A fresh independent exact read still returned UNREGISTERED. The user was directed
to copy only the displayed preview hash, not the snapshot hash, within the existing
five-minute review. This is not a passed registration or an approved prompt;
the signed successful transaction and authoritative DRAFT checks remain pending.
