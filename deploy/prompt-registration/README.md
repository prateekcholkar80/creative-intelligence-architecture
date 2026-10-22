# Batch 13 single-operator registration console

**Current result — 2026-10-13:** Batch 13's real IAP/ADMIN DRAFT registration is
complete and independently verified against the exact Vertex snapshot, full
BigQuery current/history rows and successful original native job SQL/config.
Ready revision `ci-prompt-registrar-00007-gcc` now serves 100% with registration
writes false and bootstrap disabled. Native comparison verifies unchanged image,
workspace, IAP, runtime and capacity; existing IAM/ADMIN membership is preserved.
The prompt remains DRAFT, not ACTIVE. Marketing generation/shared SSO is not
activated. Earlier deployment sections below describe historical checkpoints.
See [completion evidence](../../docs/PROMPT_REGISTRATION.md#verified-live-registration--2026-10-13).

**High / Must have — existing Batch 13 identity/registration prerequisite.**
This is a separate operator interface, not the marketing chatbot or a shared
workflow activation. It reuses the exact-version operator and existing fenced
registration executor. Real IAP verification, exact HTTPS origin, fresh ADMIN
access, exact human hash confirmation and explicit server write enablement gate
registration. Only the existing initial `DRAFT` import is supported.

The first deployment is identity-only: no workspace setting, no data permissions
on its dedicated runtime service account and registration writes disabled.
It displays only the user's verified subject/principal and audience, allowing
explicit future bootstrap pins. No IAP JWT is displayed, exported or put in app
session state. A stale WebSocket assertion denies; reload to obtain fresh identity.

## Trusted deployment settings

- `CI_PROMPT_REGISTRATION_CONSOLE_ENABLED=true`: explicit console opt-in.
- `CI_IAP_EXPECTED_AUDIENCE`: independently verified exact Cloud Run resource,
  `/projects/1042059373096/locations/asia-south1/services/ci-prompt-registrar`.
- `CI_PROMPT_REGISTRATION_ORIGIN`: independently verified exact HTTPS service URL,
  with no trailing slash, query or path. Other origins deny.
- `CI_PROMPT_REGISTRATION_WORKSPACE_ID`: omit until controlled ACTIVE workspace/
  ADMIN bootstrap is verified. This is server configuration, not a browser choice.
- `CI_PROMPT_REGISTRATION_WRITES_ENABLED`: omit/false until an authorized live
  registration is ready. Default is false, including unrecognized values.
- `CI_WORKSPACE_BOOTSTRAP_CONSOLE_ENABLED=true`: enable the separate initial
  bootstrap form while registration writes stay disabled and its workspace setting
  remains absent. Missing or unrecognized enablement composes no cloud clients.
- `CI_WORKSPACE_BOOTSTRAP_MANIFEST_JSON`: trusted JSON containing only
  `workspace_id`, `owner_subject`, `roles`, `manifest_id`, `created_at`, `expires_at`.
  Timestamps must be timezone-aware; validity is at most five minutes. This UI
  accepts only the explicit role array `["ADMIN"]`. No owner or role defaults.

For identity-only composition, the Cloud Run runtime service account needs no
BigQuery, GCS or Vertex role. Later scoped registration permissions are a separate
step; infrastructure access never supplies application ADMIN membership. Never
install OAuth secrets into the image or browser state; configure them in IAP.

## Build and verification

Build from the repository root with the allowlisted context:

```sh
gcloud builds submit . --project=project-661d501a-e709-4276-a26 --region=asia-south1 --config=deploy/prompt-registration/cloudbuild.yaml --ignore-file=deploy/prompt-registration/.gcloudignore --substitutions=_TAG=REVIEWED_SOURCE_TAG --gcs-source-staging-dir=gs://ci-prompt-builds-1042059373096/source --service-account=projects/project-661d501a-e709-4276-a26/serviceAccounts/ci-prompt-console-build@project-661d501a-e709-4276-a26.iam.gserviceaccount.com
```

The container copies source, SQL templates, JSON settings and the thin entry point only. Direct
dependency versions match the verified local environment; the base image and
transitive dependencies are not locked. Resolve and retain the built image digest
for deployment. Before broader release, pin the base digest/transitive lock and
test reproducible builds. No production CI/CD claim follows from this recipe.

All **63 affected offline tests** passed on 2026-10-13, including **17 console
tests**, actual ES256 signature/tampering checks and Streamlit rerun/default-denial
cases. Syntax and dependency checks passed. Local Docker is unavailable; the
native Cloud Build/deployment result is recorded separately in
[registration progress](../../docs/PROMPT_REGISTRATION.md).

## Current DRAFT review configuration — High / Must have

Real signed bootstrap is now independently verified in BigQuery: one ACTIVE
workspace, one ADMIN-only membership and one audit for `ci-poc-marketing`.
Ready revision `ci-prompt-registrar-00006-4c9` uses the same verified feedback image,
with bootstrap disabled/manifest removed, workspace configured and initial DRAFT
registration writes enabled. IAP, runtime identity, maximum one instance and
concurrency 16 are unchanged. Runtime registry writes are updateData-only on its
two tables. Project Vertex role contains only `aiplatform.datasetVersions.get`;
application code pins the exact planner version. There is no model dispatch or
Vertex prompt-write grant. Native role/table/environment checks pass.

Refresh, prepare a fresh exact-version review, inspect it and enter the displayed
`preview_sha256` into the confirmation field within five minutes. It is distinct
from `snapshot_sha256`. Bootstrap does not approve that review. Registration
imports DRAFT only; it does not activate generation. Batch 13 stays pending until
the signed registration and exact current/history/original-job verification succeed.
Local ADC cannot read another principal's anonymous query-result dataset; use
runtime receipt recovery or independent exact successful job/authoritative-row
verification rather than broadening access or resubmitting a mutation.

## Current native deployment

On 2026-10-13 one Cloud Build job succeeded:
`c9397d39-a72f-4d6e-a86b-ad51b4bef21c`. Its image digest
`sha256:7d7d992529812773d7f4a0fcc6e77ff1f0a04c369d423c322b8d23a4947d2479`
is deployed to `ci-prompt-registrar` in asia-south1. The service is Ready, IAP is
enabled and registration writes remain false with no workspace setting. Both
service/revision max instances are one; min instances zero. Concurrency one,
1 CPU, 1 GiB memory, 300-second request timeout. Its only service-level invoker
is the IAP service agent. Credential-free access returned HTTP 302.

Canonical URL: `https://ci-prompt-registrar-1042059373096.asia-south1.run.app`.
The older hashed URL alias in native status is also IAP-protected; the console
accepts only the configured canonical origin. The corresponding trusted
`STREAMLIT_BROWSER_SERVER_ADDRESS` and port 443 are installed for CORS/XSRF.
Custom OAuth attachment and the selected Gmail's service-scoped IAP grant were
verified on 2026-10-13. The user's browser screenshot then showed
`Google identity verified`, establishing a successful application identity check
through the deployed Streamlit connection. No runtime data role or workspace/ADMIN
bootstrap has been installed.

The screenshot also showed a failed dynamic import of `Json.D1YSMpgY.js`.
Native request logs recorded HTTP **429** for that exact asset at
`2026-10-13T07:41:35.202559Z` on the initial revision. The matching asset exists
in the pinned Streamlit 1.65.0 package. With one instance and concurrency one,
Streamlit's long-running WebSocket can occupy the sole request slot and prevent
asset delivery. Cloud Run counts WebSockets as HTTP requests; see
[WebSocket guidance](https://docs.cloud.google.com/run/docs/triggering/websockets).

The configuration-only fix raised concurrency to **16**. Ready revision
`ci-prompt-registrar-00002-t7k` now serves 100% of traffic using the same immutable
image digest. Native checks verified concurrency 16, service/revision maximum one,
IAP enabled, only the IAP service-agent invoker, writes false and no workspace.
No application code or data changed. This is request capacity for UI assets,
not a generation concurrency control. Post-fix authenticated browser rendering
and reconnect behavior still require verification. Retain concurrency greater
than one when deploying this Streamlit console.

The actual build used a dedicated keyless build account and an isolated regional
source bucket through `--service-account` and `--gcs-source-staging-dir`, preserving
the default compute account. Builder permissions are repository writer, source
object viewer and log writer only. The bucket has verified uniform access and
public access prevention. No product bucket or business rows were touched.

Server-memory review is limited to five minutes and one unchanged principal and
workspace. Uncertain submissions keep the same command, allowing explicit
same-session original-job recovery without automatic resubmission. Lost
session/process approval recovery is unsupported; consult authoritative native
job and registry state. The console does not implement promotion,
generation, durable approval storage, hard worker deadline enforcement or deployed
SSO/reconnect guarantees. Provider budgets remain cooperative and the existing
bounded transports are reused. All deployed behavior must be verified before use.

## Controlled bootstrap slice — 2026-10-13

The user confirmed post-fix identity rendering and chose `ci-poc-marketing`.
`src/ui/workspace_bootstrap_console.py` requires the installed manifest's exact
signed owner/audience, explicit workspace confirmation, disabled registration
and a live manifest window. It calls the existing `ProtectedManagementService`
slot-one transaction to create the workspace, ADMIN membership and audit together.
The UI consumes its entry before cloud composition. UNKNOWN, failure or a repeated
click cannot dispatch another write; the recovery button reconstructs and reads
the original native job. An expired installed manifest permits authenticated
recovery, not new submission. Lost sessions do not prove noncommit. No retry-slot
UI or automatic manifest renewal exists. Changed manifests fail closed.

All **102 affected offline tests passed**, including **10 new bootstrap-console
tests** covering confirmation/owner/expiry, malformed/uncertain outcomes,
read-only recovery and an actual Streamlit form/rerun case. Syntax/dependencies
pass. Live schema preflight verified all eleven workflow tables empty.

Native build `09a1da82-9c6e-423c-8345-52dd62d33628` succeeded from 222 allowlisted
files, source SHA-256
`de80c1915562d6c2cb2424ccf748872d0462e94f6cc8af7a11d86d2b66ba358d`.
Its immutable image digest is
`sha256:2f9a43c475ded09e8ed92770b40394e365e059a9a016a2dfdd0c20c53e160b72`.

The dedicated runtime now has project `roles/bigquery.jobUser`, dataset READER
on `ci_application`, and custom `ciOperatorWorkflowWriter` on only `workspaces`,
`memberships` and `audit_events`. The writer contains only
`bigquery.tables.updateData`, without table deletion/schema modification. Native
checks verified these exact table bindings and all eleven tables still empty.
IAM does not grant an application role. The dataset IAM binding attempt returned
`This feature requires allowlisting` and made no grant; the supported access-entry
API added READER using the existing etag and verified preservation of old entries.
The unused `ciOperatorWorkflowReader` custom role exists but has no binding.
No runtime Vertex/GCS permission or registry-table writer is installed.

Ready revision `ci-prompt-registrar-00003-kxz` now serves 100% of traffic using
that digest. Native configuration verified the exact manifest for the user's
subject and `ci-poc-marketing`, role ADMIN only, created at
`2026-10-13T08:10:02.336067+00:00`, expiring at
`2026-10-13T08:15:02.336067+00:00` (13:45:02 IST). IAP, concurrency 16, maximum one
instance, absent registration workspace and registration writes false were
verified. The bootstrap form requires a signed browser confirmation; expired
manifests deny new writes and retain read-only recovery. There is no automatic
renewal. The authenticated click and authoritative workspace/membership/audit
verification remain pending. No bootstrap success is implied by deployment.

The first manifest expired without any bootstrap submission. On the user's
disabled-button report, native job lookups returned NotFound for all three reserved
slots, and a fresh bounded query counted zero workspace rows in all eleven tables.
Only then, window two was installed with the same owner/roles/workspace/command
and job identity. It is valid 16:16:47–16:21:47 IST on 2026-10-13. Ready revision
`ci-prompt-registrar-00004-qmg` serves 100%; native comparison verified the exact
new manifest with other environment, image, identity, IAP and capacity unchanged.
This operator configuration update does not submit a bootstrap or renew itself.
Signed browser confirmation and authoritative commit verification remain pending.

## Bootstrap feedback fix — High / Must have

The form now states why creation is disabled (expired/not-yet-open approval window
or an already consumed entry), displays action progress and retains minimal verified
transaction receipts or safe errors across reruns. Retained display is owner/config
checked and labeled historical; it cannot grant access or replay a mutation. Action
logs exclude assertion/header/credential/exception bodies. All 105 affected tests
pass, including actual Streamlit expiry/error/receipt reruns and owner-change denial;
compilation/dependencies pass. Native checks after the user's silent-click report
found no reserved job or workspace/ADMIN/audit rows. The earlier silent-click cause
is unproven. Coordinate a valid five-minute window only after fresh original-job
and scoped-row absence checks; never use missing UI feedback as rollback proof.
Current build/deployment/window evidence is recorded in
[registration progress](../../docs/PROMPT_REGISTRATION.md).

## P6 E2 clarification deployment preparation — 2026-10-16

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

Run the read-only preflight from the repository root:

```sh
.venv/bin/python -m src.validation.clarification_console_preflight --output data/generated/campaign_clarification_policy_diagnostics/deployment_preflight_NEW.json
```

It inventories an isolated temporary context using the exact build ignore rules,
rejects missing required routes/credentials/data exports/symlinks, and hashes every
source file. It reads Cloud Run configuration, service invoker IAM, direct project
runtime bindings, dataset entries and the two policy-table IAM policies. It does
not submit BigQuery queries, impersonate the runtime, change permissions, build,
deploy or register prompts. `PASSED_INSPECTION_RUNTIME_PROBE_PENDING` explicitly
leaves effective runtime access unproven. The source hash identifies inspection
bytes; a later build must retain its own source manifest and immutable image digest.

The native inspection found versions=1 and revocations=0 and no direct runtime
reader on either table. Existing project roles are jobUser and the exact Vertex
version reader; the runtime has ci_application READER. No wider brand-memory
reader was installed. Impersonation was denied and no token was obtained. Do not
request token-creator grants merely to make a preflight pass.

The concrete proposed access change is in
`data/generated/campaign_clarification_policy_diagnostics/runtime_access_plan_20261016.json`.
Use a role with exactly `bigquery.tables.get` and `bigquery.tables.getData`, bound
only to `campaign_clarification_policy_versions` and
`campaign_clarification_policy_revocations`. Preserve all other bindings; use
etag-aware changes/readback. This proposal grants no policy writes, prompt
activation or application membership. Existing registry/workflow permissions stay
as previously configured.

Then build through the existing regional dedicated builder and deploy the resulting
immutable digest. Install the exact opt-in selector configuration documented in
[conversational evidence gaps](../../docs/CONVERSATIONAL_EVIDENCE_GAPS.md#server-configuration-and-release-boundary).
Keep registration/bootstrap writes false. Verify current service IAM, exact
identity/origin, runtime reads and both full target previews in the real IAP session.
Only then coordinate fresh ADMIN/hash-confirmed DRAFT imports; close writes after
verified receipts. No DRAFT import grants model execution.

## Package 1 scoped runtime reads — 2026-10-16


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

The dedicated runtime now has `ciClarificationPolicyReader` on only the two
clarification-policy tables. Permissions are exactly `bigquery.tables.get` and
`bigquery.tables.getData`; no dataset/project reader or policy writer was added.
The setup command is default read-only; explicit `--apply` checks exact existing
role content before creating/reusing and preserves the table IAM etag. Reapply
uses already-present bindings. Unsupported conditional-policy versions deny.

```sh
.venv/bin/python -m src.ingestion.setup_clarification_runtime_access --output data/generated/campaign_clarification_policy_diagnostics/runtime_access_NEW.json
```

Evidence: `runtime_access_completed_20261016.json` and
`runtime_access_reapply_20261016.json` under the same diagnostics directory. Earlier
FAILED reports are not rollback proof; intermediate authoritative snapshots show
exactly which grant existed. Do not broaden IAM or grant impersonation to replace
the pending actual runtime read check.

## Existing console updated for clarification prompts — 2026-10-16

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

The same URL now offers campaign-planner, campaign-evidence-intent and
campaign-evidence-questions. While writes are disabled, prepare both exact-version
reviews to verify runtime source access and parity through IAP. Then coordinate
write enablement. Configuration revisions may discard process-local reviews:
reload and prepare fresh reviews after enablement, inspect each full canonical
snapshot, enter its preview_sha256 (not snapshot_sha256) within five minutes and
submit as DRAFT. Share receipts/safe codes only. Verify both authoritative registry
states/original native jobs before closing writes. DRAFT import never activates
campaign generation. Never reuse an exported old review as approval.

## Both clarification DRAFT registrations verified; writes closed — 2026-10-16

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

## Read-only operator host — 2026-10-17

Revision `ci-prompt-registrar-00011-8vq` serves the operator-status section using
immutable image `sha256:b9c3fef4efb2df6c87124d496fea7ae3f77b8cd74d1ad7b0c4152e13c16f222f`.
`CI_OPERATOR_CONSOLE_ENABLED=true` enables the section;
`CI_OPERATOR_PUBLISHER_PRINCIPAL_ID` is the explicitly user-assigned pilot
principal. Existing IAP, prompt-registration and workspace settings are preserved.
Registration writes and bootstrap remain false. Operator composition is read-only
and has no reviewer-policy proposal. The publisher assignment is not review or
campaign-release permission.

In the console, select **Operator status** under **Console section**. The session
starts an owned worker and reports **Protected operator worker connected; writes
disabled**. **Reconnect operator runtime** closes the old worker before starting
a fresh bounded read session. It never recovers a publication intent from a job
ID. **Read current operator authority** uses fresh IAP and source checks. The five
existing authority/control/audit sources had zero metadata rows at installation;
missing authority/control correctly denies instead of creating records.

The runtime has get/getData-only table access on those five sources. No policy,
reviewer, approval or serving rows were seeded. Browser IAP/reconnect confirmation
is pending; anonymous redirect and independent configuration checks passed.
Acceptance/clarification receipt reading is local, without table creation,
controlled publication, native verification or release integration. See the latest
checkpoint in [implementation plan](../../docs/IMPLEMENTATION_PLAN.md).

Browser checkpoint update — 2026-10-17: the user confirmed the authenticated
operator page and reconnect display “Protected operator worker connected; writes
disabled.” This supersedes the pending browser observation above. It does not
verify a current-authority read, operator revocation or policy approval.

## Protected measurement runtime — 2026-10-17

Revision `ci-prompt-registrar-00012-276`, image digest
`sha256:12f81c8df8bfa204cd2b0412df06f2f2ba6680186e8ee5b7dceca4069da8a5f6`,
adds read-only measurement/approval dependencies inside the supervised worker.
`CI_MEASUREMENT_RUNTIME_ENABLED=true` requires explicit workspace, publisher,
review-baseline and provisional evaluator source hashes in trusted deployment config.
It does not activate measurement, approval, registration or campaign writes.

Open **Operator status**, click **Reconnect operator runtime**, then **Verify
measurement runtime and authority**. A successful read returns runtime_installed and
publisher_verified true, current workspace/membership revisions, and all write/release
flags false. qualifying_evaluation_configured stays false until #2 provides an actual
qualifying measured version. A five-minute manifest expiry requires reconnect; no
write intent or receipt is reconstructed. The worker rechecks IAP and membership,
and rejects missing/duplicate/revoked sources instead of creating authority records.

The exact-table get/getData-only grants cover required workflow, measurement,
approval and shared control/audit sources. Existing IAM, IAP, service account and
settings are preserved. Native membership revocation/restoration and independent
configuration/anonymous-denial checks pass. Authenticated browser/reconnect and the
coordinated fresh-request IAP revoke/restore observation remain pending closeout.
Immediate revocation of an existing WebSocket/in-flight signed token is not claimed.


Closeout — 2026-10-17: authenticated runtime/current-publisher verification and
reconnect are user-confirmed before and after the coordinated registrar IAP test.
The user observed fresh-tab denial while the explicit grant was absent, followed
by restored access and successful runtime verification after reconnect. The exact
original policy/grant and existing bindings were independently verified restored.
This supersedes the preceding pending-browser statement. The protected read-only
installation milestone is complete; writes/release remain disabled and no qualifying
report, semantic result, reviewer role or policy/prompt approval is inferred.
Next is fixed MVP step #2: the missing checkers and measured 159-case benchmark.
