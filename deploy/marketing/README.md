# Release-disabled marketing pilot

### Document-processing deployment — 2026-10-18

**Critical / Must have, fixed package 1, shared pilot:** regional Cloud Build
`497ae5af-9ad9-4689-8922-4d153ec8f47d` succeeded using the existing dedicated builder.
The 388 source-file allowlist plus build ignore file excludes data, credentials,
local caches and tests. Per-file hashes are recorded in
`data/generated/campaign_e2_diagnostics/document_build_source_20261018.json`.
Immutable digest `sha256:12ee51ac1f50a35411058fce9afe02fb9380ce7db3e33c73e3fc0757a88adea3`
is deployed as `ci-marketing-00010-cpv`, serving 100%. Server verification passes
exact image, runtime/secrets/resources/capacity preservation, identical IAP/invoker
policies, IAP-only invocation, disabled pinned release/interpreter configuration,
and anonymous HTTP 302 denial. Rollback target remains the prior verified immutable
image/revision `ci-marketing-00009-mzq`. This was an image-only update.
Evidence: `data/generated/campaign_e2_diagnostics/document_deployment_verification_20261018.json`.

Chrome computer control was denied. No signed-in browser interaction occurred;
actual extraction/status/reconnect and effective runtime query/write access remain
PENDING. The saved unfinished request still cannot authorize omission. Positive
live omission requires legitimate completed history and a fresh applicable gap;
no synthetic successful intent may be installed for that request. New tables and
narrow grants are verified, not proof of those runtime actions. No model calls,
prompt/policy approvals or generation enablement occurred. Qualifying full pipeline
benchmark remains 0/159. 88 affected offline tests, compilation/dependencies pass.

Next within the fixed MVP: (1) signed-in IAP extraction/status of the existing TXT,
verify original UNVERIFIED receipt remains unchanged, and reconnect with the same
extraction identity/NEEDS_REVIEW hold; (2) verify allowed/denied omission under
legitimate current completed-history/gap conditions, preserving the original and
original job; (3) finish source applicability and qualifying measured evaluations;
(4) present fresh exact prompt/policy versions and qualifying results for authorized
approval before any release. Browser denial is an access limitation, not approval
to replace IAP with ADC or bypass execution/quality gates.


## Current attachment intake — 2026-10-17

Current revision `ci-marketing-00009-mzq` adds protected optional supporting-material
collection and BigQuery-backed saved-request/submission history. Immutable image:
`sha256:21838bdc34e9823e719148e7fa25507ea86c5b40124431515c0226a9a56c4eab`, regional
build `4d97c4a5-60c3-4eb1-9cd5-edbea24d39ac`. Attachment config is baked at
`/app/config/clarification/attachment_intake_deployment.json` and selected through
`CI_CAMPAIGN_ATTACHMENTS_CONFIG`. Generation remains disabled in the same pinned
marketing config version 2; IAP, secret pins, service account and capacity are unchanged.

Private originals use `gs://ci-campaign-evidence-1042059373096`; canonical unverified
metadata uses `ci_application.campaign_evidence_submissions`. The bucket enforces
uniform access/public-access prevention. Runtime access is scoped to bucket
metadata/object create/get and the metadata table's existing narrow writer role.
No source asset, claim, approval, policy or model permission is inferred from an upload.

85 focused tests and eight isolated native storage/history checks pass, with
verified fixture cleanup. A pre-model optional collection path binds current saved
request authority and policy, without inventing a successful intent or missing
evidence; the actual intent/question execution gates remain unchanged. Browser storage/creator access and a real upload/reconnect are now user-confirmed;
independent BigQuery row/native-job and pinned GCS object verification pass. Files
remain unverified and ineligible; extraction and generation approval are separate. See [upload behavior/setup](../../docs/CAMPAIGN_EVIDENCE_UPLOADS.md).
The older deployment checkpoints below are historical, not current image pins.

Service: `ci-marketing`, project `project-661d501a-e709-4276-a26`, `asia-south1`.
URL: https://ci-marketing-1042059373096.asia-south1.run.app/
First Ready revision: `ci-marketing-00001-dt7`.
Image digest: `sha256:9d7542755460daaf6e3710ebc5eed00d0f8257c766a55236dcde431e20b36f55`.
Cloud Build: `fef2dfe2-639a-4656-8248-0b59e723f3e5`.
The dependency image is immutable and reused from the existing console. Build
context is allowlisted; local exports, credentials and caches are excluded.

The dedicated runtime is `ci-marketing@project-661d501a-e709-4276-a26.iam.gserviceaccount.com`.
It has query-job creation, application-dataset reads, catalog-table reads and
get/getData/updateData grants on nine workflow tables. The custom writer has no
create/delete-table permissions. It has no Vertex model permission.
Secret Manager configuration is pinned to version 2 and history cursor key to version 1,
with user-managed replication in asia-south1. No secret values are stored here.
The config comes from exact registered BigQuery snapshots; profiles are server
configuration, not approval. Model execution and generated-output release are false.

IAP is enabled. Only the IAP service agent has Cloud Run Invoker. The existing
pilot Gmail account has service-specific IAP accessor access. The original
prompt-registration service and its lifecycle records are unchanged.

## Authenticated setup

After IAP sign-in, the application checks current membership in `ci-poc-marketing`.
An existing ADMIN may explicitly click Enable my campaign creator access. That
calls the existing protected, audited management service and preserves other roles.
It is not an automatic role grant on sign-in. A CREATOR may then explicitly create
their private deterministic conversation. Unknown setup outcomes recover only the
original management operation; no alternate command/job is generated.

Each private runtime is keyed by verified principal and conversation. Current
workspace/membership/ownership checks happen before returning cached hosts.
The pilot is bounded to two private runtimes and one Cloud Run instance (2 GiB).
Retired hosts require the explicit Reconnect protected services action; reconnect
never resubmits a message or model call. This is a bounded pilot, not a measured
multi-team capacity result.

Intake audience options come from the exact BigQuery registered intent schema,
with a fresh source/access reread. NEW/RETURNING is therefore shown as customer
type; descriptive targeting can remain in the user message. No registered prompt
was changed. Saved requests are not generated campaigns or approved outputs.

## Verification and current limits

`python -m src.validation.marketing_deployment --output <report>` checks revision,
image, pinned secret reference, IAP enablement, policies and anonymous denial.
This does not establish browser authentication, revocation or worker reconnect.
The initial user sign-in was denied by IAP despite the stored accessor binding;
follow-up diagnosis is recorded in the implementation plan. Do not treat the
metadata-only PASSED report as an authenticated end-to-end pass.

An operator impersonation probe failed because the operator lacks
`iam.serviceAccounts.getAccessToken`. No Token Creator grant was added merely to
make that probe pass. Runtime transport must be verified through the real service.

The 2026-10-17 question composition is deployed in revision
`ci-marketing-00007-ptq` using immutable image digest
`95b1926b7bb9607ae8780d1fa7064ec6b2a264810751ab54c346b8b1a8de8ada`.
The owned interpreter host now includes the separate question-child writer,
registered runtime, exact context guard and durable outcome worker. A read-only
`evidence` IPC method connects readiness and validated question turns to the
existing chat renderer. Withheld provider text never crosses that transport.
This is deployed disabled-path wiring, not verified live question execution.

Execution and release remain false in `ci-marketing-config:2`.
Both prompts and the clarification policy are independently reread as DRAFT.
`interpreter_runtime.question_quality_policy` now contains exact BigQuery-verified
draft-v0.2 policy ID/version/specification/header pins, never approval booleans.
Missing configuration holds execution. The trusted
post-publication callback projects the successful durable question response,
runs authoritative quality checking and stages only a validated private turn.
Current readers still cannot grant release, and unavailable checks stay holds.

Deployment metadata, IAP-only invoker, existing pilot accessor and anonymous
redirect checks pass. The user confirms current-URL history loads after reconnect;
the browser did not report its revision. Actual question execution remains
unverified. The prior revision's fresh-page revocation
test is not evidence for existing sockets or this revision's worker execution.
Applicable measured model evaluations, operational approval verification and
explicit authorized policy/prompt approval remain incomplete. Original workflow
catalog versus N&T quality-reader scope and missing/expired raw-transcript holds
remain explicit. The existing exact read-only role is additionally granted on
only the two clarification-policy tables for marketing readiness, with other
bindings preserved; effective runtime reads still need live verification.
No approval, model invocation or activation occurred.

The subsequent identity-authority slice verifies all 23 current BigQuery products
without inventing missing sizes or granting claims. The original 20 BEAUTYFEEDS
SKUs can now reach quality checking; legacy N&T workflow-admission constraints
remain. Nine separate diagnostic model calls (not serving requests) tested exact
DRAFT clarification snapshots: 9/9 structural matches and 6/6 proposed intent
feature matches. Question semantics and the qualifying release benchmark remain
incomplete. These calls do not approve or activate a policy/prompt. Current
revision metadata/IAP/anonymous denial pass; authenticated history/readiness was
user-verified on the prior revision, not independently reverified on revision 7.
