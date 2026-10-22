# Marketing access and ownership policy

Updated: 2026-10-11. A1 policy, priority 3 read foundations and priority 4.4 gates.

The reusable policy is implemented in [access_policy.py](../src/application/access_policy.py), with [unit tests](../tests/test_access_policy.py). The server [identity adapter](../src/application/identity.py) and [BigQuery access reader](../src/application/access_reader.py) supply its trusted inputs. The [protected service gateway](../src/application/protected_service.py) connects them for job status, output summaries and artifact downloads. The [4.4 runtime gates](WORKFLOW_RUNTIME_GATES.md) add disabled-by-default protected HTTP, management and existing-invocation admission/dispatch guards. The local chat does not invoke them. General workflow/model binding, SSO activation, UI integration and deployed reconnect tests remain pending.

## Identity and authority

The user currently accesses the local POC through a personal Google account and does not yet have a company Google Workspace account. Keep the implemented policy and Google Workspace SSO design for the future team application. Company domain, approved access groups, GCP organization relationship and initial application assignments are deferred until that setup is available; local backend/UI work can continue.

The Google Workspace domain will be the company's managed email domain: for example, an employee address of `name@company.com` belongs to `company.com`. This example is not a configured domain. A personal account does not establish a company domain or application role. No personal email, fake company domain or automatic admin assignment has been configured.

The Workspace domain identifies the organization; an application `workspace_id` identifies an authorization scope in our records. Neither the domain nor a browser session ID is a principal identity or a role assignment. The adapter produces a stable principal from a verified IAP assertion; the reader loads current membership and conversation ownership from BigQuery. IAP group admission and application roles are separate checks.

The policy's `Principal` object represents an identity that a trusted adapter has already verified; constructing the object does not authenticate someone. Browser fields, prompts, email labels and local ADC must not create that identity or supply authoritative membership/ownership. The runtime's cloud identity calls BigQuery, GCS and Vertex; it is distinct from the marketing user's identity.

## Implemented identity adapter

`IapIdentityAdapter.authenticate` accepts the server-observed `X-Goog-IAP-JWT-Assertion`. It verifies an ES256 signature against Google's fixed public-key endpoint, the exact `https://cloud.google.com/iap` issuer, the configured Cloud Run audience, integer issued/expiry times and a maximum 660-second lifetime with 30-second clock skew. Optional `nbf` is also checked. These checks follow [Google's signed-header guidance](https://docs.cloud.google.com/iap/docs/signed-headers-howto). Unsupported algorithms, remote-key headers, duplicate assertion headers and malformed assertions are rejected. Tokens, claims and transport details are excluded from application error messages.

The future server must configure `CI_IAP_EXPECTED_AUDIENCE` to `/projects/PROJECT_NUMBER/locations/asia-south1/services/SERVICE_NAME` for its actual IAP-protected service. No value or service has been invented or activated. Missing configuration fails; there is no development identity fallback. Principal IDs are `iap-v1:` plus SHA-256 of the UTF-8 issuer, a NUL separator and subject. Email, hosted-domain and role claims do not assign application permissions.

`VerifiedIdentity` is an internal result of the trusted verifier, not a browser payload format. The service must authenticate each protected request before passing that result to readers. Constructing this dataclass or calling `principal_for_subject` does not verify a signature. The public-key fetch has a five-second transport timeout and fails closed on outage; key caching remains a later optimization. No private keys or user tokens are saved by this module.

## Implemented authoritative access reader

`BigQueryAccessReader.authorize` requires a fresh `VerifiedIdentity`, a typed server action and a workspace ID. It reads current `ci_application.workspaces` and the membership bound to both workspace and principal. Resource actions also read the conversation's current owner, visibility and access revision. Every call uses scoped parameterized queries without BigQuery query caching, a configured byte limit (64 MiB by default) and bounded timeouts. Missing, malformed, duplicate or unavailable records deny access; no local file fallback or automatic role assignment exists.

Missing, suspended and revoked membership denies before any conversation read. Active membership and current ownership/visibility are evaluated by the existing policy. Archived conversations permit reads/downloads only. Identity freshness is checked again after successful reads. Workspace/membership/conversation queries are separate fresh reads, not a transaction or a reusable authorization grant; future mutation/dispatch writers must reread and fence current revisions when committing. Job/output parent resolution and protected downloads are implemented below.

The reader uses ADC for database access only. It performs no data mutations, membership bootstrap or IAM changes. The operator-only `--probe` command checks deployed schemas, verifies these three access tables are empty and dry-runs summary and full download job/request/output queries and partition predicates. It does not prove a real user can sign in. It refuses populated access tables rather than printing user records.

## Protected job and output summaries

`ProtectedWorkflowService.get_job_status(headers, workspace_id=..., job_id=...)` and `get_output_summary` authenticate the server-observed IAP assertion on every call. The service accepts no principal, owner, role, parent conversation, output ID or output date supplied by a browser. A HTTP host must pass its actual request headers to this gateway; accepting headers serialized inside a user payload is outside this trust contract. There is no HTTP deployment or Streamlit integration in this slice.

The reader checks current workspace/membership/read permission before looking up a job. It derives the parent conversation from the authoritative workspace-scoped job row and verifies that copied owner metadata agrees with the conversation. Current PRIVATE/TEAM policy grants access; a copied owner field alone cannot grant it. The job projection also checks canonical SKU, schema, counters, timestamps and status invariants, without claiming full request/invocation/output integrity validation.

Only SUCCEEDED jobs resolve an output. Its ID comes from that job. The output partition range runs from the job's creation to completion, with a maximum seven-day span. Larger spans deny instead of broadening the query. Output workspace, conversation, owner, request, job, winning attempt, SKU, timestamp and original POC provenance must agree. Duplicates within the bounded lookup deny; future coordinated writers must enforce immutable parent links and uniqueness across partitions. This read does not establish those write guarantees. Before releasing an output summary, the reader fetches membership and parent access again so a revoked membership or team share during the output query can deny the result.

Returned frozen projections contain job status or output metadata only. They omit owner/principal IDs, raw briefs, model profiles, evidence payloads, artifact references and GCS paths. `initial_review_status` describes the output's original state, not its current approval. Authentication failures, missing/invalid/duplicate records, private-resource denial and database outage all produce the same public `ProtectedServiceError` with code ACCESS_DENIED. Internal access decisions remain available to future controlled audit persistence.

This gateway exposes no interpreter, generator, sharing, review or bootstrap methods. Those require validated durable records, fenced dispatch/commit, model/limit admission and current authorization. Existing local campaign generation still uses `CampaignChatService` directly; it must not be deployed as a shared authenticated service by assuming this separate gateway protects it.

## Protected artifact downloads

`ProtectedWorkflowService.download_artifact(headers, workspace_id=..., job_id=..., artifact_id=...)` authenticates the observed IAP assertion and authorizes DOWNLOAD_ARTIFACT against current BigQuery membership and conversation scope. Callers cannot supply an owner, output ID, bucket, URL, generation or lookup date. Downloads require a server-configured [GCS reader](../src/application/artifact_download.py); absent configuration denies access.

The reader fetches complete job/request/output rows and applies the [output/artifact contracts](WORKFLOW_DATA_DESIGN.md#implemented-output-and-artifact-contracts). Request lookup covers the seven days ending at the job's creation; output lookup covers job creation through completion, with a maximum seven-day duration. These windows derive from stored timestamps and do not limit an artifact's age. Records outside these bounds, missing/duplicate rows or contract failures deny rather than broaden the scan or use local exports. Future admission/writers must enforce the request-to-job delay bound and global uniqueness.

GCS reads use only the reserved workflow namespace, with both the explicit generation selector and matching preconditions. The reader checks object generation, size, MIME and absent content encoding before reading, then uses a raw byte range capped at declared size plus one. Exact length and SHA-256 must match. PNGs must decode as a single-frame 1200 x 628 image; JSON bytes must equal the canonical stored brief; text must be nonblank UTF-8 without disallowed controls. Default limits are 32 MiB per artifact, 64 MiB per output bundle, with a 20-second timeout/retry deadline per GCS operation. Cloud failures and integrity failures use the same public denial as access failures.

After fetching bytes, the service reruns the complete authoritative read, including current workspace/membership/ownership/sharing and identity freshness. Changed output metadata, manifest or stored brief blocks release. A successful response contains immutable bytes, a safe filename/MIME/hash and POC output metadata; it contains no GCS URL or user identity. Original review state is not a current approval. These separate reads do not establish an atomic snapshot or future writer fencing. The future HTTP host must return private attachments without shared caching and with MIME sniffing disabled; no HTTP route or UI wiring is added here.

[Thirty-seven download tests](../tests/test_protected_download.py) use real in-memory ES256 signatures with fake full BigQuery/GCS records. They cover private/team/archive access, revoked membership/sharing during reads, changed manifests/briefs, expiry, duplicate/missing parents, query bounds, generation preconditions, size/hash/format failures, UTF-8, animated PNG rejection and cloud outages. No test writes users or artifacts to cloud storage.

## Permission matrix

Roles are additive. Every allowed action requires active membership in the requested workspace. Resource actions also require an authoritative resource record in that workspace.

| Action | Viewer | Creator | Reviewer | Admin |
| --- | --- | --- | --- | --- |
| Read draft / download artifact | Own or team-shared | Own or team-shared | Own or team-shared | Requires an additional content role |
| Generate draft | — | Allowed | — | Requires Creator |
| Refine draft / change visibility | — | Own only | — | Requires Creator and ownership |
| Record POC creative review | — | Requires Reviewer | Own or team-shared | Requires Reviewer |
| Manage membership, model availability and limits | — | — | — | Allowed |
| Manage workspace status | — | — | — | Allowed |
| Archive conversation | — | Own only | — | Requires Creator and ownership |
| Approve benefit claims / launch campaign | Disabled | Disabled | Disabled | Disabled |

For example, a creator who also reviews drafts needs both roles. Admin does not grant access to private content or creative review. Reviewer permission alone does not generate or modify drafts. Claim approval and campaign launch have explicit disabled actions; they need separate future authorization workflows. Existing evidence restrictions still apply even if an access decision is allowed.

Reviewer scope is accessible drafts: an owned draft or a draft explicitly shared with the team. This slice does not introduce a two-person review requirement. Human POC creative review remains distinct from benefit approval and real campaign launch.

## Ownership and sharing

New drafts are private by default. `new_draft_access` derives the owner from the principal after checking generation permission. It accepts no caller-supplied owner or visibility. Future persistence must insert a new resource identity without silently replacing the owner of an existing record.

Only an active creator who owns a draft can refine it or change its visibility. `PRIVATE` grants no access to other members. `TEAM` makes it accessible to active members of the same application workspace according to their roles; it does not grant peers editing or resharing permissions. Individual-user shares and ownership transfer are outside this first contract. Conversation row/creation/sharing validators enforce these boundaries on supplied snapshots. The opt-in 4.4 management path now authorizes creator/owner archival with atomic audits and blocks related active jobs. Visibility is preserved and the access revision increases; restoration remains disabled.

`change_visibility` returns proposed metadata, preserving owner/workspace/resource identity and incrementing `access_revision` only on a change. Persistence must authorize against current records, compare the expected revision, write the change and record the audit outcome. This pure function does not provide a transaction or distributed lock.

Conversation, job, output version and artifact records will carry the same owner/workspace scope. Their access must resolve the current conversation visibility rather than rely on an old copied value after a sharing change. The [workflow schema design](WORKFLOW_DATA_DESIGN.md) defines this relationship and protected summaries/downloads implement it. Paginated history and standalone full output/evidence reads remain pending. A resource ID or GCS path alone cannot authorize access. Canonical product keys remain CI-SKU-0001 through CI-SKU-0020; workspace identity does not remap products.

Suspended or revoked membership denies owned and team content as well as generation. There is no local JSON membership fallback. The future service must fetch current authoritative membership and resource access before protected operations and recheck before mutations. Long-running jobs, UI reconnects and downloads need explicit revocation behavior in A2/A3; a previously allowed decision is not a permanent grant.

## Decisions and integration boundaries

`authorize` returns allow/deny with an action, reason code, policy version and principal/workspace/resource revisions. Missing identity, missing/inactive membership, mismatched principal/workspace, missing roles, inaccessible private content and nonowner edits fail closed. Invalid typed contracts raise an error rather than silently normalizing browser input.

`require_authorized` raises a generic `AuthorizationError` on denial. The internal decision is available for an authorized audit sink; user-facing errors must not disclose resource existence, owner identity or membership details. `to_dict` supplies audit correlation metadata but does not persist it. Audit access, redaction and retention remain part of the upcoming records design.

The gateway enforces checks for status/summary reads and downloads. Remaining service integration must protect generation, history, sharing and review writes, and connect these guards to the UI. Model-specific permissions, quotas, evidence eligibility and job-state constraints remain additional checks; a policy allow is not permission to skip them. Admin management operations require their own validated write interfaces and audit records. Membership synchronization/bootstrap must be controlled and cannot derive app roles from a browser declaration.

The existing privileged review CLI uses reviewer labels and local ADC, not verified marketing principals. Existing approvals are preserved. Binding new shared-application review decisions to verified principals is pending A3. No new cloud membership records, IAM assignments, approvals or product data are created by this milestone.

## Small remaining steps

1. Keep the deployment portion of A1 deferred: confirm the real Workspace domain, approved groups, organization relationship and initial assignments when the company account is available, before the Workspace-protected team pilot.
2. U1/F1/U3 schema setup and pure request/job/profile/idempotency validators are complete in [WORKFLOW_DATA_DESIGN.md](WORKFLOW_DATA_DESIGN.md). No memberships have been assigned. Remaining row validation and verification of the proposed coordinator are required before writers. Local development can continue with the current personal-account setup.
3. Protected download and attempt/stage-event/model-invocation/evidence/audit contract foundations are complete. Workspace, membership and conversation contracts and reader integration are complete offline. The scoped sharing coordinator and parameterized transaction SQL/query contracts are complete offline. The disabled-by-default sharing executor/sealed-intent adapter is now connected and tested offline. The isolated native probe passed 11 cases and scoped cleanup. Read-only authoritative receipt recovery is complete with 181 impacted tests and six focused native cases; see [receipt behavior](SHARING_RECEIPT_RECOVERY.md). The [trusted retry adapter](SHARING_RETRY_EXECUTOR.md) is now implemented with 252 affected offline tests passing. The separate [native retry proof](SHARING_RETRY_INTEGRATION.md) now passes ten isolated cases and 266 affected offline tests with owned cleanup. The [key/version retention contract](SHARING_SIGNING_RETENTION.md) now passes 304 affected tests. The [pinned loader](SHARING_SECRET_LOADER.md) now passes 335 affected offline tests. The [publication/refresh contract](SHARING_PUBLICATION_REFRESH.md) now passes 379 affected tests. [4.1 storage/read design](SHARING_PUBLICATION_STORAGE.md) is complete offline with 443 affected tests. 4.2 (Critical / Must have) is complete within implementation/native fixture scope: [resource setup, codecs and authoritative reads](SHARING_PUBLICATION_READER.md), the [fenced writer](SHARING_PUBLICATION_WRITER.md) and [controlled bootstrap](SHARING_SIGNING_BOOTSTRAP.md) are implemented and verified; 546 affected offline tests and 13 native bootstrap cases pass, and real serving bootstrap/shared activation remain deferred. Runtime integration, remaining runtime protection and workflow writers keep their existing IDs before shared generation, sharing or review. See the [detailed sequence](WORKFLOW_DATA_DESIGN.md#next-small-steps). Define real UI reconnect assertions; the implemented gateway requires a fresh verified assertion on each call.
4. Implement scoped durable workflow records and authorized UI history/downloads, then prepare and verify the Workspace-protected shared pilot in A4/A5.

The 21 policy tests, 16 identity tests, 21 reader tests and 22 protected-gateway tests pass. Identity and gateway integration tests exercise real ES256 signatures using ephemeral in-memory keys, including signature tampering, wrong issuer/audience and expired assertions. Reader/gateway fixtures exercise fresh revocation/sharing changes, scoped parameters, private ownership, parent-link mismatch, bounded partitions, POC provenance, archive restrictions, malformed/duplicate records and cloud failures. These tests establish module behavior; they do not verify Google-issued assertions from a deployed service, shared SSO or UI enforcement. These identity/reader/gateway tests write no fixture memberships to BigQuery. The separate native probe below uses only a fresh nonserving fixture dataset.

Live verification on 2026-10-10 used ADC and the following read-only command:

```sh
.venv/bin/python -m src.application.access_reader --probe
```

The probe returned `VERIFIED_EMPTY_ACCESS_TABLES`, region `asia-south1`, workspaces 0, memberships 0, conversations 0, writes 0. Dataset and all eleven schema/layout checks passed before these queries. Summary and full download job/request/output dry runs passed, with estimated bytes 0 for each empty table. The previous full-suite baseline passed 944 tests, including 33 sharing executor tests and 31 sharing query-contract tests and 41 sharing coordinator tests, 43 conversation contract tests, 47 membership contract tests, 34 workspace control tests, 37 download tests, 40 offline attempt/stage-event tests, 45 model-invocation tests, 44 evidence contract tests and 43 audit contract tests. Workspace validation checks complete ACTIVE/DISABLED rows and revisioned proposals without bootstrap or transaction locking. Membership contracts bind IAP identity, roles/status/origin and revisioned updates; ADMIN proposal checks evaluate supplied snapshots without authenticating or proving current access. Conversation contracts add actor-derived private creation and creator/owner sharing checks; archival is structurally validated but its authorized write path remains disabled. The sharing coordinator contract now checks bounded snapshots, immutable proposals, hashed receipt identity, current access before historical reuse and uncertain commit observations without executing queries or proving commits. The sharing SQL/query contract now defines atomic updates and bounded result validation without submitting queries; the executor now adds offline-tested sealed intents, current-access rechecks and native job correlation/classification with execution disabled by default. Native SQL/SDK behavior and overlapping fixture sharing transactions are now verified in the separate probe below; deployed workflow concurrency remains unverified. Signing keys are runtime configuration. The [retention contract](SHARING_SIGNING_RETENTION.md) supports planned rotation and compatible retained profiles offline; managed-key provisioning, deployed rollout/revocation consistency and future incompatible-version migration remain pending. Controlled bootstrap/synchronization, last-admin safeguards, lifecycle permission/auditing and atomic management writes remain pending. Audit validation binds captured observations and immutable replay without persisting or granting access. The evidence slice validates offline projections and output links; source rechecks, projection/persistence and atomic publication remain pending. No business data, approvals, IAM, artifacts or deployment settings changed. Dry runs do not verify populated-table performance, a real user's sign-in or an end-to-end live download; byte verification is exercised using test fixtures because workflow artifacts have not been published.

## Native sharing fixture verification

The separate [isolated native probe](SHARING_INTEGRATION.md) passed 11 cases
using ADC on 2026-10-10, including rollback, post-commit failure classification,
same-ID concurrent submission/recovery, genuinely overlapping distinct-intent
transactions and sharing denial after a fenced revocation. Its fresh owned
dataset was deleted. Only synthetic nonserving access rows were loaded; no actual
serving user was assigned a role. Fifteen probe tests bring the full suite to
944 passing tests. Google-issued deployed sign-in, protected management endpoints,
concurrent administrative revocation, host cancellation and process-crash recovery
remain unverified. Sharing stays disabled by default and the local UI is unchanged.
