# Authorized prompt lifecycle management

Priority 5, Batch 15 — **Critical / Must have / shared pilot**.
Implementation and planned isolated native verification are complete on
2026-10-14. Serving writes remain disabled by default. Both physical
lifecycle tables were subsequently created and verified empty on 2026-10-12;
see [physical resource verification](PROMPT_PHYSICAL_TABLES.md).
No serving prompt was promoted, activated, retired or blocked in GCP by this work.

## Components and storage

| Component | Responsibility |
| --- | --- |
| `src/application/prompt_lifecycle_contract.py` | Immutable captures/proposals, shared-control schemas/codecs, transition and retry-pin validation |
| `src/application/prompt_lifecycle_query.py` | Parameterized explicit-schema atomic SQL and strict business receipts |
| `src/application/prompt_lifecycle_store.py` | Bounded authoritative reads, schema preflight, native-job correlation, submission/recovery and rollback-proven retry issuance |
| `src/application/prompt_lifecycle_service.py` | Server-only IAP/MANAGE_MODELS authorization, preparation, submit/recover/retry and fresh post-read release checks |
| `src/ingestion/setup_prompt_lifecycle_tables.py` | Opt-in metadata check/missing-resource creation; no data rows |

Two explicit `ci_application` tables provide a shared control for each prompt key:

- `prompt_lifecycle_controls`: one current global head with task, revision/parent
  digest, selected active snapshot, command hash, changed-version digests,
  workspace, verified actor and change time.
- `prompt_lifecycle_history`: immutable copies of those heads, including command
  and transition pins, for historical receipts and authorized promotion/rollback
  provenance. Complete contiguous histories are required; no automatic expiry.

Both are unpartitioned and clustered by their logical scope. Control histories
are bounded at 128 revisions; each registry version retains the existing 64-row
history bound. Exhaustion denies rather than truncating history. Codecs recompute
control hashes and check complete parent chains, immutable prompt/task identity,
timestamps and the current active snapshot against the actual ACTIVE registry row.
The global registry has no workspace column: authorized lifecycle actions affect
that shared prompt key across workspaces. The protected server must deliberately
configure which workspace administrators may manage these shared templates.

Resource commands, **subsequently run against GCP on 2026-10-12**:

```bash
.venv/bin/python -m src.ingestion.setup_prompt_lifecycle_tables --check
.venv/bin/python -m src.ingestion.setup_prompt_lifecycle_tables --apply
```

Setup preflights both tables before creating only missing compatible resources.
It refuses incompatible metadata and never creates control rows, changes IAM,
imports prompts or activates anything. Registry, evaluation and access resources
must separately exist and pass exact metadata checks before service composition.
Both physical lifecycle schemas/layouts passed; both tables contain zero rows.
Repeated `--apply` created nothing. Native transaction and race checks subsequently
passed as recorded below; resource creation alone does not establish them.

## Lifecycle behavior

| Requested transition | Conditions and effect |
| --- | --- |
| DRAFT → REGISTERED | Explicit authorized operator action; exact current version/history; no activation |
| REGISTERED → EVALUATED | Complete PASSED, unexpired evaluation; current benchmark/policy; exact historical REGISTERED anchor and current registry/control pins |
| EVALUATED → ACTIVE | Reapply the evaluation gate; atomically retire any other ACTIVE version of the same key and activate the candidate |
| ACTIVE → RETIRED | Explicit authorized retirement; clear the active selection |
| RETIRED → ACTIVE | Controlled rollback using the original evaluation ID and current passing, unexpired policy/benchmark checks; atomically retire any replacement |
| Non-BLOCKED → BLOCKED where the existing contract permits it | Explicit authorized blocking; retain evaluation provenance and clear selection if blocking the active version |

BLOCKED versions cannot restore. Prompt content, Vertex/model/settings pins and
import metadata never change through lifecycle writes. Activation and rollback
are technical template eligibility decisions; they do not approve product claims,
creative assets, campaign launch or marketing benefits.

The original evaluation's exact stored control anchor is retained separately from
today's selected policy control. Equal policy content across two revisions cannot
silently substitute another control digest in the stored evaluation report.

## Atomic fencing and authorization

Preparation performs one bounded BigQuery transaction capturing fresh access,
global control/history, exact target/current ACTIVE registry heads and complete
histories, and evaluation/control/cases when required. Counts and bounded arrays
reject duplicate, orphan, malformed, partial or overflowing state. Registry
histories include the required partition filter. No local JSON/cache is a fallback.

New write transactions recheck the exact IAP principal/issuer/subject, ACTIVE ADMIN
membership revision, workspace status and original assertion expiry. They compare
all captured heads/histories and the complete current ACTIVE set. Promotions also
compare the report/cases, original anchor, selected evaluation control/history and
native expiry/PASSED state. The workspace fence serializes workspace policy changes;
the shared lifecycle head serializes global prompt changes across workspaces.
Registry update(s), transition history and shared control/history commit together.
Only selected registry lifecycle columns, the shared head and workspace fence may
update; no histories, unrelated versions or evaluation records are deleted.

Concurrent insert-only genesis would not provide a shared mutation fence. The
implementation therefore reserves the same global native job slot for every
candidate/actor/workspace competing for one prompt-key/control revision. Subsequent
transactions additionally update the shared head with exact revision/digest CAS.
This design uses BigQuery's documented conflict behavior for transactions mutating
the same table; mocks do not prove its native implementation. See
[BigQuery multi-statement transactions](https://docs.cloud.google.com/bigquery/docs/transactions).

Exact replay requires the immutable command receipt and complete changed-version
history prefixes. It preserves later head/ACTIVE changes. A replay receipt means
the historical command is recorded; it does not assert the version is still active.

The service requires explicit enablement and fresh signed IAP/MANAGE_MODELS checks
before preparation/dispatch and after provider operations. It rereads every
changed version and checks the exact recorded control and history prefixes before
release. Revoked access withholds results even after commit. ADC supplies provider
credentials only; no email, personal Google login, browser state or empty membership
table grants application authority. This server service is not an activated HTTP
route or UI admin workflow.

## Native jobs, recovery and controlled retries

The store looks up the exact deterministic native job before any insert. Only
authoritative NotFound permits submission, with SDK automatic retries disabled.
Lost responses reconcile that original job. Project/region/native identity, SQL,
parameters and bounded settings must match before accepting a business receipt.
Failed scripts require correlated successful child COMMIT/ROLLBACK evidence;
missing, conflicting or malformed observations stay UNKNOWN. A COMMIT observation
without a valid business receipt cannot release success.

`recover(headers, original_plan)` is read-only with respect to mutation submission.
It accepts a fresh assertion for the same principal/issuer/subject/audience but
keeps the original typed plan, query parameters, assertion expiry and job ID.
It verifies current access and stored history before release, preserving later
authorized state changes. The retained plan is server-held; this implementation
does not create a durable cold-restart intent store or read exported approvals.

`issue_retry(headers, original_plan)` permits at most **three total global native
slots** for a control revision. It requires fresh correlated positive rollback for
every exact ancestor and preserves the complete original capture, business digest,
transition, actor, evaluation, timestamp and expiry. A previously absent retry job
rechecks every ancestor immediately before insertion; an existing retry job is
recovered without another submission. UNKNOWN, successful parents, post-commit
failures, missing ancestry, changed pins or exhausted slots never authorize a new
attempt. No retry is automatic. An exhausted/expired or unverifiable chain stops
management for that occupied revision; no unsafe reset or identity substitution is
provided. Native verification must exercise these limits before serving release.

Query settings disable cache/session mode, cap cumulative write-script billed bytes
at 768 MiB (reads remain 512 MiB)
and bound RPC, result, script and statement time. These are cooperative/provider
limits; they do not establish deployed HTTP/disconnect or owned-process enforcement.
Unpartitioned shared-control updates can contend across prompt keys; this is a
conservative POC management path, not a throughput benchmark.

## Verification and remaining release gates

On 2026-10-12, **457 affected tests passed**, including **56 focused Batch 15 tests**:
10 contract/setup, 7 query, 19 store and 20 service tests. Coverage includes signed
ES256 IAP fixtures/tampering, role/revocation denial, registration/promotion,
replacement/rollback, expiry/changed policy, original evaluation-control pins,
partial post-reads, duplicate/lost response, mismatched jobs/receipts and positive
rollback-only bounded retries. A locked SDK-shaped concurrent fixture verifies
one mocked global job reservation across competing workspaces; it is not native
SQL race evidence. Compilation, dependency and changed-file whitespace/link checks
pass. During the initial offline implementation, no GCP ADC/API/model calls,
physical table creation, serving writes, IAM, deployment or activation occurred.
Subsequent physical setup is documented separately above.

Batch 13's real IAP/ADMIN DRAFT registration and this batch's isolated lifecycle
SQL/replay/concurrency/retry verification are now complete. Before activation,
Batch 16 native dispatch-binding checks, reviewed benchmarks/policies, measured
evaluation and shared deployment/identity gates remain. BigQuery hash/fence tests
do not measure RAG quality or deployed SSO.
Gmail pilot and later Google Workspace SSO remain in the existing deployment plan.

At the Batch 15 checkpoint, Priority 5 had 16 fixed batches, **14/16 complete within their documented
implementation/verification scope** (1–12 and 14–15). Batch 13's live registration
and Batch 16 were open. The next item was **Batch 16 — Critical / Must have**, resolve the
BigQuery-eligible exact Vertex version, independently verify its canonical
content/model/settings/output contract and pin all identities to invocation;
deny mismatch/unavailability without local/latest fallback. Continue in 2–3 file
groups. See [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md).

Batch 16 is implemented and verified offline. Current Priority 5 progress is
**16/16 implementation milestones complete within documented scope**; its native
dispatch-binding gate and quality/activation gates remain pending. See
[exact prompt runtime](PROMPT_RUNTIME.md).

## Planned native completion — 2026-10-14

**Critical / Must have — shared pilot:** all **21 planned native cases passed**
across two final PASSED reports: 13 core cases and eight remaining cases. Both
owned, expiring ten-table datasets were deleted and absence verified. The ten
serving tables' before/after metadata row counts match; this is not a complete
serving-row checksum. Serving writes, Vertex/model calls, GCS reads, IAM changes,
deployment and activation were zero. Fixture identities and observations are
synthetic; this does not authenticate a deployed user or approve campaign content.

The core report covers DRAFT registration/evaluation, activation, atomic active
replacement, rollback, retirement/reactivation/blocking, same-job recovery and
new-job exact reuse, discarded-response recovery, historical replay, positive
rollback-only retry, three-slot exhaustion and post-commit failure without an
invented receipt or retry. The remaining report covers absent-job UNKNOWN,
evaluation/assertion expiry, changed policy, revoked replay, overlapping
cross-workspace promotions, one shared native-slot winner and unrelated rows.

Nine expected native denial/fault observations are correlated with exact jobs,
error codes and commit classifications. Denials preserve sorted snapshots of all
ten fixture tables. The activation race has **46.688 seconds of verified native
transaction overlap**, one complete winner and positive loser rollback. Both
candidate versions, complete control/history prefixes, the single workspace-fence
increment and all other workspace fields/unrelated rows are checked. The native
conflict occurred at the unpartitioned `workspaces` fence. This proves safety for
the installed schemas, not an isolated conflict at the global-head CAS. The
separate reservation race keeps production's workspace/actor-independent global
slot semantics; SQL-race-only slots differ in the fixture namespace.

Native replacement exposed a real production query-setting defect: the 512 MiB
cumulative cap left 12 MiB before an UPDATE requiring at least 20 MiB. The bounded
write cap is now **768 MiB**, with replacement and rollback verified natively.
Exact job/configuration matching still denies a mismatched historical job; there
is no retry or resubmission fallback for a changed configuration. Fixture-only
metadata/submission/cleanup RPCs use a 30-second cap, while fixture store RPCs use
the existing allowed 20-second cap. SDK mutation retries remain disabled.

Four intermediate reports remain FAILED with verified cleanup: a metadata
timeout, an invalid fixture store setting caught by offline tests, the insufficient
write budget and a later inherited snapshot-helper timeout. The earlier pre-slice
`data/generated/prompt_lifecycle_integration/f6d3fb94331a4427a8356d0169ceceb3.json`
read failure remains retained separately. Neither partial run substitutes for the
two final complete PASSED group reports. Explicit `--core-only`/`--remaining-only`
routes have disjoint required case manifests; default execution still requires all
21. Missing/duplicate cases, unrelated SQL failures, invented receipts, absent
overlap or unverifiable losers cannot pass report validation.

**Verification:** all **79 focused Batch 15 tests** passed (56 contract/query/store/
service tests plus 23 probe guards, including nine added guards). Compilation and
dependency checks pass. An earlier full offline regression also passed 2,185 tests;
the user's continuing test policy is to run affected files/functionality and direct
dependencies, without unnecessary full-suite repetitions.

Final evidence and hashes:

- [Combined verification](../data/generated/prompt_lifecycle_diagnostics/batch15_completed_20261014/verification.json)
- [Core PASSED report](../data/generated/prompt_lifecycle_diagnostics/batch15_core_20261013/6227379f2d5649c6ba3dc6808d5bda58.json)
- [Remaining PASSED report](../data/generated/prompt_lifecycle_diagnostics/batch15_remaining_20261013/94d8d20f504545a7aec270f51e4925a2.json)

Next fixed gate: **Batch 16 native dispatch binding/promotion-versus-claim —
Critical / Must have**, then **Priority 6 measured benchmark and reviewed acceptance
policy — High / Must have**, both for the shared pilot. No new batch is added.
