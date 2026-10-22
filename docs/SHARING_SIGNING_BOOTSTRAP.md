# Controlled signing genesis bootstrap

**4.2 — Critical / Must have — shared pilot.**
[The bootstrap adapter](../src/application/signing_bootstrap.py) implements initial
publication/history/head creation without assuming an empty-table INSERT enforces
uniqueness. It is disabled by default, has no public route or local UI wiring and
does not load secret bytes, admit replicas or enable sharing.

## Ownership and immutable manifest

Trusted server configuration supplies a frozen `SigningBootstrapManifest`: one
explicit IAP principal, validated revision-one publication, acceptance time and
an exclusive deadline at most five minutes later. The publication must have no
parent. Complete typed history, canonical metadata, hashes and initial head fence
are derived from the existing contracts and explicit destination schemas.

The identity verifier checks the signed assertion and freshness; only that exact
owner can submit or recover. Marketing Admin roles, domain/email headers and ADC
cannot assign bootstrap ownership. ADC authenticates the BigQuery client only.
The manifest is a one-time ingestion proposal, not a serving source or a fallback
when BigQuery fails. It contains metadata, never signing key bytes. Its owner,
publication and times must remain unchanged across callers, restarts and retries.

Live owner/audience/project-number/secret bindings still need trusted deployment
configuration and verification before activation. Tests do not supply those
production grants. Different configured manifests deliberately compete for the
same native job slot, rather than independently writing initial heads.

## Exclusive execution through native job slots

There are exactly three reserved slots, numbered zero through two. Their IDs
derive only from the installed project, region, policy and fixed bootstrap
version. Actor, command, publication digest, timestamps, deployment epoch and
transport identifiers never create another slot. Production scope/IDs cannot be
overridden through the adapter. Every bootstrap submission uses `jobs.insert`
through the SDK with an explicit ID; automatic query/job retries are disabled.

BigQuery does not repeat a job under the same job ID, and job metadata has a
six-month retention window. This is the execution coordination boundary, not
an absent-row uniqueness check or a process-local lock.
[Google's job management documentation](https://docs.cloud.google.com/bigquery/docs/managing-jobs).
The bootstrap manifest expires in five minutes, well before that retention
window. Never delete bootstrap job metadata, rotate its namespace or refresh
the window to bypass an uncertain outcome. Job metadata is not a permanent lock
or the application's durable state; the four BigQuery control tables remain
authoritative and have no automatic expiry.

A known slot is read/recovered, never submitted again by that adapter. A native
409 collision triggers only an original-job read. Concurrent replicas may make
duplicate inserts into the job service for the same reserved slot; they cannot
request separate executions under different IDs. Exact SQL, scope, configuration,
parameters and native parent identity must correlate before an outcome is accepted.
A different manifest under the same slot stays UNKNOWN, preventing takeover.

Advancing to slot one or two requires fresh positive correlated terminal rollback
evidence for **every** predecessor. Missing, running, mismatched or uncertain jobs
block advancement. A commit blocks all later slots, even when the result receipt
was lost or the script failed after COMMIT. The caller cannot skip to a slot with
missing ancestors, change the envelope or exceed the three-slot cap. A process
that already attempted an absent/uncertain slot cannot resubmit it. Restarted
callers still use the same global IDs; an UNKNOWN outcome never creates a new ID.

These guarantees assume all initial writes use this coordinator and its reserved
namespace. Privileged out-of-band DML, resource replacement or job-metadata
deletion are outside that boundary. Runtime IAM must restrict those actions;
this slice changes no IAM. The existing publisher/checkpoint writer refuses an
absent head and uses the head fence once bootstrap has committed.

## Atomic initial state and recovery

[The SQL script](../sql/signing_bootstrap.sql) checks its native start time,
validates the slot range and asserts that **all four** policy scopes are empty.
Any existing head, publication, history or checkpoint denies bootstrap, including
orphaned/partial state. Rows for other policies are preserved. It then inserts
the initial publication, complete history and revision-one/control-revision-one
head in one transaction. There are no DELETE, MERGE, resource-reset or inferred
schema operations. Initial checkpoints stay absent; their first creation uses
the already verified shared-head fence in the regular writer.

The script materializes actual inserted records before commit. The result
validator checks exact publication/history/head, hashes, immutable owner/command
stamps, zero checkpoints and the original time window. Exception handling rolls
back and rethrows. This preserves incomplete-state denial rather than repairing
unrelated records silently.

Read-only recovery remains available with writes disabled and after the window
expires, but requires fresh owner authentication and resource preflight. Failed
parent jobs need positive correlated child COMMIT/ROLLBACK evidence; parent error
or timeout alone is insufficient. A failure after COMMIT may report COMMITTED
without a receipt. Receipt/job outcomes describe history, not current eligibility,
readiness, loaded keys or successful future dispatch. Runtime refresh in 4.3 must
read authoritative current tables independently.

Missing original job metadata or query results stays UNKNOWN. No local report or
manifest substitutes for that native observation. A terminal failure without
positive rollback evidence, changed configuration or exhausted slots remains
closed; operators must not invent a new namespace or bypass the coordinator.

## Bounds and verification

Payloads inherit the explicit 2 MiB bound. Scripts use a 512 MiB billed-byte cap,
60-second native job timeout and 20-second statement timeout. RPC/result waits
are bounded, with SDK retries disabled. Overall host deadlines/cancellation and
HTTP response/audit integration remain under 4.4. The local lock only suppresses
duplicate admission in one process; it is not the distributed coordination proof.

All **546 affected offline tests pass**, including **22 bootstrap tests** and
**five fixture-safety tests**. They cover strict settings/genesis/window contracts,
exact receipts, owner/real ES256 verification, reserved IDs, known-job/restart/409
recovery, positive rollback, every ancestor, uncertain/mismatched outcomes, the
attempt cap and owned resource cleanup guards. The run completed in 2.382 seconds.
The full suite and model evaluations were not run.

The opt-in [native probe](../src/validation/signing_bootstrap_integration.py) uses
the production SQL and adapter with an explicitly relocated owned dataset and
job namespace. Its temporary tables have destination schemas, ownership labels
and two-hour expiry. Preflight is replaced by fixture ownership checks after
schema-verified setup; IAP identities are explicit mocks. Consequently it verifies
native SQL/SDK/job coordination and adapter recovery, not deployed IAP/IAM,
production secret bindings or replica admission. No production scope is exposed
as configurable application input.

```bash
.venv/bin/python -m src.validation.signing_bootstrap_integration \
  --run --output-dir data/generated/signing_bootstrap_verification
```

All **13 native cases passed** on 2026-10-11 in
[the retained verification report](../data/generated/signing_bootstrap_verification/39f66d3306b246feb614abd239b16b6d.json):
atomic genesis, authoritative cold read, discarded-response/restart recovery,
changed-manifest denial, concurrent identical callers, conflicting owners,
rollback without partial state, positive-rollback retry/commit blocking,
UNKNOWN predecessor blocking, post-commit failure, existing orphan-state denial,
three-slot rollback cap and preservation of unrelated policy rows. Both concurrent
cases recorded one native job and one successful child commit. The fixture dataset
was deleted. The largest bootstrap script billed 125,829,120 bytes, within its cap.

Read-only `src.ingestion.setup_signing_tables --check` verified all four serving
tables with zero rows and created nothing. Python syntax, local documentation
links and dependency consistency passed. JSON reports are audit artifacts, not
serving authority. No serving controls, users, business data, secrets or IAM were
written. No default activation or deployed IAP/HTTP verification occurred.

The affected-flow test command adds `tests.test_signing_bootstrap` and
`tests.test_signing_bootstrap_integration` to the exact command recorded in
[the writer verification](SHARING_PUBLICATION_WRITER.md#verification).

## Fixed next milestone

**4.2 is complete within its implementation and isolated native verification
scope.** Real serving bootstrap remains an explicit deployment action with trusted
owner/resource pins; it was not performed by this milestone. Shared execution
remains disabled. The seven Priority 4 IDs remain unchanged: **2/7 complete,
5/7 remaining**.

Next, **4.3 — Critical / Must have — shared pilot**
connects fresh BigQuery reads, pinned Secret Manager loading and serialized
state/sealer replacement. It verifies real project/resource bindings, restart
floors, rotation and revocation before activation. Existing IDs 4.4–4.7 retain
their runtime/API, evidence/audit, durable writer and protected UI scope.
