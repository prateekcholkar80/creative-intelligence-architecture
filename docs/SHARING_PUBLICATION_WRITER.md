# Signing publication and checkpoint writer

**4.2 — Critical / Must have — shared pilot.** This slice implements an opt-in
server writer against the four existing signing-control tables. It requires an
existing validated singleton head. The subsequent
[controlled bootstrap](SHARING_SIGNING_BOOTSTRAP.md) is implemented and verified
within 4.2; empty serving tables cannot admit writes through this
adapter. Runtime sealer replacement and live resource binding remain in 4.3.
Shared execution remains disabled.

## Authorization and admission

[ProtectedSigningWriter](../src/application/signing_control_writer.py) uses the
existing signed IAP identity verifier. Trusted configuration must explicitly
enable writes and supply global operator principal IDs for publication or
principal/consumer grants for checkpoints. Marketing workspace Admin membership,
email headers, domains and local ADC do not grant these permissions. ADC supplies
the BigQuery client's cloud identity; IAP supplies the operator identity.

Initial admission captures a fresh bounded authoritative read through
[BigQuerySigningReader](../src/application/sharing_publication_reader.py), verifies
the exact request scope and constructs a validated immutable plan. Publication
transitions retain key/version/profile history and the exact parent digest.
Checkpoint targets come from the captured head, never a requested floor/hash.
The server fixes project, dataset, region and policy; no browser-supplied table
identifiers or implicit personal-account identities are accepted.

One actor/kind/consumer/command has one deterministic original native job ID.
Repreparation of the same issued business input preserves the original envelope,
including its timestamp, target and five-minute admission window; changed inputs
are rejected. Returning an already issued plan is historical admission, not a
fresh read or evidence that the current state remains unchanged. Submission still
requires fresh authentication and exact same-process issuance. Only the first
submission can use the initial absence of a native job to dispatch. Admission is
consumed before uncertain I/O; subsequent calls recover the original job only.

The issuance/admission registry is process-local. It is not a durable queue,
cross-process command registry, HTTP route or runtime restart mechanism. A
restarted process cannot use another process's plan to submit. It may perform
authorized read-only recovery with the exact trusted original envelope. Durable
operation/reconnect orchestration remains under the existing runtime/workflow
milestones; callers must not manufacture an updated envelope to resolve UNKNOWN.

## Atomic persistence

[The SQL script](../sql/signing_control_write.sql) renders only installed table
identifiers and explicit destination-schema columns. Typed JSON projections are
derived from those definitions; there is no pandas inference, business-data
loader, secret material or unrelated DELETE/MERGE.

The transaction checks the singleton head, consumer checkpoint uniqueness,
referenced publication bodies, absence of unheaded publications and the exact
captured parent/history/floor rows. It conditionally increments the existing
head's independent control revision before any insert. Both publishers and
checkpoint writers use that same fence, including first checkpoint creation.
This avoids treating an INSERT into an absent row as uniqueness enforcement.

Publication application appends the complete body and history and moves the
head atomically. Stored publisher/command/recording stamps remain immutable.
Exact replay reuses those records; changed same-revision replay is rejected.
Checkpoint application advances the current floor with checkpoint CAS revision
and the shared control fence. Exact replay retains its stamps; a compatible
already higher floor is satisfied without a downgrade or new fence mutation.
Checkpoint rows record the latest writer/command, not an append-only checkpoint
audit history. No workspace audit scope or new audit table is implied.

All writes remain scoped to the fixed policy and, for checkpoints, the configured
consumer. Other policy/consumer records are preserved. The native conflict
behavior is verified separately from the offline contract: BigQuery transactions
provide snapshot isolation and conflicting table mutations can be cancelled.
[Google's transaction documentation](https://docs.cloud.google.com/bigquery/docs/transactions).

## Limits and recovery

Each plan freezes canonical source/proposal JSON, with a 2 MiB bound per payload.
The bounded reader still requires a complete chain within its installed limits;
operators must maintain their consumer checkpoint rather than bypassing a
history-required failure or resetting a floor. The writer uses a separate
**512 MiB maximum billed bytes per script**, a **60-second native job timeout**
and **20-second statement timeout**. Read limits remain unchanged. The first
native run demonstrated that the sharing script's inherited 64 MiB cap exhausted
across statement billing minima before the fence. The larger cap is an explicit
upper bound, not an estimate of actual data size or permission for blind retries.

Schema/layout preflight is fresh, query cache and sessions are disabled, and SDK
query/job retries are disabled. The result contract verifies exact publication
and history, immutable stamps, fence/CAS progression, scope and timestamps.
Timeouts and malformed/missing responses do not imply rollback.

Recovery reads only the original correlated native job. Running, missing or
mismatched jobs stay UNKNOWN. A failed parent requires positive, bounded,
correlated child COMMIT/ROLLBACK evidence before classification; parent failure
alone cannot prove noncommit. A failure after COMMIT can report COMMITTED without
a validated result receipt. These historical outcomes do not establish current
runtime readiness, current floor eligibility or successful key loading.

## Verification

Offline tests use real BigQuery SDK objects with mocked RPCs, ephemeral real
ES256 signatures and negative authorization/scope/window/recovery cases. The
affected-flow suite includes reader/storage/publication, workflow schema/setup,
secret/signing/retry/executor/receipt/query/coordinator, access reader and identity.
All **519 affected offline tests passed**, including **31 writer tests** and
**six new probe-safety tests** (37 new cases relative to the resource/read slice).
The run completed in 2.284 seconds. No full suite or model evaluation was run.
Python syntax, local documentation links and installed dependency consistency
were also checked. Offline tests do not prove native SQL or deployed IAM.

The affected test command was:

```bash
.venv/bin/python -m unittest \
  tests.test_signing_control_writer tests.test_signing_write_integration \
  tests.test_signing_storage_io tests.test_signing_read_integration \
  tests.test_sharing_publication_storage tests.test_sharing_publication \
  tests.test_workflow_schema tests.test_setup_workflow_tables \
  tests.test_sharing_secret_loader tests.test_sharing_signing \
  tests.test_sharing_retry tests.test_sharing_retry_integration \
  tests.test_sharing_retry_executor tests.test_sharing_integration \
  tests.test_sharing_executor tests.test_sharing_receipt \
  tests.test_sharing_query tests.test_sharing_coordinator_contract \
  tests.test_access_reader tests.test_identity
```

[The opt-in native probe](../src/validation/signing_write_integration.py) creates
four explicit-schema tables in a fresh owned `asia-south1` dataset with two-hour
expiry. It uses fictitious key-free secret metadata and fixture actor IDs.
Ownership, region, labels and expiry are checked before fixture mutations and
cleanup. Sentinel records verify other policies and consumers remain unchanged.
It runs the installed SQL/query/result/job-recovery primitives; it does not
exercise a deployed IAP/HTTP writer, live IAM grants or Secret Manager access.

```bash
.venv/bin/python -m src.validation.signing_write_integration \
  --run --output-dir data/generated/signing_write_verification
```

All **13 native cases passed** on 2026-10-11 in
[the retained report](../data/generated/signing_write_verification/06a2a4b6bbe04331b922ea7234aa812a.json):
atomic publication/head/history, exact publication replay, discarded-response
original-job recovery, changed same-revision rejection, checkpoint CAS advance,
checkpoint replay, competing-parent rejection, rollback after fence, failure
after commit, overlapping publications, overlapping first checkpoints, mixed
publication/checkpoint race and preservation of other policy/consumer rows.
The three races verify actual overlapping native transaction intervals and one
positive commit plus one positive rollback. The owned dataset was deleted.

The earlier failed native attempt exhausted the inherited 64 MiB billing limit
and positively rolled back; its owned dataset was also deleted. That failed
report remains in the same directory. The successful run's largest write job
billed 492,830,720 bytes, within the installed 512 MiB cap. This illustrates
statement billing minima rather than the amount of fixture data processed.

The read-only `src.ingestion.setup_signing_tables --check` verified all four
serving tables with zero rows and created nothing. Verification JSON is an audit
artifact, never a serving fallback or runtime permission grant. No serving control
rows, user memberships, business data, secrets, IAM or default activation changed.

## Remaining fixed sequence

1. **4.2 — Critical / Must have — shared pilot — complete in implementation/native
   fixture scope:** [controlled bootstrap](SHARING_SIGNING_BOOTSTRAP.md) now uses
   reserved native job slots, one configured owner and atomic initial state, with
   duplicate/uncertain-outcome verification. Serving bootstrap was not performed.
2. **4.3 — Critical / Must have — shared pilot:** connect fresh authoritative
   state to serialized runtime refresh and pinned live secret loading; verify
   project/resource association, restart floors, rotation and revocation.

The seven Priority 4 IDs and their scope remain unchanged. The combined
resource/read/writer/bootstrap work completes 4.2 within its documented scope;
shared execution remains disabled. Continue at 4.3.
