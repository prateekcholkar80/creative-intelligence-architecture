# Signing control setup, codec and authoritative reader

**4.2 — Critical / Must have — shared pilot**, resource/read slice implemented
on 2026-10-11. This is part of the existing 4.2 milestone. Its fenced publisher,
checkpoint persistence and native mutation/recovery exit criteria remain open.
See the [fixed plan](IMPLEMENTATION_PLAN.md#fixed-remaining-priority-4-milestones).

## Delivered behavior

- [Scoped setup](../src/ingestion/setup_signing_tables.py) requires the existing
  `ci_application` dataset in `asia-south1`. It validates all four targets before
  creation, refuses incompatible schemas/layout/expiration, creates only missing
  tables, revalidates concurrent creation and resumes partial setup. It never
  changes an existing table, writes data rows, bootstraps a head/user or changes IAM.
- [Strict codec](../src/application/sharing_publication_codec.py) checks exact
  row/nested shapes, schema/policy versions, native types, INT64 counters and UTC
  timestamps. Configuration text is capped at 65,536 UTF-8 bytes; duplicate or
  unknown JSON keys, noncanonical serialization, nonfinite numbers, scalar
  coercions and incompatible bindings fail. Reconstructed configuration tuples
  use canonical ID order. Both configuration and complete publication hashes
  are recomputed; declared history counts and exact associated records must match.
- [BigQuery reader](../src/application/sharing_publication_reader.py) preflights
  dataset and all four explicit schemas on every read, uses fixed serving table
  identifiers and parameterized values, and captures one transaction snapshot.
  A temporary table retains its result after commit. All source data operations
  are SELECTs; only the job-local temporary result is materialized.
- Complete scoped counts precede row limits and version validation. Duplicate
  heads/checkpoints, unsupported scoped control rows, publications above the
  head, missing intermediate revisions/history, mismatched hashes, future/stale
  observations and conflicting known/checkpoint anchors deny the read. Up to 16
  consecutive publications plus one older checkpoint body and 1,088 history
  records can be accepted. Oversized intervals fail rather than selecting a
  recent suffix. Configuration text exceeding the byte cap is returned as NULL
  and rejected, never truncated into usable metadata.
- Every read gets a fresh preallocated job ID, with query cache, sessions and SDK
  query/job retries disabled. The actual SDK job must match project, region,
  parent status, SQL and pinned parameters/configuration before and after bounded
  result consumption. Expected configuration is copied independently of the
  submitted SDK object. At most two top-level rows are consumed; exactly one is
  required. An absent base digest is encoded as an empty parameter and decoded
  with SQL `NULLIF`; SHA-256 digest pins cannot have that representation. This
  avoids BigQuery's omission of NULL parameter values in returned job metadata
  without relaxing exact parameter correlation.

The snapshot uses BigQuery's native transaction consistency and transaction-start
timestamp semantics. Temporary tables are supported inside transactions.
[Google BigQuery transaction documentation](https://docs.cloud.google.com/bigquery/docs/transactions).
The local BigQuery SDK is 3.46.1; query/result retry controls were checked against
the installed SDK and [official QueryJob API](https://docs.cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.job.QueryJob).

Defaults remain 64 MiB maximum billed bytes, 20-second job/statement timeout,
20-second result wait and maximum snapshot age 30 seconds. Per-RPC settings do
not establish an overall host deadline; that remains in 4.4.

## Authority and activation boundary

The four serving tables have been created and verified **empty**. Reapplying setup
creates nothing. No genesis publication or checkpoint is bootstrapped by setup.
An empty control store fails closed. The eleven workflow definitions and their
execution-profile fingerprints remain unchanged.

The codec's operator/command stamps are validated metadata, not authentication
proof. A checkpoint is trusted only under the authorized writer/IAM assumptions
in the [storage design](SHARING_PUBLICATION_STORAGE.md#checkpoint-trust-and-failure-behavior).
The reader does not establish that a numeric project number belongs to the
configured project: that association and live Secret Manager bindings must be
verified by trusted deployment setup in 4.3. Browser-supplied project numbers,
consumer IDs, tickets or known publications are not supported serving inputs.

Accepted reads return metadata observations only. They do not persist revision
floors, load key bytes, grant readiness, replace runtime sealers, authenticate
marketing users or enable shared execution. STOPPED and expired ACTIVE metadata
can be read; readiness still requires the refresh/load integration in 4.3.
There is no local JSON, stale metadata or cached-secret fallback.

## Verification commands

Read-only infrastructure check, then missing-table creation:

```bash
.venv/bin/python -m src.ingestion.setup_signing_tables --check
.venv/bin/python -m src.ingestion.setup_signing_tables --apply
```

Focused offline cases:

```bash
.venv/bin/python -m unittest tests.test_signing_storage_io tests.test_signing_read_integration
```

The [native probe](../src/validation/signing_read_integration.py) relocates the
installed SQL into a fresh owned `ci_signing_read_it_<uuid>` dataset. Its four
tables use explicit destination schemas and two-hour expiry. Fixture writes and
truncation are restricted to that owned scope; cleanup verifies project, region,
labels and expiry before deleting the dataset. It uses deliberately fictitious
numeric secret metadata and accesses no secret bytes. Offline safety tests cover
scope/ownership guards. This checks native SQL/codec behavior; it does not exercise
the deployed reader's IAM or real signing policy, writer concurrency or uncertain
mutation outcomes.

```bash
.venv/bin/python -m src.validation.signing_read_integration \
  --run --output-dir data/generated/signing_read_verification
```

Verification JSON files are audit artifacts, never a serving source. Failed runs
remain visible and must not be described as passing evidence.

## Recorded verification

All **482 affected offline tests passed**, including **39 new cases** in
`tests.test_signing_storage_io` and `tests.test_signing_read_integration`. The
affected run covered those modules plus publication storage/publication,
workflow schema/setup, secret loader/signing, sharing retry/integration/executor,
receipt/query/coordinator, access reader and identity. The final behavioral run
completed in 2.083 seconds. No full suite or unrelated model evaluation was run.
All changed Python files parsed, and all local documentation links resolved.

All **14 isolated native read cases passed** in
[the retained verification report](../data/generated/signing_read_verification/9c55361562174a21a18b7a1f0cca5848.json):
cold checkpoint chain, known anchor with older checkpoint, genesis without
checkpoint, unsupported head version, invalid control fence, changed config
digest, oversized config text, changed history binding, checkpoint above head,
duplicate heads/checkpoints, missing intermediate publication/head history and
unheaded publication. The owned fixture dataset was deleted; serving row writes
and secret access were zero. This is SQL/codec evidence, not a publisher race,
deployed reader/IAM or runtime readiness test.

Two earlier native attempts failed on exact correlation of a NULL parameter;
their owned datasets were also deleted. The failed reports remain in the same
verification directory. The empty-parameter/`NULLIF` fix was tested offline and
the third native run passed all cases. Actual table setup verified the four
serving tables empty, and a second `--apply` created no resources. No existing
products/assets/business rows, approvals, users, IAM, secrets or shared-execution
settings changed.

## Subsequent 4.2 work — implemented and verified

1. **Critical / Must have — shared pilot:** explicit global operator/consumer
   grants are implemented in the [writer slice](SHARING_PUBLICATION_WRITER.md),
   independently of marketing workspace Admin roles. [Controlled genesis
   bootstrap](SHARING_SIGNING_BOOTSTRAP.md) now uses reserved native job slots
   before replica admission; an absent-head INSERT alone cannot guarantee uniqueness.
2. **Critical / Must have — shared pilot — implemented in the writer slice:** persist complete publications/history,
   immutable publisher/command stamps and conditional head updates in one
   transaction. Validate the exact authoritative parent and increment the
   independent control fence. Reject changed same-revision replay.
3. **Critical / Must have — shared pilot — implemented in the writer slice:** persist monotonically advancing
   checkpoints with expected checkpoint CAS revision and the shared head control
   fence, including first-checkpoint creation. Preserve higher floors after failure.
4. **Critical / Must have — shared pilot — adapter implemented, 13 native cases passed:** correlate original mutation jobs and
   conservatively reconcile uncertain results without blind resubmission. Verify
   duplicate/conflicting publishers, overlapping checkpoints, rollback and
   post-commit/lost-response behavior in isolated native fixtures.

Those 4.2 criteria are complete within implementation/native fixture scope.
Continue to **4.3 — Critical / Must have —
shared pilot**, for serialized runtime refresh, pinned live secret loading and
restart/rotation/revocation verification. No new milestone IDs are introduced.
