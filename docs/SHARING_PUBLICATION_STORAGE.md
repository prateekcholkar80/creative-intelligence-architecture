# Signing publication storage and authoritative read design

**4.1 — High / Must have — shared pilot**, completed as an offline design on
2026-10-11. The deliverables are
[four explicit table definitions](../src/application/sharing_publication_schema.py)
and a [pure snapshot projection contract](../src/application/sharing_publication_read_contract.py).
That design step did not create tables or perform I/O. The subsequent
[4.2 resource/read slice](SHARING_PUBLICATION_READER.md) now creates/verifies the
four control resources and implements strict decoding and authoritative SQL/SDK
reads. The [fenced writer slice](SHARING_PUBLICATION_WRITER.md) is implemented;
13 isolated native mutation cases pass. [Controlled bootstrap](SHARING_SIGNING_BOOTSTRAP.md)
is also implemented and natively verified, completing 4.2's documented scope. Key loading/runtime swaps
remain in 4.3. Shared execution stays disabled.

## Scope and table definitions

The proposed tables belong to `project-661d501a-e709-4276-a26.ci_application` in
`asia-south1`. Storage version is `sharing-publication-storage-v1`; the existing
publication format remains `sharing-publication-v1`. The policy ID is the fixed
`sharing-signing` service namespace. These controls apply to the signing service,
not to a particular marketing workspace or SKU. They have no synthetic business
provenance label. The eleven `campaign-workflow-v1` table definitions and their
installed execution-profile fingerprints are unchanged.

| Proposed table | Logical key | Contents and update rule |
| --- | --- | --- |
| signing_publications | policy_id, revision | Immutable revision, nullable genesis parent, publication/config digests, status/time, canonical configuration text, history count and publisher/command/recording stamps. Append one complete publication with its history and head in the same fenced transaction. |
| signing_heads | policy_id | Exactly one current publication revision/digest/time reference, independent control_revision mutation fence and publisher/command stamps. Conditional update against the expected predecessor/fence; never select a winner among duplicates. |
| signing_key_history | policy_id, publication_revision, key_id | Immutable complete history for each publication: publication digest, key/profile IDs, typed UTC issuance/retention times, revocation, numeric secret version and complete typed profile/settings/fingerprint/handler. History remains after active-configuration retirement. |
| signing_checkpoints | policy_id, consumer_id | Current durable revision/digest floor, independent checkpoint CAS revision, latest process instance, writer/command and update time. Only monotonically advance after authoritative validation; never store readiness, a lease, keys or a load-success flag. |

The schema module specifies every field, type, mode, nested profile and clustering
column. All fields are REQUIRED except a publication's nullable `parent_digest`.
Revisions and numeric secret versions use INTEGER/INT64, not floating-point
inference. `history_count` must equal the exact associated row count. Publisher,
writer and command fields are trusted service provenance, not user assertions.
Publication stamps are immutable; checkpoint stamps describe only its latest
update, not an append-only checkpoint audit history. No fake workspace audit
scope or additional audit store is implied.

Logical keys are application invariants, not SQL uniqueness enforcement.
BigQuery does not enforce primary/foreign keys, so readers must detect duplicates
and writers must validate uniqueness and conditional mutation counts. These
definitions install no unenforced key declarations.
[Google BigQuery key constraints](https://docs.cloud.google.com/bigquery/docs/primary-foreign-keys).

The head's `control_revision` is distinct from its publication `revision`.
4.2 must increment the control fence for publication/checkpoint mutations against
an existing singleton head, including first creation of a consumer checkpoint.
This prevents treating an insert into an absent checkpoint row as a lock.
Initial genesis/head bootstrap needs controlled single ownership before replica
admission; concurrent empty-table insertion is not uniqueness enforcement.
The fields define the intended fencing boundary, not a proven transaction lock.
Native conflict/rollback checks and bootstrap implementation were completed in
the subsequent 4.2 slices; see the [bootstrap evidence](SHARING_SIGNING_BOOTSTRAP.md).

`config_canonical_json` is a STRING containing only the metadata payload described
by `secret_config_digest`: project ID/number, region, policy version, active-key
ID, sorted key/profile/version metadata. The future codec must preserve exact
bounded canonical UTF-8, reject duplicate/unknown keys and malformed values, and
reconstruct the existing typed configuration. It must recompute `config_digest`
and the full `SigningPublication.digest` from decoded metadata and complete history,
then compare every header projection. Do not accept a stored hash without
recomputation. STRING is deliberate: BigQuery JSON does not preserve object-member
order or the original numeric string representation.
[Google BigQuery JSON type](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/data-types#json_type).

STOPPED retains the last valid configuration text but records active-key
revocation in history. Reconstructed stopped metadata must never reach the secret
loader. Expired ACTIVE metadata can still be read and pinned as a floor; the
refresh contract independently refuses readiness. Neither status nor publication
time establishes that keys were loaded or that another replica observed the stop.

## Layout and retention

All four tables are clustered by policy ID, with revision/key or consumer columns
as specified in the manifest. They are unpartitioned: revision/identity reads and
durable history must work without a recent-date filter that hides a retained
anchor. There is no automatic table/partition expiration or history pruning.
Resource setup in 4.2 must reject inherited dataset expiration or incompatible
existing tables rather than silently altering them.

Keep publication bodies, complete per-revision history and checkpoint floors
indefinitely in this v1 design. The current history cap remains 64 keys and the
existing 64 KiB canonical-payload limits; exhaustion fails closed. Do not reset
revision numbers, retire tombstones, delete a checkpoint or change consumer ID
to bypass the floor. A future retention/migration proposal is a scope change,
not a task added to the current milestone. Secret-byte retention/destruction is
separate: this step neither extends secret lifetime nor authorizes deletion.

Inspect the authoring manifest without ADC or cloud calls:

```bash
.venv/bin/python -m src.application.sharing_publication_schema
```

This output is metadata for review, never a local serving fallback. The original
4.1 design supplied no setup command; use the subsequent
[4.2 setup/reader instructions](SHARING_PUBLICATION_READER.md) for the implemented
setup command.

## Authoritative read protocol for 4.2

The following is the binding protocol for the future reader. The implemented
validator checks supplied projections; it cannot establish that the protocol ran.

1. **Bind trusted scope.** Fix project/dataset/region, storage/publication versions
   and policy namespace in server composition. Verify actual dataset/table
   identity, recursive schemas, layout and absence of expiration. Verify the
   expected numeric secret project belongs to the configured project before
   activation; it must not come from a request. Use a configured stable
   `consumer_id` shared by the deployment's replica group so replacements can
   find the same durable floor. Each process still has a fresh `instance_id` and
   read generation. A consumer ID is not a user identity or permission grant.
2. **Invalidate before I/O.** Serialize `begin_refresh` in the future runtime,
   clearing real readiness/material and capturing its exact ticket and known
   publication. 4.3 implements that serialization. The read request binds the
   trusted consumer, numeric project, ticket and known-head revision/digest.
3. **Capture one snapshot.** Use a bounded read-only multi-statement transaction
   in a single BigQuery job, without session or query cache. Capture
   `CURRENT_TIMESTAMP()` inside that transaction as `observed_at`; do not stamp
   completion time onto an old read. All referenced control tables must be native
   tables. BigQuery transaction reads share a consistent snapshot, and transaction
   time functions return the start time.
   [Google BigQuery transactions](https://docs.cloud.google.com/bigquery/docs/transactions).
4. **Count before selecting.** Count every head row for the policy, including
   malformed/unsupported versions; require exactly one. Count every checkpoint
   for the consumer/policy; allow zero or exactly one. No `ORDER BY ... LIMIT 1`,
   `DISTINCT`, version filter or aggregation may hide duplicate control records.
   Count publications above the selected head and require zero. An unheaded
   publication is an inconsistent control state, not an alternative winner.
5. **Choose the bounded interval.** With a known in-memory publication, include
   that exact anchor and every revision through the current head. Otherwise use
   the existing trusted checkpoint anchor, or genesis if no checkpoint exists.
   Include an older checkpoint publication separately if it lies below the known
   anchor. Never jump a running replica over missing transitions because another
   process advanced the checkpoint. Cold genesis reads need the complete chain.
6. **Return complete counts and records.** Require 1–16 consecutive interval
   publications; at most one extra older checkpoint body gives a maximum of 17
   returned publications. Count the selected revision set before truncation and
   reject duplicate revisions. Return complete history for each selected body,
   including the older checkpoint anchor: at most 17 × 64 = 1,088 history rows.
   Compare each publication's declared history count with actual rows. Aggregate
   counts and bounded row projections must come from the same transaction.
   Oversized result sets must fail, not be silently truncated or partially used.
7. **Decode and validate.** Strictly decode exact schema shapes and canonical
   configuration bytes; reject malformed versions/counters/timestamps/bindings.
   Reconstruct publication bodies and hashes. Check checkpoint consumer, digest,
   time and floor, unique head body/digest/time, complete interval, every parent
   transition and exact key-history projections. The validator sorts validated
   interval rows only after checking uniqueness; transport order has no authority.
8. **Preserve freshness and failure.** Admit only
   `observed_at <= now < observed_at + max_age`, using bounded differences.
   Default maximum age is 30 seconds (supported range 1–300). BigQuery read defaults
   are 64 MiB maximum billed bytes, 20-second job timeout and 20-second result wait;
   supported upper bounds are 1 GiB, 60 seconds and 60 seconds. Disable SDK query/job
   retries; a new explicit refresh gets a new read ticket. Correlate the completed
   job's project, region, query/configuration and ticket before projecting rows.
   Credential discovery and provider cancellation are not overall host deadlines;
   those remain in 4.4. Result completion must never extend freshness.

If a known/genesis interval is longer than 16, return a history-required failure.
Do not page across changing snapshots, pick the latest 16, silently adopt a newer
checkpoint for a running process or load old cached keys. A cold process may use
an existing authorized checkpoint; creating/advancing it requires 4.2's controlled
writer and fully validated chain. Any recovery needing an unsupported history
bootstrap remains blocked until explicitly designed, rather than weakening v1.

Byte limits are separate from row/result bounds. BigQuery's maximum-billed-bytes
setting controls query admission; a row LIMIT does not reliably cap scanned bytes.
Clustering is an optimization, not permission to ignore byte-limit failures.
[Google BigQuery cost controls](https://docs.cloud.google.com/bigquery/docs/best-practices-costs).

## Checkpoint trust and failure behavior

A checkpoint refers to a publication body/history in BigQuery; a revision/hash
alone cannot authenticate its creator or prove its predecessor chain. Only an
authorized service writer may create it after genesis-to-head validation or
advance it from an existing validated anchor with conditional revision checks.
4.2 must reject rollback/conflict, reconcile uncertain commits and prove these
write rules with isolated checks. A marketing Admin role alone does not grant
authority to alter global signing controls. IAM/bootstrap and actual identities
remain undeployed; no permissions are assigned by this design.

The future runtime must persist the validated higher floor before key loading
or readiness. Failed loading preserves that floor. The pure read observation
does not perform that write and never reports READY. A cold checkpoint starts
COLD; a running process retains its known anchor and requires consecutive updates.
An older checkpoint's body is checked independently; trusted existing anchors
avoid replaying their already validated predecessors. Privileged database restore
or malicious authorized-writer rollback cannot be excluded by hashes alone.

| Condition | Required behavior |
| --- | --- |
| Missing/duplicate head or checkpoint, count mismatch, unexpected row | Fail closed; no winner selection or fallback |
| Missing/conflicting intermediate revision, inconsistent head/history, unheaded publication | Fail closed; require authoritative recovery |
| Wrong scope/version/project binding, malformed configuration or changed digest | Fail closed; generic safe error with no raw metadata/token logging |
| Missing referenced checkpoint body, future checkpoint or floor above head | Fail closed; preserve known floor |
| Stale/future observation, timeout, denied permission, unavailable BigQuery or byte-limit failure | No readiness, partial result or automatic stale retry |
| Valid STOPPED head | Pin its revision; never load keys or issue intents |
| Valid expired ACTIVE head | Pin metadata only; refresh contract denies readiness |
| Valid fresh ACTIVE head | Eligible for future durable-floor/load handling; read acceptance alone grants no execution |

No client-supplied checkpoint, JSON export, Secret Manager list response or cached
application record can replace these authoritative controls. Cross-replica
instantaneous revocation and in-flight fencing remain unproven until 4.3.

## Verification and fixed next step

All **443 affected offline tests passed**, including **33 new storage/read-contract
tests**. The schema/publication/read subset accounts for **92** of these tests. They verify separation from the eleven
workflow tables, explicit nested fields, metadata copying, retention, duplicate
and count rejection, chain/head/history matching, checkpoint boundaries, overflow
bounds, scoped tickets and original freshness. No SDK cloud clients are created
by the schema/contract. The actual decoder, SQL, reader, publisher/checkpoint
writes, real-resource setup and native transaction semantics remain unverified.

```bash
.venv/bin/python -m unittest tests.test_sharing_publication_storage tests.test_workflow_schema tests.test_sharing_publication
```

The final affected run, after the independent control-fence field was added:

```bash
.venv/bin/python -m unittest \
  tests.test_sharing_publication_storage tests.test_workflow_schema \
  tests.test_setup_workflow_tables tests.test_sharing_publication \
  tests.test_sharing_secret_loader tests.test_sharing_signing \
  tests.test_sharing_retry_integration tests.test_sharing_integration \
  tests.test_sharing_retry_executor tests.test_sharing_retry \
  tests.test_sharing_receipt tests.test_sharing_executor tests.test_sharing_query \
  tests.test_sharing_coordinator_contract tests.test_access_reader tests.test_identity
```

All 443 cases passed in 1.947 seconds. No full suite or native/cloud probe was
rerun; there were no schema/data writes, secret access/provisioning, model calls,
IAM/user/approval changes, deployment or default-execution changes.

The subsequent [4.2 resource/read implementation](SHARING_PUBLICATION_READER.md)
now covers setup, strict codecs and authoritative reads. Next within
**4.2 — Critical / Must have — shared pilot** is authorized fenced
publication/checkpoint persistence and duplicate/conflict/unknown-commit checks.
There are no
additional milestones. See the
[fixed Priority 4 baseline](IMPLEMENTATION_PLAN.md#fixed-remaining-priority-4-milestones).
