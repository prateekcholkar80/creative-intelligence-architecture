# Prompt tables: physical resource verification

Priority 5, existing Batches 14–16 — **Critical / Must have / shared pilot**.
Physical resource setup completed on 2026-10-12. This closes the table-provisioning
gate within the existing batches; it does not add a batch or close their native
transaction, quality or activation gates.

## Resources created

Project: `project-661d501a-e709-4276-a26`. Dataset: `ci_application`.
Location: `asia-south1`.

| Batch | Table | Verified rows |
| --- | --- | ---: |
| 14 | `prompt_evaluation_controls` | 0 |
| 14 | `prompt_evaluation_control_history` | 0 |
| 14 | `prompt_evaluations` | 0 |
| 14 | `prompt_evaluation_cases` | 0 |
| 15 | `prompt_lifecycle_controls` | 0 |
| 15 | `prompt_lifecycle_history` | 0 |
| 16 | `invocation_prompt_bindings` | 0 |

The existing metadata-only setup modules were used unchanged. Before creation,
all seven targets were absent and all three setup preflights passed. Each module
was applied, and each repeated `--apply` returned `VERIFIED`, no missing tables
and `created_tables: []`.

```bash
.venv/bin/python -m src.ingestion.setup_prompt_evaluation_tables --apply
.venv/bin/python -m src.ingestion.setup_prompt_lifecycle_tables --apply
.venv/bin/python -m src.ingestion.setup_prompt_runtime_tables --apply
```

Exact destination schemas, identities, location, clustering and unpartitioned
layout passed the existing validators. Table expiration and incompatible layouts
were rejected by those checks. All five project datasets retain `asia-south1`
and no default table or partition expiration.

## Evidence and limits

- Active CLI authentication and ADC-backed BigQuery metadata/query access passed.
- **23 focused tests passed** across evaluation storage, lifecycle contracts and
  runtime query/setup. Application code and tests were unchanged.
- Inventory increased from **46 to 53 tables/views**. Before/after metadata for
  all 46 original resources matched, including schema hash, row count, etag and
  modified time. Dataset metadata also matched. This is metadata preservation
  evidence, not a hash comparison of every existing data row.
- An independently submitted, bounded read-only `COUNT(*)` query verified zero
  rows in each target, with exact native-job/config correlation. Processed and
  billed bytes were both **0**. Native job:
  `prompt_physical_tables_verify_07ff21f24d9b4cca828d0befb9681427`.
- Only missing table resources were created. No data rows, product assets, claim
  approvals, prompts, IAM, application roles or runtime settings were changed.
  No model invocation, GCS operation, deployment or serving activation occurred.

Read-only evidence exports: [before metadata](../data/generated/prompt_registry_diagnostics/physical_tables_20261012_before.json)
and [after metadata and row counts](../data/generated/prompt_registry_diagnostics/physical_tables_20261012_after.json).
These JSON files document verification; BigQuery remains authoritative.

## Fixed next steps

1. **Critical / Must have — existing Batches 14–16:** implement and run isolated
   owned-dataset native evaluation, lifecycle and dispatch-binding probes. Verify
   atomic writes, duplicate/replay behavior, expiry, replacement/rollback,
   promotion-versus-dispatch races and original-job recovery; delete only owned
   fixtures and verify serving-state preservation. Continue in 2–3 file groups.
2. **High / Must have — Batch 13:** complete real IAP and explicitly authorized
   workspace/ADMIN setup, then register the exact verified Vertex version as
   `DRAFT`. The planned personal Gmail pilot and future Workspace SSO remain
   under Priority 12. ADC does not supply application roles.
3. **High / Must have — Priority 6 and existing release gates:** establish reviewed
   benchmark/policy inputs, run measured evaluation and persist its evidence
   before authorized promotion. Runtime/adapter and deployed SSO verification
   remain required before shared activation.

Priority 5 remains **15/16 implementation milestones complete within documented
scope** (1–12 and 14–16); Batch 13 live registration is pending.
