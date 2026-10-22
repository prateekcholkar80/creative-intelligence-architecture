# Initial planner prompt authoring — Batch 12

**High / Must have / shared pilot — complete on 2026-10-12.**

Vertex stores the planner artifact; BigQuery will control registration, lifecycle
and runtime eligibility through the remaining Priority 5 batches. This milestone
does not activate the prompt or alter the existing runtime planner selection.

## Verified artifact

| Field | Value |
| --- | --- |
| Project | `project-661d501a-e709-4276-a26` |
| Verified project number | `1042059373096` |
| Location | `asia-south1` |
| Prompt ID | `8077808464061005824` |
| Exact version ID | `1` |
| Model | `gemini-3.5-flash` |
| Display name | `ci-campaign-planner-initial-e0bcc075a6660868` |
| Version display name | `initial-e0bcc075a6660868` |
| Thinking | `LOW` |
| Seed / maximum output tokens | `42` / `4096` |
| Function calling | Explicitly disabled; no tools |
| Output | JSON with the canonical campaign-draft-v1 schema shape |
| Safety | All four existing BLOCK_MEDIUM_AND_ABOVE categories preserved |
| Parity | `PASS` |
| BigQuery registration / activation | Pending / not performed |

Runtime contract SHA-256:
`e0bcc075a666086875aa1d6e4db678feccdf4fe36a91fd3eb638230ec008121b`.

Canonical snapshot SHA-256:
`78b7c867f499c0aab130fff55914ecce89667ac9e7190026dd9399183108340c`.

Template SHA-256:
`926ecc03ca47d6d77b4d072d0a35eb408372f32d72955902fc899bd1ab2d2305`.

## Root cause and correction

The installed Agent Platform SDK types `PromptData.tool_config` as the inner
FunctionCallingConfig. Serializing its prompt metadata directly emitted
`toolConfig: {"mode": "NONE"}`, which Vertex rejected with code 3 and
`INVALID_DATASET_METADATA`. The correct wire representation is:

```json
{"toolConfig": {"functionCallingConfig": {"mode": "NONE"}}}
```

The authoring adapter corrects only that wrapper and parses the complete message
against the native GenerateContentRequest type before submitting it. It preserves
the instruction, variables, model, generation, safety and output-schema values.
The exact reader uses the same SDK HTTP request boundary to obtain the raw numeric
version response, validates the wrapper, then converts it into the canonical
internal snapshot. It rejects missing, enabled or additional tool controls instead
of silently defaulting or dropping fields.

Vertex's original error message echoed the structured metadata. The 20,759-byte
returned error exceeded gRPC's 16,384-byte header limit, masking it as Python
ResourceExhausted/429. This does not establish a token-quota failure. The provider
status, original Python traceback and successful isolated A/B readback are retained
in `data/generated/prompt_registry_diagnostics/710af3c224c44b5a8dafdfc9bf73b65f/`.
Both disposable probe resources were deleted and absence verified.

Native version listing also rejected the display_name filter. Discovery now lists
versions only under the exact prompt, checks a complete bounded set of at most 32
unique identities and matches the version display name locally. An overflow,
duplicate, foreign parent or partial state denies authoring; no unbounded scan or
replacement create follows. Only the fixed verified numeric project alias is
normalized to the canonical project ID.

## Operator commands

Run from the repository root using ADC. No service-account key is needed.

```sh
# Offline plan; no client or mutation.
.venv/bin/python -m src.application.planner_prompt_authoring

# Read-only recovery and exact-version verification.
.venv/bin/python -m src.application.planner_prompt_authoring --recover-only

# Explicit authoring; reuses this matching artifact when it already exists.
.venv/bin/python -m src.application.planner_prompt_authoring --apply
```

The default total operation budget is 300 seconds. Each native create and
original-operation poll has a bounded RPC timeout and retries disabled. Unknown
outcomes do not trigger a replacement create. Explicit poll timeout diagnostics
retain the original operation name; read-only recovery requires an exact complete
prompt/version pair. Partial creation stops for operator investigation and cannot
create another dataset or version automatically. Deterministic names support
sequential reuse, not a distributed creation lock; this is a single-operator tool.

## Verification and remaining work

The final affected offline slice passed **279 tests**, including **45** focused
reader/parity/authoring tests. It covers all prompt-named modules and campaign
brief/generation, application/UI, workflow runtime, invocation and output contracts.
Python compilation and dependency checks passed. The full repository suite was not
run. Live create returned CREATED/PASS, independent read-only recovery returned
RECOVERED/PASS and repeated authoring returned REUSED/PASS with identical IDs/hashes.
The verification export is
`data/generated/prompt_registry_diagnostics/batch12_completed_20261012/verification.json`.
These local files are verification exports, not authoritative serving sources.

No BigQuery rows, model calls, IAM changes, approval changes or activation occurred.
The first recovery/reuse checks stopped on the unsupported version-list filter;
the corrected independent reruns passed without further creation.

The fixed remaining batches in [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) are:

1. **Batch 13 — High / Must have / shared pilot:** exact-version canonical preview,
   fresh verified IAP and BigQuery MANAGE_MODELS authorization, DRAFT registration,
   and authoritative current/history verification. ADC alone grants no application
   role. Registration does not activate a prompt.
2. **Batch 14 — Critical / Must have / shared pilot:** persist evaluations bound to
   the exact registered hash and enforce promotion gates.
3. **Batch 15 — Critical / Must have / shared pilot:** authorized lifecycle changes,
   one-active-version fencing, retirement and controlled rollback.
4. **Batch 16 — Critical / Must have / shared pilot:** resolve the BigQuery-eligible
   exact Vertex version, reverify its canonical contract and pin it to invocation.
   No latest/local fallback is permitted.
