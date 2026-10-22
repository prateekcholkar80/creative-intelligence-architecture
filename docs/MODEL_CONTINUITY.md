# Configured campaign model migration

Completed: 2026-10-10. Priority 1 / M1.

The configured campaign planner and request interpreter now use **gemini-3.5-flash** in **asia-south1**. Text and image embedding models retain their existing contracts. BigQuery remains the authoritative evidence/vector store; GCS retains original images. The validation created local review artifacts and made no cloud business-data, approval or asset-registration writes.

## Candidate decision

| Candidate | Decision |
| --- | --- |
| gemini-2.5-flash | Replaced ahead of its documented October 20, 2026 retirement. Existing legacy unit coverage remains; it is not an automatic runtime fallback. |
| gemini-3.5-flash | Selected after live project/endpoint checks and the existing grounded workflow passed. Google documents image input, structured output, asia-south1 availability and a retirement date of May 19, 2027 or later. |
| gemini-3.5-flash-lite | Not selected: its documented endpoints are global/us/eu, rather than the required asia-south1 endpoint. No application traffic was routed to it. |

References checked on 2026-10-10: [model lifecycle](https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/model-versions), [Flash capabilities/locations](https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini/3-5-flash), [Flash-Lite locations](https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini/3-5-flash-lite).

## Compatibility changes

Planning uses LOW thinking and at most 4,096 output tokens; interpretation uses MINIMAL thinking and at most 1,024 output tokens. Both retain the seed and bounded application client timeout/retry settings. Gemini 3 requests omit legacy temperature overrides and numeric thinking budgets. The tested Google Gen AI SDK is 2.28.0; requirements now specify the compatible 2.x range starting at 2.28. Output reproducibility is not guaranteed. See [Google's migration guidance](https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/guides/gemini-3-5-flash).

The live API rejected empty enum members in both the interpretation schema and optional supporting-copy schema. Their model-facing representations now use nullable values. Code converts only permitted null values to the established empty-string representation: missing campaign preferences still require clarification; an empty body still means no supporting copy. Required identity/copy options and unexpected fields remain strictly validated.

Both stages explicitly configure the existing four safety categories, disable automatic function calling, reject prompt/candidate safety blocks, and require exactly one STOP-completed response before parsing JSON. Valid JSON returned with MAX_TOKENS or a blocking verdict is rejected. Fresh BigQuery approval/product rechecks and original-image verification continue before outputs are returned.

## Verification evidence

The full suite passed **315 tests**, including Streamlit AppTest, after the active configuration was switched to the tested candidate. Live checks passed:

| Case | Verified behavior |
| --- | --- |
| Complete campaign request | CI-SKU-0001 conversion/paid-social preferences resolved correctly. |
| Second SKU and channel | CI-SKU-0002 awareness/email request with no website inspiration resolved correctly. |
| Missing channel | Clarification required; no default channel was invented. |
| Unsupported benefit claim | Cure claim classified as unsupported; generation request blocked. |
| Follow-up channel change | Earlier unsupported claim retained until explicitly removed. |

The candidate saw four actual, verified product/campaign images and 47 evidence references for the CI-SKU-0001 preview. The resulting 1200 x 628 banner passed original-image/copy/geometry and current-approval checks and was visually inspected. It remains **unregistered and unreviewed**, with SYNTHETIC_POC provenance.

Artifacts: [validation.json](../data/generated/model_validation/20261010-gemini35-verified/validation.json), [banner.png](../data/generated/model_validation/20261010-gemini35-verified/banner.png), [campaign brief](../data/generated/model_validation/20261010-gemini35-verified/campaign_brief.txt).

Banner SHA-256: `e0b967eea4f61026d1af80c2aa9043161bda106f589ee855618b821eb8517a6e`.

Earlier failed attempts remain audit artifacts: sandbox authentication was blocked, then live validation identified the two empty-enum incompatibilities. The successful report records `configured_model_changed=false` because the candidate runner does not edit settings; the application configuration was promoted only after that run passed and its settings were compared with the tested candidate.

## Repeat validation

```sh
.venv/bin/python -m unittest discover -s tests
.venv/bin/python -m src.validation.model_continuity --output-dir data/generated/model_validation/<new-run-name>
```

The cloud command uses ADC, reads all 20 existing products from BigQuery, invokes five interpretation requests and one grounded planner request, and writes a unique local directory. An existing output directory stops before billable work. The artifact is validation evidence, not a serving fallback or another authoritative database. The runner tests the explicit candidate profile; normal UI generation reads the active campaign configuration.

This is a bounded continuity check, not a labeled production RAG benchmark or evidence of campaign effectiveness. Formal E1–E5, the published model catalog/user selector, full policy records, team authentication, observability and complete cost accounting remain in the implementation plan. Continue with priority 2: ownership/access and durable workflow schema design.
