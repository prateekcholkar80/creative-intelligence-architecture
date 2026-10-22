-- Read-only, exact cosine search over current generated-only eligibility.
-- Query text is embedded with multimodalembedding@001, not the text document model.
-- Parameters: sku STRING, query_vector ARRAY<FLOAT64>, model STRING,
-- dimension INT64, location STRING, top_k INT64.
-- Logical eligibility views are not supported as VECTOR_SEARCH bases.
SELECT asset_id,sku,embedding_id,gcs_uri,gcs_generation,sha256,knowledge_snapshot_id,review_id,review_revision,
  COSINE_DISTANCE(vector,@query_vector) AS distance
FROM `project-661d501a-e709-4276-a26.ci_brand_memory.poc_generated_banner_embeddings`
WHERE sku=@sku AND source_system='SYNTHETIC_POC'
  AND status='REGISTERED_POC_DRAFT' AND approval_scope='SYNTHETIC_POC_ONLY'
  AND real_business_approved=FALSE
  AND retrieval_status='GENERATED_POC_VISUAL_REFERENCE'
  AND usage_scope='NOT_HISTORICAL_PERFORMANCE_OR_BENEFIT_EVIDENCE'
  AND review_status='APPROVED_FOR_POC' AND review_approval_scope='POC_VISUAL_REUSE_ONLY'
  AND model_id=@model AND dimension=@dimension AND model_location=@location
  AND task_type='IMAGE'
ORDER BY distance,asset_id
LIMIT @top_k;
