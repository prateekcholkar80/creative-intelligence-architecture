SELECT A.*, E.embedding_id, E.vector, E.model_id, E.model_location,
  E.dimension, E.task_type, E.created_at embedding_created_at,
  'GENERATED_POC_VISUAL_REFERENCE' retrieval_status,
  'NOT_HISTORICAL_PERFORMANCE_OR_BENEFIT_EVIDENCE' usage_scope
FROM `project-661d501a-e709-4276-a26.ci_brand_memory.poc_generated_banner_assets` A
JOIN `project-661d501a-e709-4276-a26.ci_brand_memory.image_embeddings` E
  ON E.source_id=A.asset_id AND E.sku=A.sku AND E.source_system=A.source_system
    AND E.gcs_uri=A.gcs_uri AND E.gcs_generation=A.gcs_generation AND E.content_sha256=A.sha256
    AND E.source_reference=CONCAT('project-661d501a-e709-4276-a26.ci_creative.generated_campaign_assets#asset_id=',A.asset_id)
WHERE E.modality='IMAGE' AND E.campaign_id IS NULL AND E.task_type='IMAGE'
  AND E.model_id='{model}' AND E.dimension={dimension} AND E.model_location='{location}'
  AND E.generator_version='{version}' AND ARRAY_LENGTH(E.vector)=E.dimension
  AND NOT EXISTS(SELECT 1 FROM UNNEST(E.vector) value WHERE IS_NAN(value) OR IS_INF(value))
  AND (SELECT SUM(value*value) FROM UNNEST(E.vector) value)>0
