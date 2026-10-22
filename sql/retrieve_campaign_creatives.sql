-- Read-only exact cosine search over CURRENT eligible text vectors.
-- Parameters: sku STRING, query_vector ARRAY<FLOAT64>, model STRING,
-- dimension INT64, location STRING, top_k INT64.
-- Filter eligibility and canonical identity before ranking. Logical eligibility
-- views cannot be the base of VECTOR_SEARCH; COSINE_DISTANCE preserves their
-- current policy checks without creating a second authoritative search table.
WITH candidates AS (
  SELECT k.* EXCEPT(vector), c.campaign_name,c.objective,c.channel,c.audience,
    c.start_date,c.end_date,a.headline,a.body_copy,a.call_to_action,a.visual_description,
    a.gcs_uri,a.source_image_uri,a.sha256 AS banner_sha256,
    p.product_name,
    COSINE_DISTANCE(k.vector,@query_vector) AS distance
  FROM `project-661d501a-e709-4276-a26.ci_brand_memory.poc_generation_text_embeddings` k
  JOIN `project-661d501a-e709-4276-a26.ci_creative.campaign_master` c
    ON c.campaign_id=k.campaign_id AND c.sku=k.sku
  JOIN `project-661d501a-e709-4276-a26.ci_creative.campaign_assets` a
    ON a.asset_id=k.asset_id AND a.campaign_id=c.campaign_id AND a.sku=c.sku
  JOIN `project-661d501a-e709-4276-a26.ci_product.product_master` p
    ON p.sku=k.sku AND p.brand=k.brand
  WHERE k.sku=@sku AND k.chunk_type='CAMPAIGN_CREATIVE'
    AND k.source_system='SYNTHETIC_POC' AND k.retrieval_status='POC_CONTEXT'
    AND k.approval_scope='SYNTHETIC_POC_ONLY' AND k.claim_id IS NULL
    AND k.model_id=@model AND k.dimension=@dimension
    AND k.model_location=@location AND k.task_type='RETRIEVAL_DOCUMENT'
    AND c.source_system='SYNTHETIC_POC' AND a.source_system='SYNTHETIC_POC'
    AND p.source_system='BEAUTYFEEDS' AND a.generator_version='template-v1'
    AND a.gcs_uri IN UNNEST(k.image_uris)
    AND a.source_image_uri=p.primary_image_uri AND c.product_image_uri=p.primary_image_uri
    AND c.headline=a.headline AND c.body_copy=a.body_copy AND c.call_to_action=a.call_to_action
    AND k.content_sha256=TO_HEX(SHA256(k.chunk_text))
    -- Reject snapshot descriptions if campaign metadata/copy changed afterwards.
    AND k.chunk_text=CONCAT('Type: CAMPAIGN_CREATIVE. Brand: ',p.brand,
      '. Status: POC_CONTEXT. Scope: SYNTHETIC_POC_ONLY.\n',
      'SYNTHETIC_POC: not actual historical business evidence.\n',
      'Campaign: ',c.campaign_name,'. Dates: ',CAST(c.start_date AS STRING),
      ' through ',CAST(c.end_date AS STRING),'. Channel: ',c.channel,'. Audience: ',c.audience,
      '. Objective: ',c.objective,'. Headline: ',a.headline,'. Copy: ',a.body_copy,
      '. CTA: ',a.call_to_action,'. Template description: ',a.visual_description,
      '. This is a synthetic scenario, not actual campaign history.')
)
SELECT * FROM candidates ORDER BY distance,chunk_id LIMIT @top_k;
