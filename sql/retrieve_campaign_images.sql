-- Read-only recheck of ranked selections against current BQ eligibility/metadata.
-- Parameters: sku STRING, snapshot STRING, assets ARRAY<STRING>, chunks ARRAY<STRING>.
SELECT k.snapshot_id,k.chunk_id,k.content_sha256 AS creative_content_sha256,
  k.evidence_references,k.image_uris,k.retrieval_status,k.approval_scope,k.claim_id,
  p.brand,p.product_name,a.asset_id,a.campaign_id,a.sku,a.asset_type,a.asset_role,
  a.gcs_uri,a.source_image_uri,a.mime_type,a.sha256 AS banner_sha256,a.width,a.height,
  a.headline,a.body_copy,a.call_to_action,a.visual_description,a.generator_version,a.source_system,
  c.objective,c.channel,c.audience,c.start_date,c.end_date,
  e.embedding_id,e.content_sha256,e.gcs_generation,e.model_id,e.model_location,e.dimension,e.task_type
FROM `project-661d501a-e709-4276-a26.ci_brand_memory.poc_generation_text_embeddings` k
JOIN `project-661d501a-e709-4276-a26.ci_creative.campaign_master` c
  ON c.campaign_id=k.campaign_id AND c.sku=k.sku
JOIN `project-661d501a-e709-4276-a26.ci_creative.campaign_assets` a
  ON a.asset_id=k.asset_id AND a.campaign_id=c.campaign_id AND a.sku=c.sku
JOIN `project-661d501a-e709-4276-a26.ci_product.product_master` p
  ON p.sku=k.sku AND p.brand=k.brand
JOIN `project-661d501a-e709-4276-a26.ci_brand_memory.poc_image_embeddings` e
  ON e.source_id=a.asset_id AND e.sku=a.sku AND e.campaign_id=a.campaign_id
    AND e.gcs_uri=a.gcs_uri AND e.source_system=a.source_system AND e.content_sha256=a.sha256
WHERE k.sku=@sku AND k.snapshot_id=@snapshot
  AND k.asset_id IN UNNEST(@assets) AND k.chunk_id IN UNNEST(@chunks)
  AND k.chunk_type='CAMPAIGN_CREATIVE' AND k.source_system='SYNTHETIC_POC'
  AND k.retrieval_status='POC_CONTEXT' AND k.approval_scope='SYNTHETIC_POC_ONLY'
  AND k.claim_id IS NULL AND k.content_sha256=TO_HEX(SHA256(k.chunk_text))
  AND c.source_system='SYNTHETIC_POC' AND a.source_system='SYNTHETIC_POC'
  AND p.source_system='BEAUTYFEEDS' AND a.generator_version='template-v1'
  AND a.asset_type='IMAGE' AND a.asset_role='CAMPAIGN_BANNER'
  AND a.gcs_uri IN UNNEST(k.image_uris)
  AND a.source_image_uri=p.primary_image_uri AND c.product_image_uri=p.primary_image_uri
  AND c.headline=a.headline AND c.body_copy=a.body_copy AND c.call_to_action=a.call_to_action;
