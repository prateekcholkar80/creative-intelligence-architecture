-- Read-only lookup of the original primary image and its current embedding provenance.
-- Parameter: sku STRING.
SELECT p.sku,p.brand,p.product_name,a.asset_id,a.asset_type,a.asset_role,a.is_primary,
  a.gcs_uri,a.mime_type,a.width,a.height,a.source_system,a.source_url,a.asset_description,
  e.embedding_id,e.content_sha256,e.gcs_generation,e.model_id,e.model_location,e.dimension,e.task_type
FROM `project-661d501a-e709-4276-a26.ci_product.product_master` p
JOIN `project-661d501a-e709-4276-a26.ci_product.product_assets` a USING(sku)
JOIN `project-661d501a-e709-4276-a26.ci_brand_memory.poc_image_embeddings` e
  ON e.source_id=a.asset_id AND e.sku=a.sku AND e.gcs_uri=a.gcs_uri
    AND e.source_system=a.source_system
WHERE p.sku=@sku AND p.source_system='BEAUTYFEEDS' AND a.source_system='BEAUTYFEEDS'
  AND a.is_primary AND a.gcs_uri=p.primary_image_uri;
