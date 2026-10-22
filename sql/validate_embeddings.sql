-- Active views enforce current claim eligibility and keep visual inspiration separate.
WITH texts AS (
  SELECT * FROM `project-661d501a-e709-4276-a26.ci_brand_memory.poc_generation_text_embeddings`
  UNION ALL
  SELECT * FROM `project-661d501a-e709-4276-a26.ci_brand_memory.poc_visual_reference_embeddings`
), images AS (
  SELECT * FROM `project-661d501a-e709-4276-a26.ci_brand_memory.poc_image_embeddings`
), vectors AS (
  SELECT vector, dimension FROM texts UNION ALL SELECT vector, dimension FROM images
)
SELECT
  (SELECT COUNT(*) FROM texts WHERE retrieval_status!='VISUAL_REFERENCE') knowledge_vectors,
  (SELECT COUNT(*) FROM texts WHERE retrieval_status='VISUAL_REFERENCE') visual_vectors,
  (SELECT COUNT(*) FROM images) image_vectors,
  (SELECT COUNT(*) FROM images WHERE source_system='BEAUTYFEEDS') product_vectors,
  (SELECT COUNT(*) FROM images WHERE source_system='SYNTHETIC_POC') campaign_vectors,
  (SELECT COUNT(*) FROM vectors WHERE ARRAY_LENGTH(vector)!=dimension
    OR EXISTS(SELECT 1 FROM UNNEST(vector) v WHERE IS_NAN(v) OR IS_INF(v))
    OR (SELECT SUM(v*v) FROM UNNEST(vector) v) <= 0) invalid_vectors,
  (SELECT COUNT(*) FROM texts WHERE retrieval_status='REVIEW_ONLY'
    OR chunk_type IN ('CLAIM_REVIEW_NOTE','DAILY_SALES')
    OR (sku IS NOT NULL AND NOT REGEXP_CONTAINS(sku,r'^CI-SKU-00(0[1-9]|1[0-9]|20)$'))
    OR (claim_id IS NOT NULL AND NOT EXISTS(
      SELECT 1 FROM `project-661d501a-e709-4276-a26.ci_brand_memory.poc_approved_product_claims` c
      WHERE c.claim_id=texts.claim_id AND c.sku=texts.sku))) invalid_text_scope,
  (SELECT COUNT(*) FROM texts WHERE retrieval_status='VISUAL_REFERENCE'
    AND (sku IS NOT NULL OR claim_id IS NOT NULL OR approval_scope!='VISUAL_INSPIRATION_ONLY'
      OR brand!='Naked & Thriving' OR source_system!='OFFICIAL_BRAND_WEBSITE')) invalid_visual_scope,
  (SELECT COUNT(*) FROM images WHERE SAFE_CAST(gcs_generation AS BIGNUMERIC) IS NULL
    OR SAFE_CAST(gcs_generation AS BIGNUMERIC)<=0 OR gcs_uri IS NULL
    OR NOT REGEXP_CONTAINS(sku,r'^CI-SKU-00(0[1-9]|1[0-9]|20)$')
    OR NOT REGEXP_CONTAINS(content_sha256,r'^[a-f0-9]{64}$')) invalid_image_identity,
  (SELECT COUNT(*) FROM (
    SELECT chunk_id FROM texts GROUP BY chunk_id HAVING COUNT(*)!=1)) invalid_text_duplicates,
  (SELECT COUNT(*) FROM (
    SELECT source_id,source_system FROM images GROUP BY source_id,source_system HAVING COUNT(*)!=1)) invalid_image_duplicates,
  (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_brand_memory.poc_generation_knowledge_chunks` k
    WHERE NOT EXISTS(SELECT 1 FROM texts t WHERE t.chunk_id=k.chunk_id AND t.content_sha256=k.content_sha256))
    + (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_brand_memory.poc_visual_reference_chunks` k
    WHERE NOT EXISTS(SELECT 1 FROM texts t WHERE t.chunk_id=k.chunk_id AND t.content_sha256=k.content_sha256)) invalid_missing_text;
