-- Parameter: snapshot STRING. Error counters must all be zero.
WITH chunks AS (
  SELECT * FROM `project-661d501a-e709-4276-a26.ci_brand_memory.knowledge_chunks`
  WHERE snapshot_id=@snapshot
)
SELECT COUNT(*) chunk_count, COUNT(DISTINCT sku) sku_count,
  COUNTIF(retrieval_status='REVIEW_ONLY') review_only_count,
  COUNTIF(content_sha256 != LOWER(TO_HEX(SHA256(chunk_text)))) invalid_hashes,
  COUNTIF(k.sku IS NOT NULL AND (p.sku IS NULL OR p.brand != k.brand)) invalid_skus,
  COUNTIF(ARRAY_LENGTH(evidence_references)=0 OR LENGTH(TRIM(chunk_text))=0) invalid_evidence,
  COUNTIF((chunk_type IN ('CLAIM_REVIEW_NOTE','WEBSITE_CLAIM_CANDIDATE') AND retrieval_status!='REVIEW_ONLY')
    OR (chunk_type='POC_APPROVED_CLAIM' AND NOT EXISTS (
      SELECT 1 FROM `project-661d501a-e709-4276-a26.ci_brand_memory.poc_approved_product_claims` c
      WHERE c.claim_id=k.claim_id AND c.sku=k.sku))) invalid_claim_eligibility,
  COUNTIF((chunk_type LIKE 'WEBSITE_%' AND (k.sku IS NOT NULL OR k.brand!='Naked & Thriving'
      OR (chunk_type='WEBSITE_VISUAL_REFERENCE' AND (retrieval_status!='VISUAL_REFERENCE'
        OR approval_scope!='VISUAL_INSPIRATION_ONLY' OR claim_id IS NOT NULL))
      OR (chunk_type!='WEBSITE_VISUAL_REFERENCE' AND retrieval_status!='REVIEW_ONLY')))
    OR (chunk_type='POC_RULE' AND (k.brand!='*' OR k.source_system!='SYNTHETIC_POC'))) invalid_brand_scope
FROM chunks k LEFT JOIN `project-661d501a-e709-4276-a26.ci_product.product_master` p USING(sku);
