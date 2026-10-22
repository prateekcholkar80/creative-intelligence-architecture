-- Read-only lookup. Parameter: sku STRING. One query keeps approval reads consistent.
SELECT
  ARRAY(SELECT AS STRUCT sku,brand,product_name,product_type,price,currency,
      primary_image_uri,source_system
    FROM `project-661d501a-e709-4276-a26.ci_product.product_master`
    WHERE sku=@sku) products,
  ARRAY(SELECT AS STRUCT snapshot_id,chunk_id,chunk_type,brand,sku,chunk_text,
      source_system,source_reference,evidence_references,image_uris,
      retrieval_status,approval_scope,claim_id,content_sha256
    FROM `project-661d501a-e709-4276-a26.ci_brand_memory.poc_generation_knowledge_chunks`
    WHERE sku=@sku AND chunk_type IN (
      'PRODUCT_KNOWLEDGE','INGREDIENT_KNOWLEDGE','POC_APPROVED_CLAIM')
    ORDER BY chunk_type,chunk_id) knowledge_chunks,
  ARRAY(SELECT AS STRUCT claim_id,sku,claim_type,claim_text,evidence_reference,
      evidence_excerpt,evidence_source_system,approval_status,approval_scope,
      approved_by,approval_reference,approved_at,review_notes,source_system
    FROM `project-661d501a-e709-4276-a26.ci_brand_memory.poc_approved_product_claims`
    WHERE sku=@sku ORDER BY claim_id) poc_identity_claims;
