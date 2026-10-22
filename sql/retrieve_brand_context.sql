-- Read-only lookup; parameter sku STRING. One statement gives consistent rule/approval reads.
WITH product AS (
  SELECT sku,brand,product_name,source_system
  FROM `project-661d501a-e709-4276-a26.ci_product.product_master` WHERE sku=@sku
), applicable_rules AS (
  SELECT r.* FROM `project-661d501a-e709-4276-a26.ci_brand_memory.brand_guidelines` r
  WHERE r.brand='*' OR r.brand IN (SELECT brand FROM product)
)
SELECT CURRENT_TIMESTAMP() checked_at,
  ARRAY(SELECT AS STRUCT * FROM product) products,
  ARRAY(SELECT AS STRUCT * FROM applicable_rules
    WHERE source_system='SYNTHETIC_POC' AND scope='PROJECT_POC'
      AND authority='PROJECT_POC_POLICY' AND rule_status='ACTIVE_FOR_POC'
    ORDER BY rule_id) poc_rules,
  ARRAY(SELECT AS STRUCT k.snapshot_id,k.chunk_id,k.chunk_type,k.brand,k.sku,k.campaign_id,k.asset_id,
      k.claim_id,k.chunk_text,k.source_system,k.source_reference,k.evidence_references,k.image_uris,
      k.retrieval_status,k.approval_scope,k.content_sha256
    FROM `project-661d501a-e709-4276-a26.ci_brand_memory.poc_generation_knowledge_chunks` k
    WHERE k.chunk_type='POC_RULE' AND k.source_system='SYNTHETIC_POC'
      AND k.retrieval_status='POC_POLICY' AND k.claim_id IS NULL AND k.sku IS NULL
      AND (k.brand='*' OR k.brand IN (SELECT brand FROM product))
      AND k.source_reference IN (SELECT CONCAT(
        'project-661d501a-e709-4276-a26.ci_brand_memory.brand_guidelines#rule_id=',rule_id)
        FROM applicable_rules WHERE source_system='SYNTHETIC_POC' AND scope='PROJECT_POC'
          AND authority='PROJECT_POC_POLICY' AND rule_status='ACTIVE_FOR_POC')
    ORDER BY k.chunk_id) policy_chunks,
  ARRAY(SELECT AS STRUCT claim_id,sku,claim_type,claim_text,evidence_reference,evidence_excerpt,
      evidence_source_system,approval_status,approval_scope,approved_by,approval_reference,
      approved_at,review_notes,source_system
    FROM `project-661d501a-e709-4276-a26.ci_brand_memory.poc_approved_product_claims`
    WHERE sku=@sku ORDER BY claim_id) approved_claims;
