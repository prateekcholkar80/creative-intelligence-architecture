-- Parameters: rule_ids ARRAY<STRING>, claim_ids ARRAY<STRING>.
-- Synthetic fixture expected: 7 rules, 60 claims, 20 SKUs; 20 per review status.
WITH rules AS (
  SELECT * FROM `project-661d501a-e709-4276-a26.ci_brand_memory.brand_guidelines`
  WHERE source_system='SYNTHETIC_POC' AND rule_id IN UNNEST(@rule_ids)
), claims AS (
  SELECT * FROM `project-661d501a-e709-4276-a26.ci_brand_memory.product_claims`
  WHERE source_system='SYNTHETIC_POC' AND claim_id IN UNNEST(@claim_ids)
), duplicates AS (
  SELECT claim_id FROM claims GROUP BY claim_id HAVING COUNT(*)>1
)
SELECT (SELECT COUNT(*) FROM rules) rule_count, COUNT(*) claim_count,
  COUNT(DISTINCT c.sku) sku_count,
  COUNTIF(c.approval_status='APPROVED') mock_approved_count,
  COUNTIF(c.approval_status='REJECTED') rejected_count,
  COUNTIF(c.approval_status='PENDING_REVIEW') pending_count,
  (SELECT COUNT(*) FROM duplicates) duplicate_claims,
  (SELECT COUNT(*) FROM rules WHERE scope!='PROJECT_POC'
    OR authority!='PROJECT_POC_POLICY' OR rule_status!='ACTIVE_FOR_POC') invalid_poc_rules,
  COUNTIF(p.sku IS NULL OR p.source_system!='BEAUTYFEEDS'
    OR c.evidence_source_system!='BEAUTYFEEDS'
    OR (c.claim_type='PRODUCT_NAME' AND (c.claim_text!=p.product_name OR c.evidence_excerpt!=p.product_name))
    OR (c.claim_type='BRAND' AND (c.claim_text!=p.brand OR c.evidence_excerpt!=p.brand))
    OR (c.claim_type='INGREDIENT_PRESENCE' AND NOT EXISTS (
      SELECT 1 FROM `project-661d501a-e709-4276-a26.ci_product.product_ingredients` pi
      JOIN `project-661d501a-e709-4276-a26.ci_product.ingredients` i USING(ingredient_id)
      WHERE pi.sku=c.sku AND i.inci_name=c.evidence_excerpt AND i.source_system='BEAUTYFEEDS'
        AND c.evidence_reference=CONCAT('project-661d501a-e709-4276-a26.ci_product.product_ingredients#sku=',pi.sku,'&ingredient_id=',pi.ingredient_id)))) missing_evidence,
  COUNTIF(c.approval_scope!='SYNTHETIC_POC_ONLY'
    OR c.approval_status NOT IN ('APPROVED','REJECTED','PENDING_REVIEW')
    OR (c.approval_status='APPROVED' AND (
      c.claim_type!='PRODUCT_NAME' OR c.approved_by IS NULL OR c.approved_at IS NULL
      OR c.approved_by!='SYNTHETIC_POC_REVIEWER'
      OR c.approval_reference IS NULL OR c.approval_reference!='config/brand_memory.json#review_fixture'))
    OR (c.approval_status!='APPROVED' AND (c.approved_by IS NOT NULL
      OR c.approved_at IS NOT NULL OR c.approval_reference IS NOT NULL))) invalid_approvals
FROM claims c LEFT JOIN `project-661d501a-e709-4276-a26.ci_product.product_master` p USING(sku);
