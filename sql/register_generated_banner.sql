-- Staging schema is explicitly copied from the destination table.
-- Validate current authority and scoped keys within the MERGE transaction.
BEGIN TRANSACTION;
ASSERT NOT EXISTS (
  SELECT T.asset_id FROM `project-661d501a-e709-4276-a26.ci_creative.generated_campaign_assets` T
  JOIN `{stage}` S USING(asset_id,source_system)
  GROUP BY T.asset_id HAVING COUNT(*)>1
) AS 'Duplicate destination asset keys';
ASSERT NOT EXISTS (
  SELECT 1 FROM `project-661d501a-e709-4276-a26.ci_creative.generated_campaign_assets` T
  JOIN `{stage}` S USING(asset_id,source_system)
  WHERE {immutable_mismatch}
) AS 'Existing asset has conflicting immutable identity or approval state';
ASSERT NOT EXISTS (
  SELECT 1 FROM `{stage}` S
  WHERE NOT EXISTS (
    SELECT 1 FROM `project-661d501a-e709-4276-a26.ci_product.product_master` P
    WHERE P.sku=S.sku AND P.source_system='BEAUTYFEEDS'
      AND P.product_name=S.headline AND P.primary_image_uri=S.source_image_uri
  )
) AS 'Product identity changed';
ASSERT NOT EXISTS (
  SELECT 1 FROM `{stage}` S, UNNEST(S.approved_claim_ids) required_claim_id
  WHERE NOT EXISTS (
    SELECT 1 FROM `project-661d501a-e709-4276-a26.ci_brand_memory.poc_approved_product_claims` C
    WHERE C.claim_id=required_claim_id AND C.sku=S.sku AND C.claim_text=S.headline
      AND C.claim_type='PRODUCT_NAME' AND C.approval_scope='SYNTHETIC_POC_ONLY'
  )
) AS 'POC identity approval is no longer current';
ASSERT NOT EXISTS (
  SELECT 1 FROM `{stage}` S, UNNEST(JSON_QUERY_ARRAY(S.policy_json)) policy
  WHERE NOT EXISTS (
    SELECT 1 FROM `project-661d501a-e709-4276-a26.ci_brand_memory.brand_guidelines` G
    WHERE G.rule_id=JSON_VALUE(policy,'$.rule_id')
      AND G.rule_text=JSON_VALUE(policy,'$.rule_text')
      AND G.source_reference=JSON_VALUE(policy,'$.source_reference')
      AND G.brand=JSON_VALUE(policy,'$.brand')
      AND G.scope='PROJECT_POC' AND G.authority='PROJECT_POC_POLICY'
      AND G.source_system='SYNTHETIC_POC' AND G.rule_status='ACTIVE_FOR_POC'
  )
) AS 'Applicable POC rules changed';
ASSERT NOT EXISTS (
  SELECT 1 FROM `{stage}` S
  WHERE ARRAY_LENGTH(JSON_QUERY_ARRAY(S.policy_json)) != (
    SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_brand_memory.brand_guidelines` G
    JOIN `project-661d501a-e709-4276-a26.ci_product.product_master` P ON P.sku=S.sku
    WHERE G.brand IN ('*',P.brand) AND G.scope='PROJECT_POC' AND G.authority='PROJECT_POC_POLICY'
      AND G.source_system='SYNTHETIC_POC' AND G.rule_status='ACTIVE_FOR_POC'
  )
) AS 'Applicable POC rule set changed';
MERGE `project-661d501a-e709-4276-a26.ci_creative.generated_campaign_assets` T
USING `{stage}` S ON T.asset_id=S.asset_id AND T.source_system=S.source_system
WHEN MATCHED THEN UPDATE SET
  last_verified_at=S.last_verified_at, brief_json=S.brief_json,
  evidence_packet_json=S.evidence_packet_json, policy_json=S.policy_json,
  approved_claim_ids=S.approved_claim_ids, evidence_references=S.evidence_references
WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values});
COMMIT TRANSACTION;
