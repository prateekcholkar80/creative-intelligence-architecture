-- Live eligibility for generated POC visual references only.
-- No promotion into actual historical campaign/performance evidence.
WITH claim_checks AS (
  SELECT A.asset_id, COUNT(*) claim_count,
    COUNT(DISTINCT required_claim_id) unique_claim_count,
    COUNTIF(C.claim_id IS NULL) missing_claims
  FROM `project-661d501a-e709-4276-a26.ci_creative.generated_campaign_assets` A
  CROSS JOIN UNNEST(A.approved_claim_ids) required_claim_id
  LEFT JOIN `project-661d501a-e709-4276-a26.ci_brand_memory.poc_approved_product_claims` C
    ON C.claim_id=required_claim_id AND C.sku=A.sku AND C.claim_text=A.headline
      AND C.claim_type='PRODUCT_NAME' AND C.approval_scope='SYNTHETIC_POC_ONLY'
  GROUP BY A.asset_id
), policy_checks AS (
  SELECT A.asset_id, COUNT(*) policy_count,
    COUNT(DISTINCT JSON_VALUE(policy,'$.rule_id')) unique_policy_count,
    COUNTIF(G.rule_id IS NULL) missing_policies
  FROM `project-661d501a-e709-4276-a26.ci_creative.generated_campaign_assets` A
  CROSS JOIN UNNEST(JSON_QUERY_ARRAY(A.policy_json)) policy
  LEFT JOIN `project-661d501a-e709-4276-a26.ci_brand_memory.brand_guidelines` G
    ON G.rule_id=JSON_VALUE(policy,'$.rule_id') AND G.rule_text=JSON_VALUE(policy,'$.rule_text')
      AND G.brand=JSON_VALUE(policy,'$.brand') AND G.category=JSON_VALUE(policy,'$.category')
      AND G.source_reference=JSON_VALUE(policy,'$.source_reference')
      AND G.scope='PROJECT_POC' AND G.authority='PROJECT_POC_POLICY'
      AND G.rule_status='ACTIVE_FOR_POC' AND G.source_system='SYNTHETIC_POC'
  GROUP BY A.asset_id
), applicable_rules AS (
  SELECT P.sku, COUNT(*) rule_count
  FROM `project-661d501a-e709-4276-a26.ci_product.product_master` P
  JOIN `project-661d501a-e709-4276-a26.ci_brand_memory.brand_guidelines` G ON G.brand IN ('*',P.brand)
  WHERE P.source_system='BEAUTYFEEDS' AND G.scope='PROJECT_POC'
    AND G.authority='PROJECT_POC_POLICY' AND G.source_system='SYNTHETIC_POC'
    AND G.rule_status='ACTIVE_FOR_POC'
  GROUP BY P.sku
), snapshots AS (
  SELECT DISTINCT snapshot_id
  FROM `project-661d501a-e709-4276-a26.ci_brand_memory.active_knowledge_chunks`
)
SELECT A.*
FROM `project-661d501a-e709-4276-a26.ci_creative.generated_campaign_assets` A
JOIN `project-661d501a-e709-4276-a26.ci_product.product_master` P
  ON P.sku=A.sku AND P.source_system='BEAUTYFEEDS' AND P.product_name=A.headline
    AND P.primary_image_uri=A.source_image_uri
JOIN `project-661d501a-e709-4276-a26.ci_brand_memory.poc_image_embeddings` original
  ON original.source_id=A.source_image_asset_id AND original.sku=A.sku
    AND original.source_system='BEAUTYFEEDS' AND original.gcs_uri=A.source_image_uri
    AND original.gcs_generation=A.source_image_generation AND original.content_sha256=A.source_image_sha256
JOIN snapshots snapshot ON snapshot.snapshot_id=A.knowledge_snapshot_id
JOIN claim_checks claims ON claims.asset_id=A.asset_id
JOIN policy_checks policies ON policies.asset_id=A.asset_id
JOIN applicable_rules rules ON rules.sku=A.sku
WHERE A.source_system='SYNTHETIC_POC' AND A.status='REGISTERED_POC_DRAFT'
  AND A.approval_scope='SYNTHETIC_POC_ONLY' AND A.real_business_approved=FALSE
  AND A.asset_role='GENERATED_CAMPAIGN_BANNER' AND A.mime_type='image/png'
  AND A.generator_version='grounded-banner-preview-v1' AND A.description_method='DETERMINISTIC_COMPOSITION'
  AND A.width=1200 AND A.height=628 AND A.byte_count>0
  AND REGEXP_CONTAINS(A.sku,r'^CI-SKU-00(0[1-9]|1[0-9]|20)$')
  AND REGEXP_CONTAINS(A.sha256,r'^[a-f0-9]{64}$')
  AND REGEXP_CONTAINS(A.gcs_generation,r'^[1-9][0-9]*$')
  AND A.asset_id=CONCAT('SYN-GEN-',A.sku,'-',A.sha256)
  AND A.source_image_asset_id=CONCAT('AST-',A.sku,'-PRIMARY')
  AND A.gcs_uri=CONCAT('gs://creative-brand-memory-project-661d501a-e709-4276-a26/generated_campaigns/',
      A.sku,'/',A.generator_version,'/',A.sha256,'.png')
  AND A.body_copy IN ('','Explore the product.','Discover the details.')
  AND A.call_to_action IN ('Shop now','View product','Explore product')
  AND ARRAY_LENGTH(A.evidence_references)>0 AND claims.missing_claims=0
  AND claims.claim_count=ARRAY_LENGTH(A.approved_claim_ids) AND claims.unique_claim_count=claims.claim_count
  AND policies.missing_policies=0 AND policies.policy_count=rules.rule_count
  AND policies.unique_policy_count=policies.policy_count
  AND JSON_VALUE(A.brief_json,'$.canonical_sku')=A.sku
  AND JSON_VALUE(A.brief_json,'$.copy.headline')=A.headline
  AND JSON_VALUE(A.brief_json,'$.copy.body')=A.body_copy
  AND JSON_VALUE(A.brief_json,'$.copy.cta')=A.call_to_action
  AND JSON_VALUE(A.evidence_packet_json,'$.source_of_truth')='BIGQUERY'
  AND JSON_VALUE(A.evidence_packet_json,'$.snapshot_id')=A.knowledge_snapshot_id
  AND JSON_VALUE(A.evidence_packet_json,'$.generation_constraints.benefit_claims_authorized')='false'
  AND JSON_VALUE(A.evidence_packet_json,'$.generation_constraints.claim_scope')='SYNTHETIC_POC_ONLY'
