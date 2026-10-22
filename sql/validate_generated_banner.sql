-- Parameter: asset_id STRING. Expect one row and zero invalid/missing evidence.
WITH assets AS (
  SELECT * FROM `project-661d501a-e709-4276-a26.ci_creative.generated_campaign_assets`
  WHERE asset_id=@asset_id AND source_system='SYNTHETIC_POC'
), claim_checks AS (
  SELECT A.asset_id, COUNTIF(C.claim_id IS NULL) missing_claims
  FROM assets A CROSS JOIN UNNEST(A.approved_claim_ids) required_claim_id
  LEFT JOIN `project-661d501a-e709-4276-a26.ci_brand_memory.poc_approved_product_claims` C
    ON C.claim_id=required_claim_id AND C.sku=A.sku AND C.claim_text=A.headline
  GROUP BY A.asset_id
)
SELECT COUNT(*) asset_count,
  COUNTIF(A.status!='REGISTERED_POC_DRAFT' OR A.approval_scope!='SYNTHETIC_POC_ONLY'
    OR A.real_business_approved OR A.asset_role!='GENERATED_CAMPAIGN_BANNER'
    OR A.mime_type!='image/png' OR A.width!=1200 OR A.height!=628 OR A.byte_count<=0
    OR A.description_method!='DETERMINISTIC_COMPOSITION'
    OR NOT REGEXP_CONTAINS(A.sha256,r'^[a-f0-9]{64}$')
    OR NOT REGEXP_CONTAINS(A.gcs_generation,r'^[1-9][0-9]*$')
    OR NOT ENDS_WITH(A.gcs_uri,CONCAT(A.sha256,'.png'))
    OR ARRAY_LENGTH(A.approved_claim_ids)=0 OR ARRAY_LENGTH(A.evidence_references)=0
    OR JSON_VALUE(A.brief_json,'$.canonical_sku') IS DISTINCT FROM A.sku
    OR JSON_VALUE(A.brief_json,'$.copy.headline') IS DISTINCT FROM A.headline
    OR JSON_VALUE(A.evidence_packet_json,'$.source_of_truth') IS DISTINCT FROM 'BIGQUERY') invalid_metadata,
  COUNTIF(P.sku IS NULL) missing_product_evidence,
  COUNTIF(COALESCE(checks.missing_claims,0)>0) missing_current_approvals
FROM assets A
LEFT JOIN `project-661d501a-e709-4276-a26.ci_product.product_master` P
  ON P.sku=A.sku AND P.source_system='BEAUTYFEEDS'
    AND P.product_name=A.headline AND P.primary_image_uri=A.source_image_uri
LEFT JOIN claim_checks checks ON checks.asset_id=A.asset_id;
