-- After a full run for the default 2025 campaign set:
-- Expected: 60 assets, 20 SKUs, 60 campaigns, zero errors.
WITH assets AS (
  SELECT * FROM `project-661d501a-e709-4276-a26.ci_creative.campaign_assets`
  WHERE source_system='SYNTHETIC_POC' AND generator_version='template-v1'
    AND STARTS_WITH(campaign_id, 'SYN-CMP-20250101-')
), duplicates AS (
  SELECT asset_id FROM assets GROUP BY asset_id HAVING COUNT(*) > 1
)
SELECT COUNT(*) asset_count, COUNT(DISTINCT a.sku) sku_count,
  COUNT(DISTINCT a.campaign_id) campaign_count,
  (SELECT COUNT(*) FROM duplicates) duplicate_ids,
  COUNTIF(c.campaign_id IS NULL OR c.sku != a.sku
    OR c.product_image_uri != a.source_image_uri
    OR c.headline != a.headline OR c.body_copy != a.body_copy) missing_evidence,
  COUNTIF(a.width != 1200 OR a.height != 628 OR a.mime_type != 'image/png'
    OR a.description_method != 'TEMPLATE_METADATA'
    OR NOT REGEXP_CONTAINS(a.sha256, r'^[0-9a-f]{64}$')
    OR NOT ENDS_WITH(a.gcs_uri, CONCAT(a.sha256, '.png'))) invalid_metadata
FROM assets a LEFT JOIN `project-661d501a-e709-4276-a26.ci_creative.campaign_master` c
  ON a.campaign_id=c.campaign_id AND c.source_system='SYNTHETIC_POC';
