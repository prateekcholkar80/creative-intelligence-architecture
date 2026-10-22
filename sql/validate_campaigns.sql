-- Parameters: campaign_ids ARRAY<STRING>, sales_start DATE, sales_end DATE.
-- Expected: 60 campaigns, 20 SKUs, all error counts zero.
WITH scoped AS (
  SELECT * FROM `project-661d501a-e709-4276-a26.ci_creative.campaign_master`
  WHERE source_system = 'SYNTHETIC_POC' AND campaign_id IN UNNEST(@campaign_ids)
), counts AS (
  SELECT sku, COUNT(*) n FROM scoped GROUP BY sku
), duplicates AS (
  SELECT campaign_id FROM scoped GROUP BY campaign_id HAVING COUNT(*) > 1
), overlap_pairs AS (
  SELECT a.campaign_id FROM scoped a JOIN scoped b
    ON a.sku=b.sku AND a.campaign_id < b.campaign_id
    AND a.start_date <= b.end_date AND b.start_date <= a.end_date
)
SELECT COUNT(*) campaign_count, COUNT(DISTINCT c.sku) sku_count,
  (SELECT COUNT(*) FROM counts WHERE n != 3) bad_counts,
  (SELECT COUNT(*) FROM duplicates) duplicate_ids,
  COUNTIF(c.start_date < @sales_start OR c.end_date > @sales_end
    OR c.end_date < c.start_date) invalid_periods,
  (SELECT COUNT(*) FROM overlap_pairs) overlaps,
  COUNTIF(p.sku IS NULL OR p.source_system != 'BEAUTYFEEDS'
    OR c.product_image_uri != p.primary_image_uri
    OR c.headline != p.product_name
    OR NOT EXISTS (SELECT 1 FROM `project-661d501a-e709-4276-a26.ci_product.product_assets` a
      WHERE a.sku=c.sku AND a.gcs_uri=c.product_image_uri)) missing_evidence,
  COUNTIF(c.budget <= 0 OR c.currency != UPPER(p.currency)
    OR c.campaign_status != 'SYNTHETIC_SCENARIO'
    OR c.generator_version != 'campaign-v1') invalid_metadata
FROM scoped c LEFT JOIN `project-661d501a-e709-4276-a26.ci_product.product_master` p USING(sku);
