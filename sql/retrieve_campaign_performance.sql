-- Read-only structured analytics. No embedding/model calls or daily-row exports.
-- Parameters: sku STRING, all_campaigns BOOL, campaign_ids ARRAY<STRING>.
WITH campaigns AS (
  SELECT DISTINCT c.campaign_id,c.sku,c.campaign_name,c.objective,c.channel,c.audience,
    c.start_date,c.end_date,c.budget,c.currency,a.asset_id,k.snapshot_id
  FROM `project-661d501a-e709-4276-a26.ci_creative.campaign_master` c
  JOIN `project-661d501a-e709-4276-a26.ci_creative.campaign_assets` a
    ON a.campaign_id=c.campaign_id AND a.sku=c.sku
  JOIN `project-661d501a-e709-4276-a26.ci_brand_memory.poc_generation_knowledge_chunks` k
    ON k.campaign_id=c.campaign_id AND k.asset_id=a.asset_id AND k.sku=c.sku
  JOIN `project-661d501a-e709-4276-a26.ci_product.product_master` p
    ON p.sku=c.sku AND p.brand=k.brand
  WHERE c.sku=@sku AND (@all_campaigns OR c.campaign_id IN UNNEST(@campaign_ids))
    AND c.source_system='SYNTHETIC_POC' AND c.campaign_status='SYNTHETIC_SCENARIO'
    AND a.source_system='SYNTHETIC_POC' AND a.generator_version='template-v1'
    AND a.asset_role='CAMPAIGN_BANNER' AND p.source_system='BEAUTYFEEDS'
    AND k.chunk_type='CAMPAIGN_CREATIVE' AND k.source_system='SYNTHETIC_POC'
    AND k.retrieval_status='POC_CONTEXT' AND k.approval_scope='SYNTHETIC_POC_ONLY'
    AND a.gcs_uri IN UNNEST(k.image_uris)
), performance_totals AS (
  SELECT m.campaign_id,COUNT(*) performance_rows,COUNT(DISTINCT m.performance_date) performance_days,
    MIN(m.performance_date) first_performance_date,MAX(m.performance_date) last_performance_date,
    SUM(m.impressions) impressions,SUM(m.clicks) clicks,SUM(m.conversions) conversions,
    SUM(m.attributed_units) attributed_units,SUM(m.spend) spend,
    SUM(m.attributed_revenue) attributed_revenue,
    COUNTIF(m.sku!=c.sku OR m.asset_id!=c.asset_id OR m.channel!=c.channel OR m.currency!=c.currency
      OR m.performance_date NOT BETWEEN c.start_date AND c.end_date
      OR m.impressions<0 OR m.clicks<0 OR m.conversions<0 OR m.spend<0 OR m.attributed_revenue<0
      OR m.clicks>m.impressions OR m.conversions>m.clicks OR m.attributed_units<m.conversions
      OR m.ctr!=COALESCE(SAFE_DIVIDE(CAST(m.clicks AS NUMERIC),m.impressions),0)
      OR m.conversion_rate!=COALESCE(SAFE_DIVIDE(CAST(m.conversions AS NUMERIC),m.clicks),0)
      OR m.roas!=COALESCE(SAFE_DIVIDE(m.attributed_revenue,m.spend),0)
      OR m.generator_version!='performance-v1'
      OR m.attribution_model!='SYNTHETIC_SALES_CAPPED_ATTRIBUTION') invalid_rows
  FROM `project-661d501a-e709-4276-a26.ci_creative.campaign_daily_performance` m
  -- Join by campaign ID so incorrectly linked SKU/asset rows are detected above.
  JOIN campaigns c ON c.campaign_id=m.campaign_id
  WHERE m.source_system='SYNTHETIC_POC'
  GROUP BY m.campaign_id
)
SELECT c.*,DATE_DIFF(c.end_date,c.start_date,DAY)+1 expected_days,
  COALESCE(r.performance_rows,0) performance_rows,COALESCE(r.performance_days,0) performance_days,
  r.first_performance_date,r.last_performance_date,
  r.impressions,r.clicks,r.conversions,r.attributed_units,r.spend,r.attributed_revenue,
  SAFE_DIVIDE(CAST(r.clicks AS NUMERIC),r.impressions) ctr,
  SAFE_DIVIDE(CAST(r.conversions AS NUMERIC),r.clicks) conversion_rate,
  SAFE_DIVIDE(r.attributed_revenue,r.spend) roas,COALESCE(r.invalid_rows,0) invalid_rows,
  'SYNTHETIC_POC' source_system,'SYNTHETIC_SALES_CAPPED_ATTRIBUTION' attribution_model
FROM campaigns c LEFT JOIN performance_totals r USING(campaign_id)
ORDER BY c.campaign_id;
