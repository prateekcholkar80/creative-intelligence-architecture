-- Parameters: campaign_ids ARRAY<STRING>, sales_channel STRING.
-- Expected: 1260 rows, 60 campaigns, 20 SKUs, all error counts zero.
WITH scoped AS (
  SELECT * FROM `project-661d501a-e709-4276-a26.ci_creative.campaign_daily_performance`
  WHERE source_system='SYNTHETIC_POC' AND campaign_id IN UNNEST(@campaign_ids)
), duplicates AS (
  SELECT campaign_id FROM scoped
  GROUP BY campaign_id, asset_id, performance_date, source_system HAVING COUNT(*) > 1
), coverage AS (
  SELECT campaign_id, COUNT(*) n, COUNT(DISTINCT performance_date) days,
    MIN(performance_date) first_date, MAX(performance_date) last_date, SUM(spend) spend
  FROM scoped GROUP BY campaign_id
), campaigns AS (
  SELECT * FROM `project-661d501a-e709-4276-a26.ci_creative.campaign_master`
  WHERE source_system='SYNTHETIC_POC'
)
SELECT COUNT(*) row_count, COUNT(DISTINCT r.campaign_id) campaign_count,
  COUNT(DISTINCT r.sku) sku_count,
  (SELECT COUNT(*) FROM duplicates) duplicate_keys,
  (SELECT COUNT(*) FROM coverage v JOIN campaigns c USING(campaign_id)
    WHERE v.n != DATE_DIFF(c.end_date,c.start_date,DAY)+1 OR v.days != v.n
      OR v.first_date != c.start_date OR v.last_date != c.end_date) bad_coverage,
  (SELECT COUNT(*) FROM coverage v JOIN campaigns c USING(campaign_id)
    WHERE v.spend != c.budget) bad_budget,
  COUNTIF(c.campaign_id IS NULL OR a.asset_id IS NULL OR s.sku IS NULL
    OR r.sku != c.sku OR r.sku != a.sku OR r.campaign_id != a.campaign_id
    OR r.channel != c.channel OR r.currency != c.currency OR r.currency != s.currency
    OR r.performance_date NOT BETWEEN c.start_date AND c.end_date) missing_evidence,
  COUNTIF(r.impressions < 0 OR r.clicks < 0 OR r.conversions < 0
    OR r.clicks > r.impressions OR r.conversions > r.clicks
    OR r.conversions > s.orders OR r.attributed_units < r.conversions
    OR r.attributed_units > s.units_sold OR r.attributed_revenue < 0
    OR r.attributed_revenue > s.net_sales OR r.spend < 0
    OR r.ctr != COALESCE(SAFE_DIVIDE(CAST(r.clicks AS NUMERIC), r.impressions),0)
    OR r.conversion_rate != COALESCE(SAFE_DIVIDE(CAST(r.conversions AS NUMERIC),r.clicks),0)
    OR r.roas != COALESCE(SAFE_DIVIDE(r.attributed_revenue,r.spend),0)
    OR r.generator_version != 'performance-v1'
    OR r.attribution_model != 'SYNTHETIC_SALES_CAPPED_ATTRIBUTION') invalid_metrics
FROM scoped r LEFT JOIN campaigns c USING(campaign_id)
LEFT JOIN `project-661d501a-e709-4276-a26.ci_creative.campaign_assets` a
  ON r.asset_id=a.asset_id AND a.source_system='SYNTHETIC_POC'
LEFT JOIN `project-661d501a-e709-4276-a26.ci_sales.sku_daily_sales` s
  ON r.sku=s.sku AND r.performance_date=s.sales_date
    AND s.source_system='SYNTHETIC_POC' AND s.channel=@sales_channel;
