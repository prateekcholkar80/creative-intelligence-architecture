-- Read-only structured analytics. Parameters: sku/channel STRING, start_date/end_date DATE,
-- recent_start/previous_start DATE. No daily rows or vectors are exported.
WITH product AS (
  SELECT sku,brand,product_name,UPPER(currency) currency,source_system
  FROM `project-661d501a-e709-4276-a26.ci_product.product_master`
  WHERE sku=@sku AND source_system='BEAUTYFEEDS'
), scoped AS (
  SELECT * FROM `project-661d501a-e709-4276-a26.ci_sales.sku_daily_sales`
  WHERE sku=@sku AND channel=@channel AND source_system='SYNTHETIC_POC'
    AND sales_date BETWEEN @start_date AND @end_date
), campaigns AS (
  SELECT campaign_id,sku,objective,start_date,end_date,currency,source_system
  FROM `project-661d501a-e709-4276-a26.ci_creative.campaign_master`
  WHERE sku=@sku AND source_system='SYNTHETIC_POC' AND generator_version='campaign-v1'
    AND start_date<=@end_date AND end_date>=@start_date
), campaign_matches AS (
  SELECT s.sales_date,COUNT(c.campaign_id) campaign_count
  FROM scoped s LEFT JOIN campaigns c ON s.sales_date BETWEEN c.start_date AND c.end_date
  GROUP BY s.sales_date
), totals AS (
  SELECT COUNT(*) row_count,COUNT(DISTINCT sales_date) day_count,
    MIN(sales_date) first_date,MAX(sales_date) last_date,COUNT(DISTINCT currency) currency_count,
    SUM(units_sold) units_sold,SUM(orders) orders,SUM(gross_sales) gross_sales,
    SUM(discount_amount) discount_amount,SUM(refund_amount) refund_amount,SUM(net_sales) net_sales,
    COUNTIF(units_sold IS NULL OR units_sold<0 OR orders IS NULL OR orders<0 OR orders>units_sold
      OR gross_sales IS NULL OR gross_sales<0 OR discount_amount IS NULL OR discount_amount<0
      OR refund_amount IS NULL OR refund_amount<0 OR net_sales IS NULL OR net_sales<0
      OR net_sales!=gross_sales-discount_amount-refund_amount OR currency IS NULL
      OR currency!=(SELECT ANY_VALUE(currency) FROM product)) invalid_rows
  FROM scoped
), monthly AS (
  SELECT DATE_TRUNC(sales_date,MONTH) month_start,COUNT(*) row_count,
    COUNT(DISTINCT sales_date) day_count,MIN(sales_date) first_date,MAX(sales_date) last_date,
    SUM(units_sold) units_sold,SUM(orders) orders,SUM(gross_sales) gross_sales,
    SUM(discount_amount) discount_amount,SUM(refund_amount) refund_amount,SUM(net_sales) net_sales
  FROM scoped GROUP BY month_start
), windows AS (
  SELECT IF(sales_date>=@recent_start,'RECENT','PREVIOUS') window_name,
    COUNT(*) row_count,COUNT(DISTINCT sales_date) day_count,
    MIN(sales_date) first_date,MAX(sales_date) last_date,
    SUM(units_sold) units_sold,SUM(orders) orders,SUM(gross_sales) gross_sales,
    SUM(discount_amount) discount_amount,SUM(refund_amount) refund_amount,SUM(net_sales) net_sales
  FROM scoped WHERE sales_date>=@previous_start GROUP BY window_name
), campaign_periods AS (
  SELECT c.*,GREATEST(c.start_date,@start_date) effective_start,LEAST(c.end_date,@end_date) effective_end,
    COUNT(*) row_count,COUNT(DISTINCT s.sales_date) day_count,
    MIN(s.sales_date) first_date,MAX(s.sales_date) last_date,
    SUM(s.units_sold) units_sold,SUM(s.orders) orders,SUM(s.gross_sales) gross_sales,
    SUM(s.discount_amount) discount_amount,SUM(s.refund_amount) refund_amount,SUM(s.net_sales) net_sales
  FROM campaigns c JOIN scoped s ON s.sales_date BETWEEN c.start_date AND c.end_date
  GROUP BY c.campaign_id,c.sku,c.objective,c.start_date,c.end_date,c.currency,c.source_system
)
SELECT ARRAY(SELECT AS STRUCT * FROM product) products,
  (SELECT AS STRUCT * FROM totals) totals,
  ARRAY(SELECT AS STRUCT * FROM monthly ORDER BY month_start) monthly,
  ARRAY(SELECT AS STRUCT * FROM windows ORDER BY window_name) windows,
  ARRAY(SELECT AS STRUCT * FROM campaign_periods ORDER BY campaign_id) campaign_periods,
  (SELECT COUNT(*) FROM campaigns) campaign_count,
  (SELECT COUNTIF(campaign_count>1) FROM campaign_matches) overlapping_campaign_days,
  (SELECT AS STRUCT recent.units_sold-previous.units_sold units_change,
    recent.orders-previous.orders orders_change,recent.net_sales-previous.net_sales net_sales_change,
    SAFE_DIVIDE(CAST(recent.units_sold-previous.units_sold AS NUMERIC),previous.units_sold) units_change_fraction,
    SAFE_DIVIDE(CAST(recent.orders-previous.orders AS NUMERIC),previous.orders) orders_change_fraction,
    SAFE_DIVIDE(recent.net_sales-previous.net_sales,previous.net_sales) net_sales_change_fraction
   FROM windows recent CROSS JOIN windows previous
   WHERE recent.window_name='RECENT' AND previous.window_name='PREVIOUS') trends;
