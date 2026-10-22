-- Parameters: start_date DATE, end_date DATE, channel STRING.
-- Expected: 7300 rows, 20 SKUs, all error counts zero.
WITH scoped AS (
  SELECT * FROM `project-661d501a-e709-4276-a26.ci_sales.sku_daily_sales`
  WHERE source_system = 'SYNTHETIC_POC' AND channel = @channel
    AND sales_date BETWEEN @start_date AND @end_date
), coverage AS (
  SELECT sku, COUNT(*) n, COUNT(DISTINCT sales_date) days,
    MIN(sales_date) first_date, MAX(sales_date) last_date
  FROM scoped GROUP BY sku
), duplicates AS (
  SELECT sku FROM scoped GROUP BY sales_date, sku, channel, source_system
  HAVING COUNT(*) > 1
)
SELECT COUNT(*) row_count, COUNT(DISTINCT sku) sku_count,
  COUNTIF(sku NOT IN (SELECT sku FROM `project-661d501a-e709-4276-a26.ci_product.product_master`)
    OR NOT REGEXP_CONTAINS(sku, r'^CI-SKU-00(0[1-9]|1[0-9]|20)$')) bad_skus,
  (SELECT COUNT(*) FROM duplicates) duplicate_keys,
  COUNTIF(units_sold IS NULL OR units_sold < 0 OR orders IS NULL
    OR orders < 0 OR orders > units_sold OR gross_sales IS NULL
    OR net_sales IS NULL OR gross_sales < 0 OR discount_amount < 0 OR refund_amount < 0
    OR discount_amount IS NULL OR refund_amount IS NULL
    OR net_sales != gross_sales - discount_amount - refund_amount OR net_sales < 0
    OR average_selling_price IS NULL OR average_selling_price < 0
    OR currency IS NULL) invalid_financials,
  (SELECT COUNT(*) FROM coverage WHERE n != 365 OR days != 365
    OR first_date != @start_date OR last_date != @end_date) bad_coverage
FROM scoped;
