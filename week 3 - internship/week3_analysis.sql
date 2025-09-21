-- ================================
-- week3_analysis.sql
-- Analytical Queries for Week 3
-- ================================

-- 1. Daily Active Users (DAU) by country (last 30 days)
SELECT
  DATE(click_time) AS click_date,
  location AS country,
  COUNT(DISTINCT user_id) AS daily_active_users
FROM curated_dw.clickstream
WHERE DATE(click_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY click_date, country
ORDER BY click_date, country;


-- 2. Revenue per currency vs USD (last 30 days)
SELECT
  currency,
  ROUND(SUM(CAST(amount AS FLOAT64)), 2) AS total_revenue_original,
  ROUND(SUM(amount_in_usd), 2) AS total_revenue_usd
FROM curated_dw.transactions
WHERE DATE(txn_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY currency
ORDER BY total_revenue_usd DESC;


-- 3. Funnel Analysis: visits → cart → transactions (last 30 days)
-- Assuming page_url = '/cart' represents add-to-cart
WITH visits AS (
  SELECT COUNT(DISTINCT session_id) AS total_visits
  FROM curated_dw.clickstream
  WHERE DATE(click_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
),
carts AS (
  SELECT COUNT(DISTINCT session_id) AS total_carts
  FROM curated_dw.clickstream
  WHERE DATE(click_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND page_url = '/cart'
),
transactions AS (
  SELECT COUNT(DISTINCT user_id) AS total_txns
  FROM curated_dw.transactions
  WHERE DATE(txn_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
)
SELECT
  v.total_visits,
  c.total_carts,
  t.total_txns,
  ROUND(c.total_carts / v.total_visits * 100, 2) AS cart_conversion_rate,
  ROUND(t.total_txns / v.total_visits * 100, 2) AS txn_conversion_rate
FROM visits v, carts c, transactions t;


-- 4. Partition Filter Comparison (cost optimization)
-- Query WITHOUT partition filter (scans all data)
SELECT COUNT(*) AS total_txns_no_filter
FROM curated_dw.transactions;

-- Query WITH partition filter (last 30 days only)
SELECT COUNT(*) AS total_txns_last_30d
FROM curated_dw.transactions
WHERE DATE(txn_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);

