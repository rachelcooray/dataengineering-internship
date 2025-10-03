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
-- Funnel: Visits → Carts → Transactions (last 30 days)
WITH visits AS (
  SELECT DISTINCT session_id, user_id
  FROM curated_dw.clickstream
  WHERE DATE(click_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
),

carts AS (
  SELECT DISTINCT session_id, user_id
  FROM curated_dw.clickstream
  WHERE page_url = '/cart'
    AND DATE(click_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
),

txns AS (
  SELECT DISTINCT t.user_id
  FROM curated_dw.transactions t
  WHERE DATE(t.txn_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
)

SELECT
  (SELECT COUNT(*) FROM visits) AS total_visits,
  (SELECT COUNT(*) FROM carts) AS total_carts,
  (
    SELECT COUNT(DISTINCT c.session_id)
    FROM carts c
    JOIN txns t ON c.user_id = t.user_id
  ) AS total_txns,
  ROUND(
    100.0 * (
      SELECT COUNT(DISTINCT c.session_id)
      FROM carts c
      JOIN txns t ON c.user_id = t.user_id
    ) / NULLIF((SELECT COUNT(*) FROM visits), 0),
    2
  ) AS visit_to_txn_conversion_pct,
  ROUND(
    100.0 * (
      SELECT COUNT(DISTINCT c.session_id)
      FROM carts c
      JOIN txns t ON c.user_id = t.user_id
    ) / NULLIF((SELECT COUNT(*) FROM carts), 0),
    2
  ) AS cart_to_txn_conversion_pct;



-- 4. Partition Filter Comparison (cost optimization)
-- Query WITHOUT partition filter (scans all data)
SELECT COUNT(*) AS total_txns_no_filter
FROM curated_dw.transactions;

-- Query WITH partition filter (last 30 days only)
SELECT COUNT(*) AS total_txns_last_30d
FROM curated_dw.transactions
WHERE DATE(txn_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);

