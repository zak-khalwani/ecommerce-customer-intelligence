-- This query pivots the cohort analysis result to create a human-readable retention matrix directly in SQL.

-- CTE 1: Assign each customer to a 'cohort' based on their first purchase month.
WITH customer_cohort AS (
    SELECT
        c.customer_unique_id,
        MIN(DATE_TRUNC('month', o.order_purchase_timestamp))::DATE AS cohort_month
    FROM customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_unique_id
),
-- CTE 2: Calculate the monthly activity for each customer relative to their cohort month.
cohort_activity AS (
    SELECT
        cc.customer_unique_id,
        cc.cohort_month,
        (EXTRACT(YEAR FROM o.order_purchase_timestamp) - EXTRACT(YEAR FROM cc.cohort_month)) * 12 +
        (EXTRACT(MONTH FROM o.order_purchase_timestamp) - EXTRACT(MONTH FROM cc.cohort_month)) AS cohort_index
    FROM customer_cohort cc
    INNER JOIN customers c ON cc.customer_unique_id = c.customer_unique_id
    INNER JOIN orders o ON c.customer_id = o.customer_id
)
-- Final Selection: Aggregate and pivot the data into a matrix, rounding the results.
SELECT
    TO_CHAR(cohort_month, 'YYYY-MM') AS cohort,
    -- Get the initial size of each cohort (the 100% base).
    COUNT(DISTINCT CASE WHEN cohort_index = 0 THEN customer_unique_id END) AS new_customers,
    -- Use conditional aggregation to calculate retention for each month.
    ROUND((COUNT(DISTINCT CASE WHEN cohort_index = 1 THEN customer_unique_id END) * 100.0 / COUNT(DISTINCT CASE WHEN cohort_index = 0 THEN customer_unique_id END))::NUMERIC, 2) AS "Month 1",
    ROUND((COUNT(DISTINCT CASE WHEN cohort_index = 2 THEN customer_unique_id END) * 100.0 / COUNT(DISTINCT CASE WHEN cohort_index = 0 THEN customer_unique_id END))::NUMERIC, 2) AS "Month 2",
    ROUND((COUNT(DISTINCT CASE WHEN cohort_index = 3 THEN customer_unique_id END) * 100.0 / COUNT(DISTINCT CASE WHEN cohort_index = 0 THEN customer_unique_id END))::NUMERIC, 2) AS "Month 3",
    ROUND((COUNT(DISTINCT CASE WHEN cohort_index = 4 THEN customer_unique_id END) * 100.0 / COUNT(DISTINCT CASE WHEN cohort_index = 0 THEN customer_unique_id END))::NUMERIC, 2) AS "Month 4",
    ROUND((COUNT(DISTINCT CASE WHEN cohort_index = 5 THEN customer_unique_id END) * 100.0 / COUNT(DISTINCT CASE WHEN cohort_index = 0 THEN customer_unique_id END))::NUMERIC, 2) AS "Month 5",
    ROUND((COUNT(DISTINCT CASE WHEN cohort_index = 6 THEN customer_unique_id END) * 100.0 / COUNT(DISTINCT CASE WHEN cohort_index = 0 THEN customer_unique_id END))::NUMERIC, 2) AS "Month 6",
    ROUND((COUNT(DISTINCT CASE WHEN cohort_index = 7 THEN customer_unique_id END) * 100.0 / COUNT(DISTINCT CASE WHEN cohort_index = 0 THEN customer_unique_id END))::NUMERIC, 2) AS "Month 7",
    ROUND((COUNT(DISTINCT CASE WHEN cohort_index = 8 THEN customer_unique_id END) * 100.0 / COUNT(DISTINCT CASE WHEN cohort_index = 0 THEN customer_unique_id END))::NUMERIC, 2) AS "Month 8",
    ROUND((COUNT(DISTINCT CASE WHEN cohort_index = 9 THEN customer_unique_id END) * 100.0 / COUNT(DISTINCT CASE WHEN cohort_index = 0 THEN customer_unique_id END))::NUMERIC, 2) AS "Month 9",
    ROUND((COUNT(DISTINCT CASE WHEN cohort_index = 10 THEN customer_unique_id END) * 100.0 / COUNT(DISTINCT CASE WHEN cohort_index = 0 THEN customer_unique_id END))::NUMERIC, 2) AS "Month 10",
    ROUND((COUNT(DISTINCT CASE WHEN cohort_index = 11 THEN customer_unique_id END) * 100.0 / COUNT(DISTINCT CASE WHEN cohort_index = 0 THEN customer_unique_id END))::NUMERIC, 2) AS "Month 11",
    ROUND((COUNT(DISTINCT CASE WHEN cohort_index = 12 THEN customer_unique_id END) * 100.0 / COUNT(DISTINCT CASE WHEN cohort_index = 0 THEN customer_unique_id END))::NUMERIC, 2) AS "Month 12"
FROM cohort_activity
GROUP BY cohort_month
ORDER BY cohort_month;