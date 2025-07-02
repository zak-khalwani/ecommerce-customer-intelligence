-- This query is for cohort analysis
-- It tracks cohorts of new customers and their purchasing activity over subsequent months.

-- CTE 1: Assign each customer to an acquisition 'cohort' based on their first purchase month.
WITH customer_cohort AS (
    SELECT
        c.customer_unique_id,
        MIN(DATE_TRUNC('month', o.order_purchase_timestamp))::DATE AS cohort_month
    FROM customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_unique_id
),
-- CTE 2: For each customer, calculate their monthly activity relative to their start date.
cohort_activity AS (
    SELECT
        cc.customer_unique_id,
        cc.cohort_month,
        -- The 'cohort_index' is the number of months that have passed since the first purchase.
        (EXTRACT(YEAR FROM o.order_purchase_timestamp) - EXTRACT(YEAR FROM cc.cohort_month)) * 12 +
        (EXTRACT(MONTH FROM o.order_purchase_timestamp) - EXTRACT(MONTH FROM cc.cohort_month)) AS cohort_index
    FROM customer_cohort cc
    INNER JOIN customers c ON cc.customer_unique_id = c.customer_unique_id
    INNER JOIN orders o ON c.customer_id = o.customer_id
),
-- CTE 3: Calculate the initial size of each cohort (the 100% base for retention).
cohort_size AS (
    SELECT
        cohort_month,
        -- CORRECTED: Count distinct customers from the original cohort definition.
        COUNT(DISTINCT customer_unique_id) AS total_customers
    FROM customer_cohort
    GROUP BY cohort_month
)
-- Final Selection: Aggregate the data to create the retention table.
SELECT
    ca.cohort_month,
    cs.total_customers,
    ca.cohort_index,
    -- Count the number of unique customers from the cohort who were active in this period.
    COUNT(DISTINCT ca.customer_unique_id) AS active_customers,
    -- Calculate retention rate using floating-point division.
    ROUND((COUNT(DISTINCT ca.customer_unique_id) * 100.0 / cs.total_customers)::NUMERIC, 2) AS retention_rate
FROM cohort_activity ca
INNER JOIN cohort_size cs ON ca.cohort_month = cs.cohort_month
GROUP BY ca.cohort_month, cs.total_customers, ca.cohort_index
ORDER BY ca.cohort_month, ca.cohort_index;