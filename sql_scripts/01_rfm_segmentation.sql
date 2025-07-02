-- This query performs a complete RFM (Recency, Frequency, Monetary) analysis
-- to segment customers into actionable, behavior-based groups.

-- CTE 1: Calculate raw RFM values for each customer.
WITH rfm_base AS (
    SELECT
        c.customer_unique_id,      
        -- Recency: Days since last purchase. Analysis date is 1 day after the last recorded purchase.
        -- Casting to ::DATE removes the time component for a clean "days" calculation.
        ( (SELECT MAX(order_purchase_timestamp)::DATE + INTERVAL '1 day' FROM orders) - MAX(o.order_purchase_timestamp)::DATE ) AS recency_days,
        -- Frequency: Total number of distinct delivered orders.
        COUNT(DISTINCT o.order_id) AS frequency,
        -- Monetary: Total spend across all orders (item price + shipping).
        SUM(oi.price + oi.freight_value) AS monetary
    FROM orders AS o
    INNER JOIN order_items AS oi ON o.order_id = oi.order_id
    INNER JOIN customers AS c ON o.customer_id = c.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
),
-- CTE 2: Assign scores from 1-4 to each RFM dimension using quartiles (NTILE).
rfm_score AS (
    SELECT
        *,
        -- R_Score: More recent is better (higher score).
        NTILE(4) OVER (ORDER BY recency_days ASC) AS r_score,
        -- F_Score & M_Score: Higher value is better (higher score).
        NTILE(4) OVER (ORDER BY frequency ASC) AS f_score,
        NTILE(4) OVER (ORDER BY monetary ASC) AS m_score
    FROM rfm_base
)
-- Final Selection: Apply business logic to create named segments.
SELECT
    *,
    -- This CASE statement translates the numeric scores into strategic segments for marketing.
    CASE
        WHEN r_score >= 3 AND f_score >= 3 THEN 'Champions'
        WHEN r_score >= 3 AND f_score < 3  THEN 'Potential Loyalists'
        WHEN r_score < 3 AND f_score >= 3  THEN 'At Risk'
        WHEN r_score = 2 AND f_score <= 2  THEN 'Needs Attention'
        WHEN r_score = 1 AND f_score <= 2  THEN 'Hibernating'
        ELSE 'Other'
    END AS rfm_segment
FROM rfm_score
ORDER BY r_score DESC, f_score DESC;