-- This query finds the top 5 most valuable product categories for each RFM segment, ranked by total revenue.

-- CTE 1: Perform the full RFM analysis to establish customer segments.
WITH rfm_analysis AS (
    -- (This nested query calculates Recency, Frequency, and Monetary scores for each customer)
    WITH rfm_base AS (
        SELECT
            c.customer_unique_id,
            ( (SELECT MAX(order_purchase_timestamp)::DATE + INTERVAL '1 day' FROM orders) - MAX(o.order_purchase_timestamp)::DATE ) AS recency_days,
            COUNT(DISTINCT o.order_id) AS frequency,
            SUM(oi.price + oi.freight_value) AS monetary
        FROM orders AS o
        INNER JOIN order_items AS oi ON o.order_id = oi.order_id
        INNER JOIN customers AS c ON o.customer_id = c.customer_id
        WHERE o.order_status = 'delivered'
        GROUP BY c.customer_unique_id
    ),
    rfm_score AS (
        SELECT
            *,
            NTILE(4) OVER (ORDER BY recency_days ASC) AS r_score,
            NTILE(4) OVER (ORDER BY frequency ASC) AS f_score,
            NTILE(4) OVER (ORDER BY monetary ASC) AS m_score
        FROM rfm_base
    )
    SELECT
        *,
        CASE
            WHEN r_score >= 3 AND f_score >= 3 THEN 'Champions'
            WHEN r_score >= 3 AND f_score < 3  THEN 'Potential Loyalists'
            WHEN r_score < 3 AND f_score >= 3  THEN 'At Risk'
            WHEN r_score = 2 AND f_score <= 2  THEN 'Needs Attention'
            WHEN r_score = 1 AND f_score <= 2  THEN 'Hibernating'
            ELSE 'Other'
        END AS rfm_segment
    FROM rfm_score
),
-- CTE 2: Calculate total orders and revenue for each product category within each segment.
segment_product_kpis AS (
    SELECT
        r.rfm_segment,
        p.product_category_name,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(oi.price + oi.freight_value) AS total_revenue
    FROM rfm_analysis r
    INNER JOIN customers c ON r.customer_unique_id = c.customer_unique_id
    INNER JOIN orders o ON c.customer_id = o.customer_id
    INNER JOIN order_items oi ON o.order_id = oi.order_id
    INNER JOIN products p ON oi.product_id = p.product_id
    WHERE o.order_status = 'delivered' AND p.product_category_name IS NOT NULL
    GROUP BY r.rfm_segment, p.product_category_name
)
-- Final Query: Rank products by revenue and select the top 5 for each segment.
SELECT *
FROM (
    SELECT
        rfm_segment,
        product_category_name,
        -- Value-based metrics: How much money does this category bring in?
        ROUND(total_revenue::NUMERIC, 2) AS total_revenue,
        ROUND((total_revenue * 100.0 / SUM(total_revenue) OVER (PARTITION BY rfm_segment))::NUMERIC, 2) AS pct_of_segment_revenue,
        -- Volume-based metrics: How many orders does this category have?
        total_orders,
        ROUND((total_orders * 100.0 / SUM(total_orders) OVER (PARTITION BY rfm_segment))::NUMERIC, 2) AS pct_of_segment_orders,
        -- Rank categories by their contribution to segment revenue.
        ROW_NUMBER() OVER (PARTITION BY rfm_segment ORDER BY total_revenue DESC) AS revenue_rank
    FROM segment_product_kpis
) AS product_ranks
WHERE revenue_rank <= 5
ORDER BY rfm_segment, revenue_rank;