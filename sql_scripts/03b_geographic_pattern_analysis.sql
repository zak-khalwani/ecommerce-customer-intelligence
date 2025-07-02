-- This query finds the top 5 states with the highest concentration of customers for each RFM segment.

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
        SELECT *, 
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
-- CTE 2: Aggregate customer count and revenue by state for each segment.
segment_geo_kpis AS (
    SELECT
        r.rfm_segment,
        c.customer_state AS state,
        -- Count unique customers to measure concentration.
        COUNT(DISTINCT r.customer_unique_id) AS total_customers,
        -- Sum the pre-calculated monetary value for financial context.
        SUM(r.monetary) AS total_revenue
    FROM rfm_analysis r
    -- Join to customers table to get state information.
    INNER JOIN customers c ON r.customer_unique_id = c.customer_unique_id
    GROUP BY r.rfm_segment, c.customer_state
)
-- Final Query: Rank states by customer count and select the top 5 for each segment.
SELECT *
FROM (
    SELECT
        rfm_segment,
        state,
        total_customers,
        -- Calculate this state's share of its segment's total customer base.
        ROUND((total_customers * 100.0 / SUM(total_customers) OVER (PARTITION BY rfm_segment))::NUMERIC, 2) AS pct_of_segment_customers,
        ROUND(total_revenue::NUMERIC, 2) AS total_revenue,
        -- Calculate this state's share of its segment's total revenue.
        ROUND((total_revenue * 100.0 / SUM(total_revenue) OVER (PARTITION BY rfm_segment))::NUMERIC, 2) AS pct_of_segment_revenue,
        -- Rank states by customer count to find the most concentrated areas.
        ROW_NUMBER() OVER (PARTITION BY rfm_segment ORDER BY total_customers DESC) AS geo_rank
    FROM segment_geo_kpis
) AS ranks
WHERE geo_rank <= 5
ORDER BY rfm_segment, geo_rank;