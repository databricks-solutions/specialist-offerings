-- HiveQL queries for retail analytics reporting

-- 1. Revenue summary with LATERAL VIEW and explode
SELECT
    o.order_year,
    o.order_month,
    tag,
    SUM(o.total_amount) AS revenue,
    COUNT(DISTINCT o.customer_id) AS unique_customers
FROM fact_orders o
JOIN product_catalog p ON o.product_id = p.product_id
LATERAL VIEW explode(p.tags) tag_table AS tag
WHERE o.status = 'completed'
GROUP BY o.order_year, o.order_month, tag
HAVING SUM(o.total_amount) > 1000
SORT BY o.order_year, o.order_month, revenue DESC;

-- 2. Customer segmentation using Hive-specific functions
SELECT
    customer_id,
    first_name,
    last_name,
    tier,
    lifetime_value,
    PERCENTILE_APPROX(lifetime_value, 0.5) OVER () AS median_ltv,
    IF(lifetime_value > 10000, 'high_value',
       IF(lifetime_value > 1000, 'medium_value', 'low_value')) AS segment,
    NVL(phone, 'N/A') AS phone,
    COALESCE(email, CONCAT(first_name, '.', last_name, '@unknown.com')) AS email_filled
FROM dim_customers
WHERE is_active = TRUE
DISTRIBUTE BY tier
SORT BY lifetime_value DESC;

-- 3. Sessionization with Hive-specific syntax
SELECT
    session_id,
    user_id,
    event_type,
    event_timestamp,
    LEAD(event_timestamp) OVER (PARTITION BY session_id ORDER BY event_timestamp) AS next_event_time,
    unix_timestamp(LEAD(event_timestamp) OVER (PARTITION BY session_id ORDER BY event_timestamp))
        - unix_timestamp(event_timestamp) AS time_to_next_event,
    SIZE(properties) AS num_properties,
    properties['page_category'] AS page_category
FROM raw_clickstream
WHERE dt = '${hiveconf:report_date}'
  AND hour BETWEEN 9 AND 17;

-- 4. Multi-insert statement (Hive-specific)
FROM fact_orders o
JOIN dim_customers c ON o.customer_id = c.customer_id
INSERT OVERWRITE TABLE monthly_revenue PARTITION (order_year, order_month)
    SELECT
        c.tier,
        SUM(o.total_amount) AS total_revenue,
        COUNT(*) AS order_count,
        o.order_year,
        o.order_month
    WHERE o.status = 'completed'
    GROUP BY c.tier, o.order_year, o.order_month
INSERT OVERWRITE TABLE customer_monthly_summary PARTITION (order_year, order_month)
    SELECT
        o.customer_id,
        c.first_name,
        c.last_name,
        SUM(o.total_amount) AS monthly_spend,
        COUNT(*) AS monthly_orders,
        o.order_year,
        o.order_month
    GROUP BY o.customer_id, c.first_name, c.last_name, o.order_year, o.order_month;

-- 5. Query using TRANSFORM (custom mapper/reducer)
SELECT TRANSFORM(customer_id, first_name, last_name, email)
    USING 'python3 /scripts/normalize_customer.py'
    AS (customer_id BIGINT, normalized_name STRING, email_domain STRING)
FROM dim_customers
WHERE is_active = TRUE;

-- 6. Complex aggregation with GROUPING SETS
SELECT
    order_year,
    order_month,
    payment_method,
    status,
    SUM(total_amount) AS revenue,
    COUNT(*) AS order_count,
    AVG(total_amount) AS avg_order_value,
    GROUPING__ID AS grouping_id
FROM fact_orders
WHERE order_year = 2024
GROUP BY order_year, order_month, payment_method, status
GROUPING SETS (
    (order_year, order_month, payment_method, status),
    (order_year, order_month, payment_method),
    (order_year, order_month),
    (order_year),
    ()
);

-- 7. CTE with Hive hints
WITH ranked_products AS (
    SELECT
        /*+ MAPJOIN(p) */
        p.product_id,
        p.name,
        p.category,
        SUM(o.total_amount) AS total_sales,
        ROW_NUMBER() OVER (PARTITION BY p.category ORDER BY SUM(o.total_amount) DESC) AS rank
    FROM product_catalog p
    JOIN fact_orders o ON p.product_id = o.product_id
    WHERE o.status = 'completed'
    GROUP BY p.product_id, p.name, p.category
)
SELECT * FROM ranked_products
WHERE rank <= 10
ORDER BY category, rank;

-- 8. TABLESAMPLE and CLUSTER BY
SELECT customer_id, first_name, last_name, tier, lifetime_value
FROM dim_customers TABLESAMPLE(10 PERCENT)
CLUSTER BY tier;

-- 9. Map-side join with Hive hint
SELECT /*+ STREAMTABLE(o) */
    o.order_id,
    o.order_date,
    o.total_amount,
    c.first_name,
    c.last_name,
    c.tier,
    p.name AS product_name,
    p.category
FROM fact_orders o
JOIN dim_customers c ON o.customer_id = c.customer_id
JOIN product_catalog p ON o.product_id = p.product_id
WHERE o.order_year = 2024 AND o.order_month = 3;
