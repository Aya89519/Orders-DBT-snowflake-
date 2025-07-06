WITH first_orders AS (
    SELECT
        customer_id,
        MIN(DATE_TRUNC('month', order_date)) AS cohort_month
    FROM {{ ref('stg_orders') }}
    GROUP BY customer_id
),
orders_with_cohort AS (
    SELECT
        o.customer_id,
        DATE_TRUNC('month', o.order_date) AS order_month,
        f.cohort_month,
        DATEDIFF(month, f.cohort_month, DATE_TRUNC('month', o.order_date)) AS months_since_first
    FROM {{ ref('stg_orders') }} o
    JOIN first_orders f ON o.customer_id = f.customer_id
),
retention_counts AS (
    SELECT
        cohort_month,
        months_since_first,
        COUNT(DISTINCT customer_id) AS active_customers
    FROM orders_with_cohort
    GROUP BY cohort_month, months_since_first
)
SELECT *
FROM retention_counts
ORDER BY cohort_month, months_since_first