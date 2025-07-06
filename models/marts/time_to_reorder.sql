WITH customer_orders AS (
    SELECT
        customer_id,
        order_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS rn
    FROM {{ ref('stg_orders') }}
),
order_lags AS (
    SELECT
        customer_id,
        order_date,
        LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS previous_order_date
    FROM customer_orders
)
SELECT
    customer_id,
    order_date,
    previous_order_date,
    DATEDIFF(day, previous_order_date, order_date) AS days_between_orders
FROM order_lags
WHERE previous_order_date IS NOT NULL