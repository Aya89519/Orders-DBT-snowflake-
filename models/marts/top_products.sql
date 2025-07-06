SELECT
    p.product_name AS product_name,
    SUM(oi.total_price) AS revenue
FROM {{ ref('stg_order_items') }} oi
JOIN {{ ref('stg_products') }} p ON oi.product_id = p.product_id
GROUP BY p.product_name
ORDER BY revenue DESC
LIMIT 10