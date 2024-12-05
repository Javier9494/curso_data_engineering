WITH delivery_data AS (
    SELECT
        stg_orders.ORDER_ID,
        stg_orders.ESTIMATED_DELIVERY_AT,
        stg_orders.DELIVERED_AT,
        DATEDIFF(day, stg_orders.ESTIMATED_DELIVERY_AT, stg_orders.DELIVERED_AT) AS delivery_delay,
        CASE
            WHEN stg_orders.DELIVERED_AT <= stg_orders.ESTIMATED_DELIVERY_AT THEN 'On Time'
            WHEN stg_orders.DELIVERED_AT > stg_orders.ESTIMATED_DELIVERY_AT THEN 'Late'
            ELSE 'Not Delivered'
        END AS delivery_status
    FROM
        {{ ref('stg_sql_server_dbo__orders') }} stg_orders
),
delivery_summary AS (
    SELECT
        delivery_status,
        COUNT(ORDER_ID) AS total_orders,
        AVG(delivery_delay) AS avg_delivery_delay
    FROM
        delivery_data
    GROUP BY
        delivery_status
)
SELECT
    delivery_status,
    total_orders,
    avg_delivery_delay
FROM
    delivery_summary
