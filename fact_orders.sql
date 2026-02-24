-- models/marts/fact_orders.sql
-- Grain: one row per order line item
-- Incremental strategy: merge on order_key
-- Lookback window: reprocesses last N days for late-arriving data

{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'order_key',
    cluster_by = ['order_date', 'channel'],
    tags = ['facts', 'core', 'incremental'],
    post_hook = [
      "ALTER TABLE {{ this }} CLUSTER BY (order_date, channel)"
    ]
  )
}}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}

    {% if is_incremental() %}
    -- Incremental: only process recent + late-arriving data
    WHERE order_date >= (
        SELECT DATEADD('day', -{{ var('incremental_lookback_days') }}, MAX(order_date))
        FROM {{ this }}
    )
    {% endif %}
),

-- Point-in-time join: match customer to their attributes AT order time
-- Critical for SCD2 accuracy — don't just use is_current = TRUE
customer_dim AS (
    SELECT
        customer_id,
        customer_key,
        valid_from,
        valid_to
    FROM {{ ref('dim_customer') }}
),

product_dim AS (
    SELECT
        product_id,
        product_key,
        cost_price
    FROM {{ ref('dim_product') }}
),

geo_dim AS (
    SELECT
        city,
        state,
        country,
        geo_key
    FROM {{ ref('dim_geography') }}
),

date_dim AS (
    SELECT date_key, full_date
    FROM {{ ref('dim_date') }}
),

-- Tag first-ever order per customer (for new vs returning analysis)
first_orders AS (
    SELECT
        customer_id,
        MIN(order_date) AS first_order_date
    FROM {{ ref('stg_orders') }}
    GROUP BY 1
),

assembled AS (
    SELECT
        -- Surrogate key for this fact row
        {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} AS order_key,

        -- Foreign keys (surrogate — NOT natural keys)
        c.customer_key,
        p.product_key,
        d.date_key,
        g.geo_key,

        -- Degenerate dimensions (high-cardinality, no dim table needed)
        o.order_id,
        o.order_timestamp,
        o.order_date,
        o.channel,

        -- Additive measures
        o.quantity,
        o.unit_price,
        o.discount_pct,
        ROUND(o.gross_revenue * o.discount_pct / 100, 2)       AS discount_amount,
        o.gross_revenue,
        o.net_revenue,
        ROUND(p.cost_price * o.quantity, 2)                    AS cost_of_goods,
        ROUND(o.net_revenue - (p.cost_price * o.quantity), 2)  AS gross_profit,
        o.shipping_cost,
        ROUND(o.net_revenue * 0.08, 2)                         AS tax_amount,

        -- Semi-additive / non-additive
        o.order_status = 'returned'                            AS return_flag,
        fo.first_order_date = o.order_date                     AS is_first_order,

        -- Audit
        CURRENT_TIMESTAMP                                      AS dw_loaded_at

    FROM orders o

    -- Point-in-time SCD2 join (THE most important join in this warehouse)
    LEFT JOIN customer_dim c
        ON  o.customer_id  = c.customer_id
        AND o.order_date   >= c.valid_from
        AND o.order_date   <  c.valid_to

    LEFT JOIN product_dim p
        ON o.product_id = p.product_id

    LEFT JOIN geo_dim g
        ON  o.city    = g.city
        AND o.state   = g.state
        AND o.country = g.country

    LEFT JOIN date_dim d
        ON o.order_date = d.full_date

    LEFT JOIN first_orders fo
        ON o.customer_id = fo.customer_id
)

SELECT * FROM assembled
