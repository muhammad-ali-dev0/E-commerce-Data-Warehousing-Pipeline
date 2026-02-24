-- ============================================================
-- 04_facts.sql
-- Fact table: fact_orders
-- Strategy: INCREMENTAL load (only new/updated rows each run)
-- Uses date-range filtering to avoid full table scans
-- ============================================================

-- -------------------------------------------------------
-- Create fact table (run once)
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.fact_orders (
    -- Surrogate key
    order_key           STRING        NOT NULL,   -- MD5 of order_id

    -- Foreign keys → dimension surrogate keys
    customer_key        INTEGER       NOT NULL,   -- joins dim_customer
    product_key         STRING        NOT NULL,   -- joins dim_product
    date_key            INTEGER       NOT NULL,   -- joins dim_date (YYYYMMDD)
    geo_key             STRING,                   -- joins dim_geography

    -- Degenerate dimension (order_id stored directly, no dim table needed)
    order_id            STRING        NOT NULL,
    order_timestamp     TIMESTAMP     NOT NULL,
    order_date          DATE          NOT NULL,   -- matches partition column
    channel             STRING,                   -- web/app/marketplace/retail

    -- Measures
    quantity            INTEGER,
    unit_price          NUMERIC(12,2),
    discount_pct        NUMERIC(5,2),
    discount_amount     NUMERIC(12,2),
    gross_revenue       NUMERIC(12,2),
    net_revenue         NUMERIC(12,2),
    cost_of_goods       NUMERIC(12,2),
    gross_profit        NUMERIC(12,2),
    shipping_cost       NUMERIC(8,2),
    tax_amount          NUMERIC(8,2),

    -- Semi-additive facts
    return_flag         BOOLEAN       DEFAULT FALSE,
    is_first_order      BOOLEAN,

    -- Fulfillment
    fulfillment_days    INTEGER,                  -- days from order to delivery

    -- Audit
    dw_loaded_at        TIMESTAMP     DEFAULT CURRENT_TIMESTAMP

)
CLUSTER BY (order_date, channel)   -- Snowflake: micro-partition pruning
-- BigQuery equivalent: PARTITION BY order_date CLUSTER BY customer_id, channel
COMMENT = 'Grain: one row per order line item.'
;


-- -------------------------------------------------------
-- Incremental load procedure
-- Called by dbt / Airflow each pipeline run
-- Only processes rows with order_date >= last watermark
-- -------------------------------------------------------
MERGE INTO core.fact_orders AS target

USING (

    WITH source_orders AS (
        SELECT * FROM staging.stg_orders
        WHERE order_date >= (
            -- Watermark: reprocess last 3 days to catch late-arriving data
            SELECT DATEADD(day, -3, MAX(order_date))
            FROM core.fact_orders
        )
    ),

    -- Resolve customer_key: point-in-time join (critical for SCD2 accuracy)
    -- We want the customer's segment AS IT WAS on the order date
    customer_keys AS (
        SELECT
            o.order_id,
            d.customer_key

        FROM source_orders o
        LEFT JOIN core.dim_customer d
            ON  o.customer_id  = d.customer_id
            AND o.order_date   >= d.valid_from
            AND o.order_date   <  d.valid_to     -- point-in-time!
    ),

    -- Resolve product_key (SCD1 — just current)
    product_keys AS (
        SELECT
            o.order_id,
            p.product_key,
            p.cost_price

        FROM source_orders o
        LEFT JOIN core.dim_product p USING (product_id)
    ),

    -- Resolve geo_key
    geo_keys AS (
        SELECT
            o.order_id,
            g.geo_key

        FROM source_orders o
        LEFT JOIN core.dim_geography g
            ON  o.city    = g.city
            AND o.state   = g.state
            AND o.country = g.country
    ),

    -- Flag first-ever order per customer
    first_orders AS (
        SELECT
            customer_id,
            MIN(order_date) AS first_order_date

        FROM staging.stg_orders
        GROUP BY 1
    ),

    -- Assemble final fact rows
    assembled AS (
        SELECT
            MD5(o.order_id)                             AS order_key,
            ck.customer_key,
            pk.product_key,
            CAST(TO_CHAR(o.order_date, 'YYYYMMDD') AS INTEGER)  AS date_key,
            gk.geo_key,

            o.order_id,
            o.order_timestamp,
            o.order_date,
            o.channel,

            o.quantity,
            o.unit_price,
            o.discount_pct,
            ROUND(o.gross_revenue * o.discount_pct / 100, 2)    AS discount_amount,
            o.gross_revenue,
            o.net_revenue,
            ROUND(pk.cost_price * o.quantity, 2)                AS cost_of_goods,
            ROUND(o.net_revenue - (pk.cost_price * o.quantity), 2) AS gross_profit,
            o.shipping_cost,
            ROUND(o.net_revenue * 0.08, 2)                      AS tax_amount,  -- 8% flat rate example

            o.order_status = 'returned'                         AS return_flag,
            fo.first_order_date = o.order_date                  AS is_first_order,

            NULL::INTEGER                                        AS fulfillment_days,  -- set by separate events pipeline

            CURRENT_TIMESTAMP                                    AS dw_loaded_at

        FROM source_orders o
        LEFT JOIN customer_keys ck USING (order_id)
        LEFT JOIN product_keys  pk USING (order_id)
        LEFT JOIN geo_keys      gk USING (order_id)
        LEFT JOIN first_orders  fo ON o.customer_id = fo.customer_id
    )

    SELECT * FROM assembled

) AS source ON target.order_key = source.order_key

-- Update returned/updated orders
WHEN MATCHED AND (
    target.return_flag != source.return_flag
    OR target.net_revenue != source.net_revenue
) THEN UPDATE SET
    target.return_flag   = source.return_flag,
    target.net_revenue   = source.net_revenue,
    target.gross_profit  = source.gross_profit,
    target.dw_loaded_at  = CURRENT_TIMESTAMP

-- Insert new orders
WHEN NOT MATCHED THEN INSERT (
    order_key, customer_key, product_key, date_key, geo_key,
    order_id, order_timestamp, order_date, channel,
    quantity, unit_price, discount_pct, discount_amount,
    gross_revenue, net_revenue, cost_of_goods, gross_profit,
    shipping_cost, tax_amount, return_flag, is_first_order,
    fulfillment_days, dw_loaded_at
)
VALUES (
    source.order_key, source.customer_key, source.product_key,
    source.date_key, source.geo_key,
    source.order_id, source.order_timestamp, source.order_date, source.channel,
    source.quantity, source.unit_price, source.discount_pct, source.discount_amount,
    source.gross_revenue, source.net_revenue, source.cost_of_goods, source.gross_profit,
    source.shipping_cost, source.tax_amount, source.return_flag, source.is_first_order,
    source.fulfillment_days, source.dw_loaded_at
)
;


-- -------------------------------------------------------
-- Data quality checks (run after each load)
-- -------------------------------------------------------

-- 1. Orphaned fact rows (no matching customer)
SELECT COUNT(*) AS orphaned_orders
FROM core.fact_orders
WHERE customer_key IS NULL;
-- Expected: 0

-- 2. Revenue sanity check (net must be <= gross)
SELECT COUNT(*) AS revenue_errors
FROM core.fact_orders
WHERE net_revenue > gross_revenue;
-- Expected: 0

-- 3. Duplicate order check
SELECT order_id, COUNT(*) AS cnt
FROM core.fact_orders
GROUP BY 1
HAVING cnt > 1;
-- Expected: empty

-- 4. Completeness — today's orders loaded
SELECT order_date, COUNT(*) AS order_count
FROM core.fact_orders
WHERE order_date >= DATEADD(day, -3, CURRENT_DATE)
GROUP BY 1
ORDER BY 1 DESC;
