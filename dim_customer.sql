-- models/marts/dim_customer.sql
-- SCD Type 2 dimension using dbt snapshots pattern
-- Each customer attribute change creates a new row
-- Join to fact table using: valid_from <= order_date < valid_to

-- NOTE: In dbt, SCD2 is best handled via SNAPSHOTS (snapshots/snap_customers.sql)
-- This model reads from the snapshot table and adds business-friendly columns

{{
  config(
    materialized = 'table',
    schema = 'core',
    tags = ['dimensions', 'scd2']
  )
}}

WITH snapshot_data AS (
    -- dbt snapshot produces: dbt_scd_id, dbt_valid_from, dbt_valid_to, dbt_updated_at
    SELECT * FROM {{ ref('snap_customers') }}
),

enriched AS (
    SELECT
        -- Surrogate key: unique per customer VERSION (not per customer)
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_valid_from']) }}
                                                        AS customer_key,

        -- Natural key
        customer_id,

        -- Attributes (captured at point-in-time)
        email,
        full_name,
        first_name,
        last_name,
        acquisition_channel,
        acquisition_date,
        customer_segment,         -- THIS is what SCD2 tracks
        lifetime_value,
        city,
        state,
        country,
        is_active,

        -- SCD2 metadata
        dbt_valid_from              AS valid_from,
        COALESCE(
            dbt_valid_to,
            CAST('{{ var("scd2_max_date") }}' AS TIMESTAMP)
        )                           AS valid_to,

        -- Convenience flag (avoids having to check valid_to in every query)
        dbt_valid_to IS NULL        AS is_current,

        -- Days this version was/is active
        DATEDIFF(
            'day',
            dbt_valid_from,
            COALESCE(dbt_valid_to, CURRENT_TIMESTAMP)
        )                           AS days_active,

        -- Version number for this customer (1 = original, 2 = first change, etc.)
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY dbt_valid_from
        )                           AS version_number,

        -- Audit
        dbt_updated_at              AS dw_updated_at

    FROM snapshot_data
)

SELECT * FROM enriched
