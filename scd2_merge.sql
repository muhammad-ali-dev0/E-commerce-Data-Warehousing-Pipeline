-- macros/scd2_merge.sql
-- Reusable macro for applying SCD Type 2 logic to any dimension
-- Usage:
--   {{ scd2_merge(
--        source_model = 'stg_customers',
--        target_table = 'dim_customer',
--        natural_key   = 'customer_id',
--        tracked_cols  = ['customer_segment', 'city', 'state', 'country', 'is_active'],
--        valid_from_col = 'updated_at'
--      )
--   }}

{% macro scd2_merge(source_model, target_table, natural_key, tracked_cols, valid_from_col='updated_at') %}

{%- set max_date = var('scd2_max_date', '9999-12-31 23:59:59') -%}

MERGE INTO {{ target_table }} AS target

USING (

    WITH incoming AS (
        SELECT * FROM {{ ref(source_model) }}
    ),

    current_dim AS (
        SELECT *
        FROM {{ target_table }}
        WHERE is_current = TRUE
    ),

    -- Hash-based change detection: only process rows where tracked columns changed
    changed AS (
        SELECT i.*
        FROM incoming i
        LEFT JOIN current_dim d USING ({{ natural_key }})

        WHERE d.{{ natural_key }} IS NULL  -- new record
           OR MD5(CONCAT(
                {% for col in tracked_cols %}
                COALESCE(CAST(i.{{ col }} AS STRING), '')
                {%- if not loop.last %}, {% endif %}
                {% endfor %}
              )) !=
              MD5(CONCAT(
                {% for col in tracked_cols %}
                COALESCE(CAST(d.{{ col }} AS STRING), '')
                {%- if not loop.last %}, {% endif %}
                {% endfor %}
              ))
    )

    SELECT * FROM changed

) AS source ON (
    target.{{ natural_key }} = source.{{ natural_key }}
    AND target.is_current = TRUE
)

-- Close the existing current record
WHEN MATCHED THEN UPDATE SET
    target.valid_to      = DATEADD('second', -1, source.{{ valid_from_col }}),
    target.is_current    = FALSE,
    target.dw_updated_at = CURRENT_TIMESTAMP

-- Insert brand-new customers (no existing record)
WHEN NOT MATCHED THEN INSERT (
    {{ natural_key }},
    {% for col in tracked_cols %}
    {{ col }},
    {% endfor %}
    valid_from,
    valid_to,
    is_current,
    dw_created_at,
    dw_updated_at
)
VALUES (
    source.{{ natural_key }},
    {% for col in tracked_cols %}
    source.{{ col }},
    {% endfor %}
    source.{{ valid_from_col }},
    CAST('{{ max_date }}' AS TIMESTAMP),
    TRUE,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
)

{% endmacro %}


-- ============================================================
-- macros/generate_date_spine.sql
-- Utility macro to generate a date spine for dim_date
-- ============================================================

{% macro generate_date_spine(start_date, end_date) %}

SELECT
    CAST(TO_CHAR(d.date_day, 'YYYYMMDD') AS INTEGER)   AS date_key,
    d.date_day                                          AS full_date,
    EXTRACT(DOW FROM d.date_day)                        AS day_of_week,
    TO_CHAR(d.date_day, 'Day')                          AS day_name,
    EXTRACT(DAY FROM d.date_day)                        AS day_of_month,
    EXTRACT(DOY FROM d.date_day)                        AS day_of_year,
    EXTRACT(WEEK FROM d.date_day)                       AS week_number,
    EXTRACT(MONTH FROM d.date_day)                      AS month_number,
    TO_CHAR(d.date_day, 'Month')                        AS month_name,
    TO_CHAR(d.date_day, 'Mon')                          AS month_short,
    EXTRACT(QUARTER FROM d.date_day)                    AS quarter,
    CONCAT('Q', EXTRACT(QUARTER FROM d.date_day))       AS quarter_name,
    EXTRACT(YEAR FROM d.date_day)                       AS year,
    CASE
        WHEN EXTRACT(MONTH FROM d.date_day) >= {{ var('fiscal_year_start_month', 10) }}
        THEN EXTRACT(YEAR FROM d.date_day) + 1
        ELSE EXTRACT(YEAR FROM d.date_day)
    END                                                 AS fiscal_year,
    EXTRACT(DOW FROM d.date_day) IN (0, 6)              AS is_weekend,
    EXTRACT(DOW FROM d.date_day) NOT IN (0, 6)          AS is_weekday,
    FALSE                                               AS is_holiday,
    NULL                                                AS holiday_name

FROM (
    {{ dbt_utils.date_spine(
        datepart = 'day',
        start_date = "cast('" ~ start_date ~ "' as date)",
        end_date   = "cast('" ~ end_date   ~ "' as date)"
    ) }}
) d

{% endmacro %}
