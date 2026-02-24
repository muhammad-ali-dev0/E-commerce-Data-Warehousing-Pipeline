-- ============================================================
-- 03_dimensions.sql
-- Core dimension tables
-- dim_customer uses SCD Type 2 for full history tracking
-- dim_product, dim_date, dim_geography use SCD Type 1
-- ============================================================


-- ============================================================
-- DIM_CUSTOMER — SCD TYPE 2
-- Tracks changes to customer attributes over time.
-- Each customer change creates a NEW row. Old row is closed
-- by setting valid_to and is_current = FALSE.
-- ============================================================

-- Step 1: Create the dimension table with SCD2 columns
CREATE TABLE IF NOT EXISTS core.dim_customer (
    customer_key        INTEGER AUTOINCREMENT PRIMARY KEY,  -- surrogate key
    customer_id         STRING  NOT NULL,                   -- natural key from source
    email               STRING,
    full_name           STRING,
    first_name          STRING,
    last_name           STRING,
    acquisition_channel STRING,
    acquisition_date    DATE,
    customer_segment    STRING,   -- Bronze/Silver/Gold/Platinum — THIS CHANGES
    lifetime_value      NUMERIC(12,2),
    city                STRING,
    state               STRING,
    country             STRING,
    is_active           BOOLEAN,

    -- SCD Type 2 tracking columns
    valid_from          TIMESTAMP NOT NULL,
    valid_to            TIMESTAMP NOT NULL DEFAULT '9999-12-31 23:59:59',
    is_current          BOOLEAN   NOT NULL DEFAULT TRUE,

    -- Audit
    dw_created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dw_updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
COMMENT = 'Customer dimension with SCD Type 2. Join on customer_key, not customer_id.'
;

-- Step 2: MERGE procedure — runs each pipeline cycle
-- Detects changed rows → closes old record → inserts new record
MERGE INTO core.dim_customer AS target

USING (
    -- Incoming: latest snapshot from staging
    WITH incoming AS (
        SELECT
            customer_id,
            email,
            full_name,
            first_name,
            last_name,
            acquisition_channel,
            acquisition_date,
            customer_segment,
            lifetime_value,
            city,
            state,
            country,
            is_active,
            updated_at AS valid_from

        FROM staging.stg_customers
    ),

    -- Current records in the dimension
    current_dim AS (
        SELECT *
        FROM core.dim_customer
        WHERE is_current = TRUE
    ),

    -- Identify rows that have actually changed (hash comparison is efficient)
    changed AS (
        SELECT
            i.customer_id,
            i.email,
            i.full_name,
            i.first_name,
            i.last_name,
            i.acquisition_channel,
            i.acquisition_date,
            i.customer_segment,
            i.lifetime_value,
            i.city,
            i.state,
            i.country,
            i.is_active,
            i.valid_from

        FROM incoming i
        LEFT JOIN current_dim d USING (customer_id)

        WHERE d.customer_id IS NULL  -- brand new customer
           OR MD5(CONCAT(
                COALESCE(i.customer_segment,''),
                COALESCE(i.city,''),
                COALESCE(i.state,''),
                COALESCE(i.country,''),
                COALESCE(CAST(i.is_active AS STRING),'')
              )) !=
              MD5(CONCAT(
                COALESCE(d.customer_segment,''),
                COALESCE(d.city,''),
                COALESCE(d.state,''),
                COALESCE(d.country,''),
                COALESCE(CAST(d.is_active AS STRING),'')
              ))  -- something changed → needs new SCD2 row
    )

    SELECT * FROM changed

) AS source ON (
    target.customer_id = source.customer_id
    AND target.is_current = TRUE
)

-- Close the existing current record
WHEN MATCHED THEN UPDATE SET
    target.valid_to    = DATEADD(second, -1, source.valid_from),
    target.is_current  = FALSE,
    target.dw_updated_at = CURRENT_TIMESTAMP

-- Insert new records (both new customers AND changed existing ones)
-- NOTE: The INSERT for changed customers is handled separately (see below)
WHEN NOT MATCHED THEN INSERT (
    customer_id, email, full_name, first_name, last_name,
    acquisition_channel, acquisition_date, customer_segment,
    lifetime_value, city, state, country, is_active,
    valid_from, valid_to, is_current
)
VALUES (
    source.customer_id, source.email, source.full_name,
    source.first_name, source.last_name,
    source.acquisition_channel, source.acquisition_date,
    source.customer_segment, source.lifetime_value,
    source.city, source.state, source.country, source.is_active,
    source.valid_from, '9999-12-31 23:59:59', TRUE
)
;

-- Step 3: Insert new version of CHANGED records (those we just closed above)
-- The MERGE closed them; now we open a fresh current row
INSERT INTO core.dim_customer (
    customer_id, email, full_name, first_name, last_name,
    acquisition_channel, acquisition_date, customer_segment,
    lifetime_value, city, state, country, is_active,
    valid_from, valid_to, is_current
)
SELECT
    s.customer_id, s.email, s.full_name, s.first_name, s.last_name,
    s.acquisition_channel, s.acquisition_date, s.customer_segment,
    s.lifetime_value, s.city, s.state, s.country, s.is_active,
    s.updated_at, '9999-12-31 23:59:59', TRUE

FROM staging.stg_customers s
INNER JOIN core.dim_customer d
    ON s.customer_id = d.customer_id
    AND d.is_current = FALSE                 -- just closed by MERGE
    AND d.valid_to > DATEADD(hour, -1, CURRENT_TIMESTAMP)  -- recently closed
;

-- ============================================================
-- SCD2 QUERY PATTERNS
-- ============================================================

-- Get CURRENT customer info (typical lookup)
SELECT * FROM core.dim_customer
WHERE is_current = TRUE;

-- Get customer info AT A SPECIFIC POINT IN TIME (e.g., at order time)
-- This is how fact tables join to dimensions for historical accuracy
SELECT d.*
FROM core.dim_customer d
WHERE d.customer_id = '{{customer_id}}'
  AND d.valid_from <= '{{order_date}}'
  AND d.valid_to   >= '{{order_date}}';

-- Audit: see full history of changes for a customer
SELECT
    customer_id,
    customer_segment,
    city,
    valid_from,
    valid_to,
    is_current,
    DATEDIFF(day, valid_from, IFF(is_current, CURRENT_TIMESTAMP, valid_to)) AS days_in_segment
FROM core.dim_customer
WHERE customer_id = '{{customer_id}}'
ORDER BY valid_from;


-- ============================================================
-- DIM_DATE — Static / pre-populated (SCD Type 0 — never changes)
-- ============================================================
CREATE TABLE IF NOT EXISTS core.dim_date (
    date_key        INTEGER PRIMARY KEY,   -- YYYYMMDD format for fast joins
    full_date       DATE    NOT NULL,
    day_of_week     INTEGER,               -- 1=Sunday, 7=Saturday
    day_name        STRING,
    day_of_month    INTEGER,
    day_of_year     INTEGER,
    week_number     INTEGER,
    month_number    INTEGER,
    month_name      STRING,
    month_short     STRING,
    quarter         INTEGER,
    quarter_name    STRING,
    year            INTEGER,
    fiscal_year     INTEGER,               -- adjust offset for your fiscal calendar
    fiscal_quarter  INTEGER,
    is_weekend      BOOLEAN,
    is_weekday      BOOLEAN,
    is_holiday      BOOLEAN,               -- populate via seed file
    holiday_name    STRING
)
;

-- Populate 10 years of dates (run once)
INSERT INTO core.dim_date
SELECT
    CAST(TO_CHAR(d.d, 'YYYYMMDD') AS INTEGER)   AS date_key,
    d.d                                          AS full_date,
    DAYOFWEEK(d.d)                               AS day_of_week,
    DAYNAME(d.d)                                 AS day_name,
    DAY(d.d)                                     AS day_of_month,
    DAYOFYEAR(d.d)                               AS day_of_year,
    WEEKOFYEAR(d.d)                              AS week_number,
    MONTH(d.d)                                   AS month_number,
    MONTHNAME(d.d)                               AS month_name,
    TO_CHAR(d.d, 'Mon')                          AS month_short,
    QUARTER(d.d)                                 AS quarter,
    CONCAT('Q', QUARTER(d.d))                    AS quarter_name,
    YEAR(d.d)                                    AS year,
    -- Fiscal year starts October 1 — adjust to match your business
    IFF(MONTH(d.d) >= 10, YEAR(d.d) + 1, YEAR(d.d)) AS fiscal_year,
    CASE
        WHEN MONTH(d.d) IN (10,11,12) THEN 1
        WHEN MONTH(d.d) IN (1,2,3)    THEN 2
        WHEN MONTH(d.d) IN (4,5,6)    THEN 3
        ELSE 4
    END                                          AS fiscal_quarter,
    DAYOFWEEK(d.d) IN (1, 7)                     AS is_weekend,
    DAYOFWEEK(d.d) NOT IN (1, 7)                 AS is_weekday,
    FALSE                                        AS is_holiday,   -- updated from seed
    NULL                                         AS holiday_name

FROM (
    SELECT DATEADD(day, seq4(), '2019-01-01') AS d
    FROM TABLE(GENERATOR(ROWCOUNT => 3652))   -- 10 years
) d
;


-- ============================================================
-- DIM_PRODUCT — SCD Type 1 (overwrite, no history needed)
-- Product prices and attributes are corrected, not versioned
-- ============================================================
CREATE OR REPLACE TABLE core.dim_product AS

SELECT
    {{ dbt_utils.surrogate_key(['product_id']) }}   AS product_key,
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    cost_price,
    list_price,
    margin_pct,
    weight_kg,
    is_active,
    launch_date,
    CURRENT_TIMESTAMP                               AS dw_updated_at

FROM staging.stg_products
;


-- ============================================================
-- DIM_GEOGRAPHY — SCD Type 1
-- ============================================================
CREATE OR REPLACE TABLE core.dim_geography AS

SELECT
    {{ dbt_utils.surrogate_key(['city', 'state', 'country']) }} AS geo_key,
    city,
    state,
    country,
    region,
    sub_region,
    timezone,
    latitude,
    longitude

FROM raw.src_geography
;
