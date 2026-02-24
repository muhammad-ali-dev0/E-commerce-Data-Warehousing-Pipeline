-- tests/assert_scd2_no_gaps.sql
-- Validates SCD2 integrity for dim_customer:
-- 1. No gaps between valid_from/valid_to for the same customer
-- 2. Exactly one is_current = TRUE per customer
-- 3. valid_from is always before valid_to

-- ---- Test 1: Exactly one current record per customer ----
WITH current_counts AS (
    SELECT
        customer_id,
        COUNT(*) AS current_row_count
    FROM {{ ref('dim_customer') }}
    WHERE is_current = TRUE
    GROUP BY 1
    HAVING COUNT(*) != 1    -- should be exactly 1
),

-- ---- Test 2: No date gaps between versions ----
version_gaps AS (
    SELECT
        customer_id,
        valid_from,
        valid_to,
        LEAD(valid_from) OVER (
            PARTITION BY customer_id
            ORDER BY valid_from
        ) AS next_valid_from,

        -- Gap exists if next version starts more than 1 second after this one ends
        DATEDIFF(
            'second',
            valid_to,
            LEAD(valid_from) OVER (PARTITION BY customer_id ORDER BY valid_from)
        ) AS gap_seconds

    FROM {{ ref('dim_customer') }}
    WHERE is_current = FALSE    -- only check closed records
),

gaps AS (
    SELECT *
    FROM version_gaps
    WHERE gap_seconds > 1   -- allow 1 second tolerance for the -1s logic in MERGE
),

-- ---- Test 3: valid_from < valid_to ----
bad_dates AS (
    SELECT *
    FROM {{ ref('dim_customer') }}
    WHERE valid_from >= valid_to
),

-- Combine all failures
failures AS (
    SELECT customer_id, 'multiple_current_records' AS issue FROM current_counts
    UNION ALL
    SELECT customer_id, 'date_gap_found'           AS issue FROM gaps
    UNION ALL
    SELECT customer_id, 'invalid_date_range'       AS issue FROM bad_dates
)

SELECT * FROM failures
-- Test passes when 0 rows returned
