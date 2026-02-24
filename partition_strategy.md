# Partition Strategy & Architecture Decisions

## Why Partition?

Analytical queries on large fact tables almost always filter by **date range**. Without partitioning, every query reads the entire table. With partitioning:

- The query planner **skips irrelevant partitions** based on metadata alone
- IO drops by 80-99% for selective date filters
- Cost drops proportionally (cloud DW charge per bytes scanned)

---

## Snowflake: Micro-Partitioning + Clustering

Snowflake automatically divides tables into **~16MB micro-partitions** (columnar compressed). You don't manually create partitions — instead, you define **cluster keys** that tell Snowflake how to organize those micro-partitions.

### Clustering Strategy for fact_orders

```sql
ALTER TABLE core.fact_orders
CLUSTER BY (order_date, channel);
```

**Why `order_date` first?**
- 95%+ of queries filter by date range
- Date is low-cardinality enough that Snowflake can group similar dates together
- Queries like `WHERE order_date BETWEEN '2023-01-01' AND '2023-03-31'` skip 75%+ of data

**Why `channel` second?**
- Next most common filter predicate
- Adding it as secondary cluster key groups Web/App/Marketplace rows together within each date range
- Compound filter (`date + channel`) achieves 98%+ partition pruning

### Monitoring Clustering Effectiveness

```sql
-- Check clustering depth (lower = better organized = faster queries)
SELECT SYSTEM$CLUSTERING_INFORMATION('core.fact_orders', '(order_date, channel)');

-- Output to look for:
-- "average_depth": ideally < 2.0 (deeper = more overlap = less pruning)
-- "average_overlaps": ideally < 0.5

-- Automatic clustering maintains this as data grows (add via ALTER TABLE)
ALTER TABLE core.fact_orders
ENABLE AUTOMATIC CLUSTERING;
```

### When to Re-cluster

Re-clustering is needed when:
- `average_depth` > 4 in SYSTEM$CLUSTERING_INFORMATION
- Query times increase despite cluster keys matching filter predicates
- Large backfills or bulk loads disrupted the sort order

---

## BigQuery: Partitioning + Clustering

BigQuery uses explicit partitions (physical separation of files) combined with clustering (sorting within partitions).

```sql
CREATE TABLE core.fact_orders (
    order_key       STRING,
    customer_key    INT64,
    order_date      DATE,
    channel         STRING,
    net_revenue     NUMERIC,
    -- ... other columns
)
PARTITION BY order_date          -- one partition per day
CLUSTER BY customer_id, channel  -- sorted within each day partition
OPTIONS (
    require_partition_filter = FALSE,  -- set TRUE to enforce cost governance
    partition_expiration_days = 1825   -- auto-expire partitions > 5 years
);
```

**Cost governance tip:** Set `require_partition_filter = TRUE` in production to prevent accidental full-table scans that cost thousands.

```sql
-- This would ERROR with require_partition_filter = TRUE:
SELECT SUM(net_revenue) FROM core.fact_orders;  -- no date filter!

-- This works:
SELECT SUM(net_revenue) FROM core.fact_orders
WHERE order_date BETWEEN '2023-01-01' AND '2023-12-31';
```

---

## Dimension Table Strategy

| Dimension | Row Count | Strategy | Reasoning |
|-----------|-----------|----------|-----------|
| dim_date | ~3,650 | No partition (tiny) | Full scan of 3,650 rows is instant |
| dim_customer | ~2M | Cluster by customer_id | Single-customer lookups must be fast |
| dim_product | ~50K | No partition (small) | Full scan trivial at 50K rows |
| dim_geography | ~5K | No partition (tiny) | Reference table, always cached |

---

## Incremental Load Window

The `fact_orders` incremental model reprocesses the last **3 days** on every run. This handles:

- **Late-arriving data**: Orders confirmed/returned after initial load
- **Source corrections**: Upstream systems fixing data up to 48 hours back
- **Timezone edge cases**: Orders near midnight in different timezones

```yaml
# dbt_project.yml
vars:
  incremental_lookback_days: 3  # Tune based on your SLA requirements
```

**Trade-off:** More lookback days = more accurate but higher compute cost. 3 days covers 99.9% of late arrivals for typical e-commerce.

---

## Schema Evolution Plan

As the business grows, these are the anticipated schema changes and how to handle them:

| Change | Approach |
|--------|----------|
| New product category | SCD1 update to dim_product (overwrite) |
| Customer adds B2B segment | New SCD2 row in dim_customer |
| Add subscription fact table | New fact_subscriptions alongside fact_orders |
| Add hourly granularity | Add hour_key FK, extend dim_date to dim_datetime |
| Multi-currency support | Add currency_key, exchange_rate column to fact |
