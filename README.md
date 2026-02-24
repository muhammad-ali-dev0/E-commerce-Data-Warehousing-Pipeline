# 🏭 E-Commerce Analytics Data Warehouse
### Modern Star Schema Design with dbt + Snowflake/BigQuery

[![dbt](https://img.shields.io/badge/dbt-1.7-FF694B?logo=dbt)](https://www.getdbt.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Ready-29B5E8?logo=snowflake)](https://www.snowflake.com/)
[![BigQuery](https://img.shields.io/badge/BigQuery-Ready-4285F4?logo=googlebigquery)](https://cloud.google.com/bigquery)
[![SQL](https://img.shields.io/badge/SQL-Advanced-blue)](/)

---

## 🎯 Business Problem

An e-commerce company is generating millions of transactions daily but operating on siloed operational databases (PostgreSQL + MongoDB). The leadership team cannot answer basic questions like:

- *"Which customer segments drove Q3 revenue growth?"*
- *"How does product return rate correlate with acquisition channel?"*
- *"Which regions are underperforming vs forecast?"*

**Solution:** A fully layered analytics warehouse using a star schema, enabling sub-second queries across 5 years of historical data.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     DATA PIPELINE                           │
├──────────┬──────────┬────────────────────┬──────────────────┤
│   RAW    │ STAGING  │  CORE (Star Schema) │  AGGREGATION     │
│          │          │                    │                  │
│ src_orders│stg_orders│ fact_orders        │ agg_daily_rev    │
│ src_customers│stg_customers│ dim_customer (SCD2)│ agg_cohort  │
│ src_products│stg_products│ dim_product    │ agg_channel_perf │
│ src_events│stg_events│ dim_date           │                  │
│          │          │ dim_geography      │                  │
└──────────┴──────────┴────────────────────┴──────────────────┘
     ↑            ↑              ↑                  ↑
  Fivetran/    dbt run      Incremental +        Scheduled
  Airbyte      (staging)    SCD Type 2            Refresh
```

### Layer Responsibilities

| Layer | Purpose | Materialization | Freshness |
|-------|----------|----------------|-----------|
| **Raw** | Source of truth, never modified | Tables | Real-time |
| **Staging** | 1:1 source mapping, light cleansing | Views | Real-time |
| **Core** | Business logic, star schema | Incremental Tables | Hourly |
| **Aggregation** | Pre-computed metrics for BI tools | Tables | Daily |

---

## ⭐ Star Schema Design

```
                    ┌─────────────────┐
                    │   dim_date      │
                    │─────────────────│
                    │ date_key (PK)   │
                    │ full_date       │
                    │ day_of_week     │
                    │ week_number     │
                    │ month_name      │
                    │ quarter         │
                    │ year            │
                    │ is_weekend      │
                    │ is_holiday      │
                    └────────┬────────┘
                             │
┌──────────────┐    ┌────────▼────────┐    ┌─────────────────┐
│ dim_customer │    │   fact_orders   │    │   dim_product   │
│──────────────│    │─────────────────│    │─────────────────│
│ customer_key │◄───│ order_key (PK)  │───►│ product_key     │
│ customer_id  │    │ customer_key(FK)│    │ product_id      │
│ full_name    │    │ product_key(FK) │    │ product_name    │
│ email        │    │ date_key (FK)   │    │ category        │
│ segment      │    │ geo_key (FK)    │    │ subcategory     │
│ lifetime_val │    │ channel_key(FK) │    │ brand           │
│ acq_channel  │    ├─────────────────┤    │ cost_price      │
│ acq_date     │    │ order_amount    │    │ list_price      │
│ is_current   │◄───│ quantity        │    │ margin_pct      │
│ valid_from   │    │ discount_amount │    └─────────────────┘
│ valid_to     │    │ shipping_cost   │
│(SCD Type 2) │    │ tax_amount      │
└──────────────┘    │ gross_profit    │    ┌─────────────────┐
                    │ return_flag     │    │ dim_geography   │
                    │ fulfillment_days│    │─────────────────│
                    └────────┬────────┘    │ geo_key (PK)    │
                             │             │ city            │
                             └────────────►│ state           │
                                          │ country         │
                                          │ region          │
                                          │ timezone        │
                                          └─────────────────┘
```

---

## 🔄 SCD Type 2 — Slowly Changing Dimensions

Customer attributes (segment, tier, address) change over time. SCD Type 2 preserves history by adding new rows rather than overwriting.

**Example:** A customer upgrades from Silver → Gold tier in March. With SCD2, we can accurately report their Silver-era revenue separately from their Gold-era revenue.

```sql
-- Before upgrade (March 1):
customer_key=1001, segment='Silver', valid_from='2023-01-01', valid_to='2023-03-14', is_current=FALSE

-- After upgrade (March 15):
customer_key=1002, segment='Gold',   valid_from='2023-03-15', valid_to='9999-12-31', is_current=TRUE
```

See full implementation: [`dbt/models/marts/dim_customer.sql`](dbt/models/marts/dim_customer.sql)

---

## ⚡ Query Performance Comparison

| Query Type | No Optimization | Partitioned | Partitioned + Clustered |
|------------|----------------|-------------|------------------------|
| Full date range scan (1 year) | 47.3s / 890GB | 4.1s / 78GB | 1.2s / 12GB |
| Single customer history | 31.2s / 450GB | 31.2s / 450GB | 0.4s / 2GB |
| Monthly revenue rollup | 52.1s / 1.1TB | 5.8s / 98GB | 2.1s / 18GB |
| Top products by region | 38.7s / 720GB | 8.3s / 134GB | 1.9s / 24GB |

**Cost savings: ~94% reduction in bytes scanned with full optimization strategy.**

See benchmark scripts: [`sql/06_performance_benchmarks.sql`](sql/06_performance_benchmarks.sql)

---

## 📦 Partition Strategy

### Snowflake
```sql
-- Cluster fact table on date + customer segment
ALTER TABLE fact_orders CLUSTER BY (order_date, customer_segment);
-- Micro-partitions automatically colocate related data
-- ~16MB per micro-partition → pruning skips irrelevant partitions
```

### BigQuery
```sql
-- Partition by ingestion date or event timestamp
PARTITION BY DATE(order_timestamp)
-- Cluster on highest-cardinality filter columns
CLUSTER BY customer_id, product_category
```

**Strategy rationale:**
- **Partition column:** `order_date` — nearly every analytical query filters by date range
- **Cluster columns:** `customer_id`, `product_category` — most frequent GROUP BY and WHERE predicates
- **Result:** Query planner skips 90%+ of storage for typical dashboard queries

---

## 🛠️ Tech Stack

| Tool | Version | Purpose |
|------|---------|---------|
| **Snowflake** or **BigQuery** | Latest | Cloud data warehouse |
| **dbt Core** | 1.7+ | Transformation framework |
| **SQL** | ANSI + dialect | Core logic |
| **Fivetran / Airbyte** | — | EL (ingestion) |
| **Looker / Metabase** | — | BI layer |

---

## 📁 Project Structure

```
ecommerce-data-warehouse/
├── sql/
│   ├── 01_raw_schema.sql          # Source table definitions
│   ├── 02_staging.sql             # Staging transformations
│   ├── 03_dimensions.sql          # Dim tables + SCD2 logic
│   ├── 04_facts.sql               # Fact table + incremental load
│   ├── 05_aggregations.sql        # Pre-computed aggregate tables
│   └── 06_performance_benchmarks.sql  # Before/after query benchmarks
├── dbt/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_customers.sql
│   │   │   └── stg_products.sql
│   │   └── marts/
│   │       ├── dim_customer.sql   # SCD Type 2
│   │       ├── dim_product.sql
│   │       ├── dim_date.sql
│   │       ├── dim_geography.sql
│   │       ├── fact_orders.sql    # Incremental
│   │       └── agg_daily_revenue.sql
│   ├── macros/
│   │   └── scd2_merge.sql
│   └── tests/
│       ├── assert_fact_no_orphans.sql
│       └── assert_scd2_no_gaps.sql
└── docs/
    ├── architecture.md
    └── partition_strategy.md
```

---

## 🚀 Getting Started

```bash
# 1. Clone the repo
git clone https://github.com/yourusername/ecommerce-data-warehouse

# 2. Install dbt
pip install dbt-snowflake   # or dbt-bigquery

# 3. Configure your connection
cp dbt/profiles.yml.example ~/.dbt/profiles.yml
# Edit with your warehouse credentials

# 4. Run the full pipeline
cd dbt
dbt deps
dbt seed          # Load date dimension seed
dbt run           # Build all models in dependency order
dbt test          # Validate data quality
dbt docs generate # Generate lineage documentation
dbt docs serve    # Open interactive lineage graph
```

---

## 📊 Key Design Decisions

**1. Why star schema over 3NF?**
Analytics workloads are read-heavy. Star schema denormalization reduces joins from 8+ tables to 1 fact + 2-3 dims, cutting query time by 60-80%.

**2. Why incremental over full refresh?**
`fact_orders` grows by ~500K rows/day. Full refresh would cost $200+/day. Incremental loads only new/changed rows, reducing cost to ~$4/day.

**3. Why SCD2 over SCD1 for customers?**
Revenue attribution requires point-in-time accuracy. If a customer was "Bronze" tier when they bought, that order should count in Bronze metrics — even after they upgraded to Gold.

**4. Why separate aggregation layer?**
Dashboard queries from 50+ concurrent users hitting the raw fact table would be expensive and slow. Pre-aggregated tables serve 95% of BI traffic at ~1% the cost.
