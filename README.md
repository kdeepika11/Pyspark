# Databricks PySpark Mini Project — Lakehouse Pipeline (Bronze/Silver/Gold) + Incremental Load + Data Quality

This project demonstrates a beginner-friendly, end-to-end Data Engineering workflow in **Databricks** using **PySpark + Delta Lake**:

- ✅ Bronze/Silver/Gold Lakehouse tables
- ✅ Joins, aggregations, window functions (row_number, lag/lead)
- ✅ Incremental loading using a watermark (process only new arrivals)
- ✅ CDC-style upserts using `MERGE INTO` (Delta)
- ✅ Data Quality checks + audit logging
- ✅ Config-driven DQ rules (thresholds stored in a table)

---

## Architecture (High Level)

**Source (simulated batches / tables)**  
→ **Bronze** (raw + `_ingested_at`, `_source`)  
→ **Silver** (cleaned, typed, deduped)  
→ **Gold** (analytics aggregates)

Incremental processing uses a **watermark table** to track the last processed ingestion timestamp.

---

## Tech Stack

- Databricks (Community / Workspace)
- PySpark DataFrame API
- Delta Lake tables (managed tables)
- Spark SQL (`MERGE INTO`)
- Audit logging and config-driven DQ checks

---

## Data Model

### Customers
- CustomerId (bigint)
- CustomerName (string)
- Country (string)
- LastUpdatedDate (timestamp)

### Orders
- CustomerId (bigint)
- OrderId (bigint)
- Amount (double)
- OrderDate (date)
- _ingested_at (timestamp)
- _source (string)

---

## How to Run (Notebooks)

Open notebooks in order:

1. `notebooks/01_read_and_write.ipynb`  
   Read data, inspect schema, write parquet/delta.

2. `notebooks/02_select_filter.ipynb`  
   Select, filter, sort, basic transformations.

3. `notebooks/03_columns_cleaning_dates.ipynb`  
   `withColumn`, `when/otherwise`, `trim/upper`, date parsing.

4. `notebooks/04_groupby_aggregations.ipynb`  
   `groupBy`, `countDistinct`, sums, max/min, monthly grouping.

5. `notebooks/05_joins.ipynb`  
   Inner/left joins, customer spend, country revenue.

6. `notebooks/06_window_latest_per_customer.ipynb`  
   `row_number()` to get latest order per customer.

7. `notebooks/07_lag_lead_cdc_merge.ipynb`  
   `lag/lead` change detection + Delta `MERGE` upsert example.

8. `notebooks/08_bronze_silver_gold.ipynb`  
   Build Bronze/Silver/Gold tables and Gold aggregates.

9. `notebooks/09_incremental_watermark.ipynb`  
   Watermark-based incremental load from Bronze → Silver using MERGE.

10. `notebooks/10_data_quality_audit.ipynb`  
   Standard DQ checks (nulls, duplicates, negative values) and audit table logging.

11. `notebooks/11_dq_config_driven.ipynb`  
   Store DQ thresholds in a config table and auto-run checks.

---

## Tables Created

- Bronze:
  - `workspace.default.bronze_customers`
  - `workspace.default.bronze_orders`

- Silver:
  - `workspace.default.silver_customers`
  - `workspace.default.silver_orders`

- Gold:
  - `workspace.default.gold_revenue_by_country`
  - `workspace.default.gold_latest_order_per_customer`
  - `workspace.default.gold_monthly_revenue`

- Pipeline metadata:
  - `workspace.default.etl_watermarks`
  - `workspace.default.etl_audit_results`
  - `workspace.default.dq_check_config`

---

## What This Proves (Skills)

- Building scalable transformations with PySpark
- Using Delta Lake for reliable upserts (MERGE)
- Incremental processing (watermark pattern)
- Data Quality gates + auditability
- Clean repo + reproducible notebooks (portfolio-ready)

---

## Notes
- This repo uses Databricks-managed Delta tables (Unity Catalog / workspace schema).
- If your workspace restricts DBFS root, managed tables avoid path permission issues.
