# Databricks E-commerce Lakehouse (End-to-End Data Engineering Project)

## Overview
This project implements an end-to-end **Databricks Lakehouse** pipeline for high-volume e-commerce event data.  
The goal is to demonstrate modern data engineering practices including **medallion architecture (Bronze/Silver/Gold)**, **Delta Lake**, **Spark-based ingestion and transformation**, and **workflow orchestration** using Databricks serverless compute.

The pipeline ingests synthetic clickstream-style events, validates and cleans them, and produces analytics-ready datasets suitable for BI tools and downstream consumers.

---

## Architecture

![Architecture Diagram](diagrams/architecture.png)

**High-level flow:**
1. Synthetic e-commerce events are generated using Apache Spark
2. Raw events are ingested into **Bronze Delta tables**
3. Data is cleaned, validated, and deduplicated into **Silver Delta tables**
4. Business-ready facts and aggregates are produced in **Gold Delta tables**
5. The entire pipeline is orchestrated using **Databricks Workflows** on serverless compute

---

## Data Source

The dataset is **synthetically generated** to simulate large-scale e-commerce traffic.  
This approach allows:
- Full control over volume and schema
- Public sharing of the repository (no licensing or NDA constraints)
- Testing of partitioning, deduplication, and incremental processing patterns

Event types include:
- `page_view`
- `product_view`
- `add_to_cart`
- `checkout_started`
- `purchase`

Each event contains user, session, product, device, platform, and marketing attribution fields.

---

## Medallion Architecture

### 🥉 Bronze Layer
**Table:** `ecomm_lakehouse.bronze_events_raw`

- Stores raw JSON event payloads
- Append-only
- Includes ingestion metadata (`ingest_ts`, `ingest_batch_id`, source)
- No transformations beyond basic capture

Purpose: **immutability and traceability**

---

### 🥈 Silver Layer
**Tables:**
- `ecomm_lakehouse.silver_events_clean`
- `ecomm_lakehouse.silver_events_quarantine`

**Silver Clean**
- Parsed and strongly typed columns
- Deduplicated by `event_id`
- Partitioned by `event_date`
- Enforced basic validation rules (required fields, non-negative values)

**Silver Quarantine**
- Records that fail validation
- Raw JSON preserved with a failure reason

Purpose: **data quality and correctness**

---

### 🥇 Gold Layer
**Tables:**
- `ecomm_lakehouse.gold_fact_purchases`
- `ecomm_lakehouse.gold_mart_funnel_daily`

Gold tables contain analytics-ready datasets:
- Purchase facts with revenue metrics
- Daily funnel aggregates (page → product → cart → checkout → purchase)

Purpose: **business analytics and reporting**

---

## Orchestration

The pipeline is orchestrated using **Databricks Workflows** with three dependent tasks:

1. `01_generate_bronze`  
   Generates synthetic e-commerce events and writes to the Bronze table  
   (parameterized with `rows` to control data volume)

2. `02_bronze_to_silver`  
   Parses raw JSON, validates records, deduplicates events, and writes to Silver tables

3. `03_silver_to_gold`  
   Builds analytics-ready Gold tables and aggregates

All tasks run on **Databricks serverless compute**, which abstracts infrastructure management while still executing Spark workloads.

---

## Data Quality & Validation

The pipeline enforces multiple quality checks:
- Required field validation (`event_id`, `event_ts`, `event_name`, `user_id`)
- Enum validation for event types
- Non-negative price checks
- Quantity validation for purchase-related events
- Deduplication using window functions

Invalid records are quarantined rather than dropped, enabling auditability and debugging.

---

## Scalability & Performance Considerations

Key design choices:
- **Delta Lake** for ACID transactions and schema enforcement
- **Partitioning by `event_date`** for efficient reads
- **Parameterized ingestion** to simulate large volumes (millions of rows per run)
- Idempotent transformations suitable for retries and backfills

The same design can scale to cloud object storage (S3 / ADLS) in a production environment.

---

## Repository Structure

```text
databricks-ecomm-lakehouse/
├── notebooks/
│ ├── 01_generate_bronze.py
│ ├── 02_bronze_to_silver.py
│ └── 03_silver_to_gold.py
├── jobs/
│ └── ecomm_lakehouse_job.json
├── diagrams/
│ └── architecture.png
├── screenshots/
│ ├── job_graph.png
│ └── gold_funnel.png
└── README.md
```

---

## How to Run

1. Create a Databricks workspace (Free / Trial)
2. Import notebooks into the workspace
3. Create a Databricks Workflow with the three notebooks
4. Pass the `rows` parameter to the ingestion task
5. Run the job and inspect Bronze, Silver, and Gold tables

---

## Future Improvements

- Add dbt models and tests on top of Gold tables
- Implement incremental Silver processing using batch identifiers
- Add data freshness and row-count quality gates
- Extend dataset with product and customer dimension tables
- Add BI dashboards (Databricks SQL / external tools)

---

## Key Takeaways

This project demonstrates:
- End-to-end data pipeline design
- Spark + Delta Lake usage
- Medallion architecture best practices
- Orchestration with Databricks Workflows
- Practical data quality handling at scale
