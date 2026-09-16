# SNCF Data Pipeline — Databricks & dbt

End-to-end data engineering project built on public SNCF (French national railway) GTFS data, using a medallion architecture on Databricks with PySpark and dbt for transformation and data quality testing. The final metrics are visualized in a Power BI dashboard.

This is a personal portfolio project, built to practice and demonstrate the modern data stack (PySpark, Delta Lake, Databricks, dbt) end-to-end, following advice from my mentor to build hands-on projects with public datasets.

## Dashboard Preview

![SNCF Dashboard](C:\Users\Mayra\Desktop\AnalyticsEngineer_Practice\sncf_dbt\docs)

## Architecture

This project follows the medallion architecture:

- **Bronze** → Raw ingestion from SNCF GTFS data, minimal transformation
- **Silver** → Cleaned, typed, deduplicated, joined with reference tables
- **Gold** → Business-level aggregations (punctuality, delays, traffic)
- **Power BI** → Final dashboard built on top of the gold layer

## Tech Stack

- **Databricks** — compute platform, notebook orchestration
- **PySpark** — data ingestion and transformation (bronze/silver layers)
- **Delta Lake** — storage format, ACID transactions, merge/upsert logic
- **dbt** — declarative transformations and data quality testing (gold layer)
- **Power BI** — final reporting and visualization

## Project Structure

- `notebooks/` — PySpark notebooks (Databricks): ingestion, silver transformation, gold metrics, data quality checks, Delta merge demo
- `models/gold/` — dbt models for punctuality, route summary, and station traffic
- `models/staging/` — dbt source definitions
- `tests/` — custom dbt tests
- `macros/` — dbt macros
- `seeds/` — reference/lookup data
- `docs/` — documentation assets (screenshots)
- `pbi_sncf.pbix` — Power BI dashboard file
- `dbt_project.yml` — dbt project configuration


## Data Quality

Data quality is addressed at two levels:

**1. PySpark checks (silver layer, `04_data_quality.py`)**
- Null value counts per column
- Duplicate detection via `groupBy`
- Orphan key detection via `left_anti` join
- Sequence order validation using window functions (`lag`)
- Quarantine table for records failing validation

**2. dbt tests (gold layer)**
- `not_null` tests on primary keys
- `relationships` tests validating referential integrity (e.g. every stop references a valid station)
- Custom test: `assert_no_negative_revenue.sql`

Run with: `dbt test`

## Key Metrics (Gold Layer)

- Punctuality rate by line and station
- Average delay by route and date
- Station traffic (number of stops/trips per station)

## Notes

- Built entirely on public GTFS data published by SNCF — no proprietary or client data is used.
- The Delta merge notebook (`05_delta_merge_demo.py`) demonstrates incremental upsert logic typical of production pipelines.

## Author

**Mayra Suarez** — Data Analyst / Analytics Engineer
[GitHub](https://github.com/mayrasuarezc)




