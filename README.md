\# SNCF Data Pipeline — Databricks \& dbt



End-to-end data engineering project built on public SNCF (French national railway) GTFS data, using a medallion architecture on Databricks with PySpark and dbt for transformation and data quality testing. The final metrics are visualized in a Power BI dashboard.



This is a personal portfolio project, built to practice and demonstrate the modern data stack (PySpark, Delta Lake, Databricks, dbt) end-to-end, following advice from my mentor to build hands-on projects with public datasets.



\## Dashboard Preview



!\[SNCF Dashboard](docs/dashboard\_screenshot.png)



\## Architecture



This project follows the \*\*medallion architecture\*\* (bronze → silver → gold):



Raw GTFS data (SNCF)

│

▼

🥉 BRONZE → Raw ingestion, minimal transformation

│

▼

🥈 SILVER → Cleaned, typed, deduplicated, joined with reference tables

│

▼

🥇 GOLD → Business-level aggregations (punctuality, delays, traffic)

│

▼

📊 Power BI Dashboard





\## Tech Stack



\- \*\*Databricks\*\* — compute platform, notebook orchestration

\- \*\*PySpark\*\* — data ingestion and transformation (bronze/silver layers)

\- \*\*Delta Lake\*\* — storage format, ACID transactions, merge/upsert logic

\- \*\*dbt\*\* — declarative transformations and data quality testing (gold layer)

\- \*\*Power BI\*\* — final reporting and visualization



\## Project Structure



sncf\_dbt/

├── notebooks/ # PySpark notebooks (Databricks)

│ ├── 00\_setup\_snc\_project.py # Environment \& catalog setup

│ ├── 01\_ingest\_gtfs\_bronze.py # Raw GTFS ingestion → bronze

│ ├── 02\_transform\_silver.py # Cleaning, typing, dedup → silver

│ ├── 03\_create\_gold\_metrics.py # Business aggregations → gold

│ ├── 04\_data\_quality.py # Manual PySpark data quality checks

│ └── 05\_delta\_merge\_demo.py # Delta Lake merge/upsert example

│

├── models/ # dbt models (gold layer)

│ ├── staging/

│ │ └── source.yml

│ └── gold/

│ ├── dbt\_gold\_trip\_summary.sql

│ ├── dbt\_gold\_route\_summary.sql

│ ├── dbt\_gold\_station\_traffic.sql

│ └── schema.yml # dbt tests: not\_null, relationships, custom tests

│

├── tests/ # custom dbt tests

├── macros/ # dbt macros

├── seeds/ # reference/lookup data

├── docs/ # documentation assets (screenshots)

├── pbi\_sncf.pbix # Power BI dashboard file

├── dbt\_project.yml

└── README.md





\## Data Quality



Data quality is addressed at two levels:



\*\*1. PySpark checks (silver layer, `04\_data\_quality.py`)\*\*

\- Null value counts per column

\- Duplicate detection via `groupBy`

\- Orphan key detection via `left\_anti` join

\- Sequence order validation using window functions (`lag`)

\- Quarantine table for records failing validation



\*\*2. dbt tests (gold layer)\*\*

\- `not\_null` tests on primary keys

\- `relationships` tests validating referential integrity (e.g. every stop references a valid station)

\- Custom test: `assert\_no\_negative\_revenue.sql`



Run with:

```bash

dbt test

```



\## Key Metrics (Gold Layer)



\- \*\*Punctuality rate\*\* by line and station

\- \*\*Average delay\*\* by route and date

\- \*\*Station traffic\*\* (number of stops/trips per station)


C:\Users\Mayra\Desktop\AnalyticsEngineer_Practice\sncf_dbt\docs

\## Notes



\- Built entirely on \*\*public GTFS data\*\* published by SNCF — no proprietary or client data is used.

\- The Delta merge notebook (`05\_delta\_merge\_demo.py`) demonstrates incremental upsert logic typical of production pipelines.



\## Author



\*\*Mayra Suarez\*\* — Data Analyst / Analytics Engineer

\[LinkedIn](#) · \[GitHub](https://github.com/mayrasuarezc)





