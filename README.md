# Medical Insurance Claims & Denial Analytics

Insurance denials are expensive, but a denial code alone doesn't tell a revenue-cycle team what to fix first. This project takes CMS's synthetic Medicare claims data, models claim adjudication and denial reasons, and turns them into a ranked view of **which denial patterns are costing the most money and which ones are potentially preventable**.

The result is a data warehouse and Power BI dashboard designed to help a revenue-cycle team prioritize process improvements instead of reviewing denials one claim at a time.

## Business Problem

A few denied claims are easy to handle. Thousands of denials across providers, denial reasons, and A/R aging buckets are not.

The project answers questions such as:

- Which denial reasons occur most often?
- Which CARCs represent the largest estimated financial exposure?
- Which denials are potentially preventable?
- Which providers have weaker first-pass performance?
- Where is money sitting in 30/60/90+ day A/R?

### Example: CARC 18

CARC 18 means **"Exact duplicate claim/service."**

The CMS DE-SynPUF data does not contain a real denial/CARC field, so this project models adjudication and CARC assignment. Denied claims are identified from the payment data, and CARCs are assigned using a fixed, seeded distribution. CARC 18 has the highest modeled weight, so it becomes the most frequent denial reason in this dataset.

The pipeline then:

1. Identifies the modeled denial.
2. Assigns CARC 18.
3. Classifies it as **Preventable - Process**.
4. Counts the affected claims.
5. Estimates financial exposure using the average paid amount from real paid DE-SynPUF claims.

So the resulting denial counts and dollar exposure are **modeled estimates**, not real payer denial measurements. Replacing the simulation with 835/EOB remittance data would make those metrics real.

## Architecture

```text
CMS DE-SynPUF claims
        |
        v
Local landing zone
        |
        v
PySpark ingestion + standardization
        |
        v
Snowflake RAW
        |
        v
dbt staging
        |
        v
dbt star schema
        |
        v
KPI marts
        |
        v
Power BI Denial Control Tower
```

A DuckDB-based local prototype mirrors the same flow without requiring Snowflake, dbt, or Power BI. See `build_warehouse.py` and `sql/kpi_models.sql`.

## Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| Data | CMS DE-SynPUF | Synthetic Medicare claims |
| ETL | PySpark / DuckDB | Ingestion, standardization, deduplication |
| Warehouse | Snowflake | RAW data and dbt transformations |
| Analytics | dbt | Star schema, denial logic, tests, KPI marts |
| BI | Power BI | Denial Control Tower dashboard |
| Version Control | Git / GitHub | Source control |

## Data Model

The dbt model is built around a claim-level fact table and supporting dimensions:

- **Dim_Patient** — beneficiary demographics and chronic-condition count.
- **Dim_Provider** — provider information and claim volume.
- **Dim_Diagnosis** — ICD-9-CM diagnosis codes and broad categories.
- **Dim_CARC_Denials** — denial reasons and preventability classification.
- **Fact_Claims_Adjudication** — one row per deduplicated claim, with modeled adjudication status, CARC, and A/R aging.
- **KPI marts** — pre-aggregated tables for dashboard KPIs.

The diagnosis data is based on ICD-9-CM because the source predates the 2015 ICD-10 transition.

## How to Run

### 1. Local prototype

```bash
pip install duckdb pandas openpyxl

python build_warehouse.py
python export_dashboard_data.py
```

Then open:

```text
dashboard/denial_control_tower_dashboard.html
```

### 2. PySpark

```bash
spark-submit production/pyspark/ingest_claims.py   --input s3://your-bucket/synpuf/   --output s3://your-bucket/warehouse/

python production/pyspark/load_carc_dimension.py   --input CARC_Codes.xlsx   --output s3://your-bucket/warehouse/Dim_CARC_Denials
```

### 3. Snowflake

Load the Parquet output into the `RAW` schema. This stage contains the landing tables; the dimensional model is built by dbt.

### 4. dbt

```bash
cd production/dbt_project

dbt debug
dbt run
dbt test
```

This builds the dimensions, fact table, and KPI marts.

### 5. Power BI

Connect Power BI to the KPI marts for the dashboard, or connect to the full star schema for custom analysis.

See `production/powerbi/POWER_BI_SETUP.md` for the setup instructions.

## Denial Control Tower

The dashboard focuses on:

- First-Pass Clean Claim Rate
- Net Collection Ratio
- 30/60/90+ day A/R aging
- Top CARCs by estimated financial exposure
- Preventable vs. non-preventable denials
- Provider-level performance

For this modeled sample, the First-Pass Clean Claim Rate is approximately **96.2%**. CARC 18 (duplicate claim), CARC 16 (missing information), and CARC 197 (authorization-related) are the three highest-loss modeled denial reasons.

## Important Data Limitation

The DE-SynPUF dataset is synthetic and does not contain actual payer denial/CARC data.

Therefore:

- Denial status is modeled.
- CARC assignment is modeled.
- Preventability is rules-based.
- Dollar exposure uses real paid amounts but applies them to modeled denials.

The dashboard should therefore be viewed as an **analytics engineering demonstration**, not as an analysis of actual payer denial performance.

## Future Improvements

- Replace modeled denials with real 835/EOB remittance data.
- Replace the paid-amount proxy with actual billed/charge data.
- Add the full star schema to Power BI for deeper analysis.
- Add ICD-9 → ICD-10 mapping using CMS GEMs.
- Add incremental/merge loading in dbt.
- Add GitHub Actions to run `dbt build` against a Snowflake development database.
