# Healthcare Insurance Claims & Denial Analytics

An end-to-end data pipeline that turns CMS Data Entrepreneurs' Synthetic Public Use File (DE-SynPUF) claims data into an executive dashboard for identifying insurance claim denial patterns and revenue-cycle bottlenecks.

## Business problem

Insurance claims are sometimes delayed or denied because of problems like duplicate submissions, missing information, or coverage issues. Finding these problems across thousands of claims can be difficult. This project uses CMS's synthetic healthcare claims data to build a data warehouse and an interactive Power BI dashboard that helps identify the most common denial reasons, compare provider performance, and highlight where improvements can reduce delays and financial losses.

## Architecture

```
CMS DE-SynPUF CSVs (Beneficiary Summary, Inpatient Claims, Outpatient Claims)
        |
        v
Local landing zone (data/raw/)
        |
        v
PySpark ingestion -> inpatient_claims_extract, outpatient_claims_extract, beneficiary_extract
        |
        v
PySpark standardization -> claim segments deduplicated, dates parsed, one row per claim
        |
        v
Snowflake RAW schema (landing tables only)
        |
        v
dbt staging (stg_fact_claims_adjudication)
        |
        v
dbt marts (dim_patient, dim_provider, dim_diagnosis, dim_carc_denials, fact_claims_adjudication)
        |
        v
dbt KPI marts (kpi_header_cards, kpi_clean_claim_rate_by_provider, kpi_ar_aging_matrix, kpi_top_carc_denials)
        |
        v
Power BI Denial Control Tower dashboard
```

A DuckDB-based local prototype mirrors this same pipeline end-to-end without Snowflake/dbt/Power BI installed, for fast iteration — see `build_warehouse.py` and `sql/kpi_models.sql`.

## Technology stack

| Layer | Tool | Purpose |
|---|---|---|
| Data source | CMS DE-SynPUF | Synthetic Medicare beneficiary and claims data |
| ETL | PySpark / DuckDB | Ingest, standardize types, deduplicate claim segments |
| Local storage | CSV / DuckDB file | Landing zone and prototype warehouse during local development |
| Data warehouse | Snowflake | RAW landing tables, plus compute for dbt's transformations |
| Analytics engineering | dbt | Star schema, denial-simulation logic, testing, KPI marts |
| BI and reporting | Power BI | Denial Control Tower dashboard |
| Version control | Git and GitHub | Source control and portfolio repository |

## Data model

The dimensional model is built in dbt, from the CMS DE-SynPUF Inpatient and Outpatient Claims samples and the 2008–2010 Beneficiary Summary files, after PySpark has deduplicated and standardized them.

- **Dim_Patient** — one row per beneficiary (latest snapshot year), with demographics and a `chronic_condition_count` derived from the 11 CMS chronic-condition flags. Built in `dbt/models/marts/dim_patient.sql`.
- **Dim_Provider** — one row per `PRVDR_NUM`, with attending NPI and claim volume.
- **Dim_Diagnosis** — one row per ICD-9-CM diagnosis code appearing on a claim, with a `diagnosis_category_approx` derived in dbt (Circulatory, Endocrine/Metabolic, Respiratory, Injury/Poisoning, V-code, E-code, Other). *Note: this data predates the 2015 ICD-10 transition — see Future Improvements.*
- **Dim_CARC_Denials** — one row per Claim Adjustment Reason Code, with a `preventability_bucket` derived in dbt (Preventable – Process, Preventable – Front-End, Non-Preventable – Coverage, Non-Preventable – Clinical, Non-Preventable – Patient Responsibility, Unclassified).
- **Fact_Claims_Adjudication** — one row per claim (multi-segment claims deduplicated), joined to a simulated `adjudication_status` and `CARC_CODE` since the source data carries no true denial field, plus an `AR_AGING_BUCKET` computed against the sample's own as-of date.
- **KPI marts** (`kpi_header_cards`, `kpi_clean_claim_rate_by_provider`, `kpi_ar_aging_matrix`, `kpi_top_carc_denials`) — the wide, pre-aggregated analytics marts that power the dashboard, computed in dbt.

## How to run

### 1. Local prototype (DuckDB, no other infrastructure needed)

```
pip install duckdb pandas openpyxl
python build_warehouse.py           # builds the star schema + denial simulation
python export_dashboard_data.py     # runs sql/kpi_models.sql, exports dashboard JSON
```

Open `dashboard/denial_control_tower_dashboard.html` to view the results directly.

### 2. PySpark (production-scale ingestion)

```
spark-submit production/pyspark/ingest_claims.py \
  --input  s3://your-bucket/synpuf/ \
  --output s3://your-bucket/warehouse/

python production/pyspark/load_carc_dimension.py \
  --input  CARC_Codes.xlsx \
  --output s3://your-bucket/warehouse/Dim_CARC_Denials
```

### 3. Snowflake (RAW schema only)

Load the Parquet output from step 2 into a `RAW` schema in Snowflake. This step loads landing tables only — the star schema (dimensions, fact, marts) does not exist yet at this point.

### 4. dbt (builds the entire star schema and KPI marts)

```
cd production/dbt_project
dbt debug
dbt run
dbt test
```

`dbt run` creates every star-schema and mart object — `dim_patient`, `dim_provider`, `dim_diagnosis`, `dim_carc_denials`, `fact_claims_adjudication`, and the four `kpi_*` marts — from the RAW tables loaded in step 3. `dbt test` runs the `not_null`, `unique`, and `accepted_values` data-integrity tests defined in `schema.yml`.

### 5. Power BI

Connect Power BI to the `kpi_*` marts for a fast start, or to the full star schema for custom measures — see `production/powerbi/POWER_BI_SETUP.md` for field-by-field visual instructions, and `Claims_Adjudication_PowerBI_Data.xlsx` for a ready-to-import data source covering both approaches.

## Denial Control Tower dashboard

KPIs and visuals built from the KPI marts:

- First-Pass Clean Claim Rate (national and by provider)
- Net Collection Ratio
- 30/60/90+ day A/R aging, by provider (heatmap matrix)
- Top Claim Adjustment Reason Codes (CARCs) ranked by estimated financial loss
- Preventable vs. non-preventable denial mix

Across the claims in this sample, the First-Pass Clean Claim Rate is approximately 96.2%, with duplicate submissions (CARC 18), missing information (CARC 16), and absent authorization (CARC 197) the three highest-loss denial reasons.

## Future improvements

- **Load the full star schema into Power BI** rather than just the pre-aggregated KPI marts: import `Fact_Claims_Adjudication` alongside all four `Dim_*` tables, build relationships in Model view on `PATIENT_ID` / `PROVIDER_ID` / `DIAGNOSIS_CODE` / `CARC_CODE`, and write custom DAX measures directly against the claim grain — enabling slices the current marts don't support (e.g., denial rate by patient age band or chronic-condition count).
- Replace the rules-based denial simulation with real 835/EOB remittance data once available, so CARC assignment reflects actual payer adjudication rather than a modeled distribution.
- Apply a CMS General Equivalence Mapping (GEM) crosswalk to translate `Dim_Diagnosis`'s ICD-9-CM codes into ICD-10-CM, enabling joins against current code lookups and category rollups.
- Add incremental/merge loading in dbt once newer claims extracts are available, rather than full-refresh.
- Extend the model with a true charge-master or billed-amount source, replacing the avg-paid-amount proxy currently used for "$ at risk" on denied claims.
- Add a CI job (GitHub Actions) that runs `dbt build` on every pull request against a Snowflake dev database.
