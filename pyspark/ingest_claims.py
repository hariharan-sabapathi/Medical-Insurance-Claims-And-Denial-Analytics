"""
Claims Adjudication & Denial Intelligence Platform
Production PySpark ETL — Step A/B (Ingestion + Star Schema staging)

Mirrors the DuckDB prototype logic (build_warehouse.py) at production scale.
Run: spark-submit ingest_claims.py --input s3://.../synpuf/ --output s3://.../warehouse/
"""
import argparse
from pyspark.sql import SparkSession, functions as F, Window

def get_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="Base path containing the raw CMS SynPUF CSVs")
    p.add_argument("--output", required=True, help="Base path to write Parquet star-schema tables")
    return p.parse_args()

def main():
    args = get_args()
    spark = (SparkSession.builder
             .appName("claims-adjudication-etl")
             .config("spark.sql.shuffle.partitions", "200")
             .getOrCreate())

    inpatient = (spark.read.option("header", True).csv(f"{args.input}/DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.csv")
                 .withColumn("CLAIM_TYPE", F.lit("Inpatient")))
    outpatient = (spark.read.option("header", True).csv(f"{args.input}/DE1_0_2008_to_2010_Outpatient_Claims_Sample_1.csv")
                  .withColumn("CLAIM_TYPE", F.lit("Outpatient")))

    bene_frames = []
    for yr in (2008, 2009, 2010):
        df = (spark.read.option("header", True).csv(f"{args.input}/DE1_0_{yr}_Beneficiary_Summary_File_Sample_1.csv")
              .withColumn("SNAPSHOT_YEAR", F.lit(yr)))
        bene_frames.append(df)
    bene_all = bene_frames[0]
    for df in bene_frames[1:]:
        bene_all = bene_all.unionByName(df, allowMissingColumns=True)

    # ---- Dim_Patient: latest snapshot per beneficiary + chronic condition count ----
    w = Window.partitionBy("DESYNPUF_ID").orderBy(F.col("SNAPSHOT_YEAR").desc())
    chronic_cols = ["SP_ALZHDMTA","SP_CHF","SP_CHRNKIDN","SP_CNCR","SP_COPD",
                     "SP_DEPRESSN","SP_DIABETES","SP_ISCHMCHT","SP_OSTEOPRS","SP_RA_OA","SP_STRKETIA"]
    chronic_count = sum(F.when(F.col(c) == "1", 1).otherwise(0) for c in chronic_cols)

    dim_patient = (bene_all
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .withColumn("BIRTH_DT", F.to_date("BENE_BIRTH_DT", "yyyyMMdd"))
        .withColumn("DEATH_DT", F.to_date("BENE_DEATH_DT", "yyyyMMdd"))
        .withColumn("SEX", F.when(F.col("BENE_SEX_IDENT_CD") == "1", "Male")
                              .when(F.col("BENE_SEX_IDENT_CD") == "2", "Female").otherwise("Unknown"))
        .withColumn("CHRONIC_CONDITION_COUNT", chronic_count)
        .select(F.col("DESYNPUF_ID").alias("PATIENT_ID"), "BIRTH_DT", "DEATH_DT", "SEX",
                "BENE_RACE_CD", "SP_STATE_CODE", "CHRONIC_CONDITION_COUNT", "SNAPSHOT_YEAR"))

    # ---- Dim_Provider ----
    claims_union = inpatient.select("PRVDR_NUM", "AT_PHYSN_NPI").unionByName(
        outpatient.select("PRVDR_NUM", "AT_PHYSN_NPI"))
    dim_provider = (claims_union.filter(F.col("PRVDR_NUM").isNotNull())
        .groupBy("PRVDR_NUM")
        .agg(F.first("AT_PHYSN_NPI", ignorenulls=True).alias("ATTENDING_NPI"),
             F.count("*").alias("CLAIM_VOLUME"))
        .withColumnRenamed("PRVDR_NUM", "PROVIDER_ID"))

    # ---- Dim_Diagnosis ----
    # NOTE: SynPUF (2008-2010) predates the ICD-10 transition (Oct 2015). Diagnosis
    # codes are ICD-9-CM. If a true ICD-9 -> ICD-10 mapping is required downstream,
    # apply the CMS General Equivalence Mapping (GEM) crosswalk here before this
    # step; it is not included in the source files supplied for this project.
    dx_cols = [c for c in inpatient.columns if c.startswith("ICD9_DGNS_CD_")][:3]
    dx_codes = None
    for src in (inpatient, outpatient):
        for c in dx_cols:
            col_df = src.select(F.col(c).alias("CODE")).filter(F.col("CODE").isNotNull())
            dx_codes = col_df if dx_codes is None else dx_codes.unionByName(col_df)
    dim_diagnosis = (dx_codes.distinct()
        .withColumn("CODE_SYSTEM", F.lit("ICD-9-CM"))
        .withColumn("DIAGNOSIS_CATEGORY_APPROX",
            F.when(F.col("CODE").startswith("V"), "Supplemental/V-code")
             .when(F.col("CODE").startswith("E"), "External cause/E-code")
             .otherwise("Other"))
        .withColumn("DESCRIPTION", F.lit(None).cast("string"))
        .withColumnRenamed("CODE", "DIAGNOSIS_CODE"))

    # ---- Claim-level base (deduplicate SEGMENT rows to one row per CLAIM_ID) ----
    def claim_base(df):
        return df.select(
            F.col("CLM_ID").alias("CLAIM_ID"),
            F.col("DESYNPUF_ID").alias("PATIENT_ID"),
            F.col("PRVDR_NUM").alias("PROVIDER_ID"),
            "CLAIM_TYPE",
            F.to_date("CLM_FROM_DT", "yyyyMMdd").alias("CLM_FROM_DT"),
            F.to_date("CLM_THRU_DT", "yyyyMMdd").alias("CLM_THRU_DT"),
            F.col("CLM_PMT_AMT").cast("double").alias("CLM_PMT_AMT"),
            F.col("NCH_PRMRY_PYR_CLM_PD_AMT").cast("double").alias("PRIMARY_PYR_PD_AMT"),
            F.col("ICD9_DGNS_CD_1").alias("PRIMARY_DIAGNOSIS_CODE"),
        )
    claim_detail = claim_base(inpatient).unionByName(claim_base(outpatient))

    fact_base = (claim_detail.groupBy("CLAIM_ID").agg(
        F.first("PATIENT_ID").alias("PATIENT_ID"),
        F.first("PROVIDER_ID").alias("PROVIDER_ID"),
        F.first("CLAIM_TYPE").alias("CLAIM_TYPE"),
        F.min("CLM_FROM_DT").alias("CLM_FROM_DT"),
        F.max("CLM_THRU_DT").alias("CLM_THRU_DT"),
        F.sum("CLM_PMT_AMT").alias("CLM_PMT_AMT"),
        F.sum("PRIMARY_PYR_PD_AMT").alias("PRIMARY_PYR_PD_AMT"),
        F.first("PRIMARY_DIAGNOSIS_CODE").alias("PRIMARY_DIAGNOSIS_CODE"),
    ))

    # ---- Denial simulation (source has no true denial/CARC field) ----
    # A claim is flagged Denied when CLM_PMT_AMT = 0. CARC assignment uses the
    # weighted distribution below, matching common real-world RCM denial-reason
    # frequency (duplicate / missing info / auth / medical necessity dominate).
    # Replace with your actual 835/EOB remittance CARC codes if/when available.
    carc_weights = {
        "18": 18, "16": 15, "197": 12, "50": 12, "29": 8,
        "27": 8, "96": 8, "119": 6, "109": 6, "1": 4, "2": 3,
    }
    codes, weights = list(carc_weights.keys()), list(carc_weights.values())
    total_w = sum(weights)
    cum = 0.0
    branches = []
    for code, w in zip(codes, weights):
        lo = cum
        cum += w / total_w
        branches.append((lo, cum, code))

    rand_col = F.rand(seed=42)
    carc_expr = F.lit(None).cast("string")
    for lo, hi, code in branches:
        carc_expr = F.when((rand_col >= lo) & (rand_col < hi), F.lit(code)).otherwise(carc_expr)

    as_of = fact_base.select(F.max("CLM_THRU_DT").alias("as_of")).collect()[0]["as_of"]

    fact = (fact_base
        .withColumn("ADJUDICATION_STATUS",
            F.when((F.col("CLM_PMT_AMT").isNull()) | (F.col("CLM_PMT_AMT") == 0), "Denied")
             .otherwise("Paid - First Pass"))
        .withColumn("CARC_CODE", F.when(F.col("ADJUDICATION_STATUS") == "Denied", carc_expr))
        .withColumn("DAYS_SINCE_SUBMISSION", F.datediff(F.lit(as_of), F.col("CLM_FROM_DT")))
        .withColumn("AR_AGING_BUCKET",
            F.when(F.col("DAYS_SINCE_SUBMISSION") <= 30, "0-30")
             .when(F.col("DAYS_SINCE_SUBMISSION") <= 60, "31-60")
             .when(F.col("DAYS_SINCE_SUBMISSION") <= 90, "61-90")
             .otherwise("90+"))
        .withColumnRenamed("PRIMARY_DIAGNOSIS_CODE", "DIAGNOSIS_CODE")
        .withColumnRenamed("CLM_PMT_AMT", "BILLED_PAID_AMT"))

    # ---- Write out star schema as Parquet, partitioned where useful ----
    dim_patient.write.mode("overwrite").parquet(f"{args.output}/Dim_Patient")
    dim_provider.write.mode("overwrite").parquet(f"{args.output}/Dim_Provider")
    dim_diagnosis.write.mode("overwrite").parquet(f"{args.output}/Dim_Diagnosis")
    fact.write.mode("overwrite").partitionBy("CLAIM_TYPE").parquet(f"{args.output}/Fact_Claims_Adjudication")

    print(f"Fact_Claims_Adjudication: {fact.count():,} rows written to {args.output}/Fact_Claims_Adjudication")
    spark.stop()

if __name__ == "__main__":
    main()
