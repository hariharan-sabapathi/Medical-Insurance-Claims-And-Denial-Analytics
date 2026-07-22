"""
Loads Dim_CARC_Denials from the CARC_Codes.xlsx reference file.

This is a small static lookup (~300 rows), not claims volume, so it's loaded
with pandas/openpyxl (Spark has no native xlsx reader) and written once as
Parquet for the warehouse to join against.

Run: python load_carc_dimension.py --input CARC_Codes.xlsx --output s3://.../warehouse/Dim_CARC_Denials
"""
import argparse
import pandas as pd
import openpyxl  # noqa: F401  (engine used implicitly by pandas)

PREVENTABLE_KEYWORDS = {
    "duplicate": "Preventable - Process",
    "timely filing": "Preventable - Process",
    "prior authorization": "Preventable - Front-End",
    "eligibility": "Preventable - Front-End",
    "non-covered": "Non-Preventable - Coverage",
    "medical necessity": "Non-Preventable - Clinical",
    "coordination of benefits": "Preventable - Process",
    "coinsurance": "Non-Preventable - Patient Responsibility",
    "deductible": "Non-Preventable - Patient Responsibility",
}

def bucket_for(desc: str) -> str:
    if not isinstance(desc, str):
        return "Unclassified"
    d = desc.lower()
    for kw, b in PREVENTABLE_KEYWORDS.items():
        if kw in d:
            return b
    return "Unclassified"

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    args = p.parse_args()

    df = pd.read_excel(args.input, sheet_name="CARC Codes", skiprows=3)
    df.columns = ["CARC_CODE", "DESCRIPTION"]
    df = df.dropna(subset=["CARC_CODE"])
    df["CARC_CODE"] = df["CARC_CODE"].astype(str).str.strip()
    df["PREVENTABILITY_BUCKET"] = df["DESCRIPTION"].apply(bucket_for)
    df.to_parquet(args.output, index=False)
    print(f"Wrote {len(df)} CARC codes to {args.output}")

if __name__ == "__main__":
    main()
