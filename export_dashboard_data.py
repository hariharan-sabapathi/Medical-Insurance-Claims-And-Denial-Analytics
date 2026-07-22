import json
from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent

DATA_DIR = PROJECT_ROOT / "data"
DASHBOARD_DIR = PROJECT_ROOT / "dashboard"

con = duckdb.connect(str(DATA_DIR / "claims_warehouse.duckdb"))
con.execute((PROJECT_ROOT / "kpi_models.sql").read_text())


def q(sql):
    cur = con.execute(sql)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


data = {
    "header_cards": q("SELECT * FROM kpi_header_cards")[0],
    "clean_claim_rate": q("SELECT * FROM kpi_clean_claim_rate")[0],
    "top_carc_denials": q("SELECT * FROM kpi_top_carc_denials LIMIT 10"),
    "ar_aging_matrix": q("SELECT * FROM kpi_ar_aging_matrix"),
    "clean_claim_by_provider_worst10": q("SELECT * FROM kpi_clean_claim_rate_by_provider LIMIT 10"),
    "dq_orphaned_claims": q("SELECT COUNT(*) AS n FROM dq_orphaned_claims")[0]["n"],
    "dq_duplicate_claim_ids": q("SELECT COUNT(*) AS n FROM dq_duplicate_claim_ids")[0]["n"],
    "dq_unmapped_diagnosis": q("SELECT COUNT(*) AS n FROM dq_unmapped_diagnosis")[0]["n"],
}

# Collapse AR matrix to top providers by total exposure for a readable heatmap
providers = {}
for row in data["ar_aging_matrix"]:
    p = row["PROVIDER_ID"]
    providers.setdefault(p, {"0-30": 0, "31-60": 0, "61-90": 0, "90+": 0, "total": 0})
    providers[p][row["AR_AGING_BUCKET"]] = row["at_risk_amt_proxy"] or 0
    providers[p]["total"] += row["at_risk_amt_proxy"] or 0

top_providers = sorted(providers.items(), key=lambda x: -x[1]["total"])[:12]
data["ar_matrix_top_providers"] = [{"provider": p, **v} for p, v in top_providers]

DASHBOARD_DIR.mkdir(exist_ok=True)

with open(DASHBOARD_DIR / "data.json", "w") as f:
    json.dump(data, f, indent=2, default=str)

print(json.dumps(data["header_cards"], indent=2, default=str))
print("Top CARC denials:")
for r in data["top_carc_denials"][:5]:
    print(" ", r["CARC_CODE"], r["DESCRIPTION"], r["denial_count"], r["estimated_financial_loss"])
print(
    "DQ:",
    data["dq_orphaned_claims"],
    data["dq_duplicate_claim_ids"],
    data["dq_unmapped_diagnosis"],
)

con.close()