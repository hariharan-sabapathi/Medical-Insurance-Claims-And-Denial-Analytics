-- =============================================================================
-- These mirror what would live in a dbt project (models/marts/*.sql).
-- Written as plain SQL views here so they run directly against the DuckDB
-- prototype warehouse; the production dbt translation is in /production/dbt.
-- =============================================================================

-- 1. Clean Claim Rate (overall + by provider)
CREATE OR REPLACE VIEW kpi_clean_claim_rate AS
SELECT
    COUNT(*) FILTER (WHERE ADJUDICATION_STATUS = 'Paid - First Pass') * 100.0 / COUNT(*) AS clean_claim_rate_pct,
    COUNT(*) AS total_claims,
    COUNT(*) FILTER (WHERE ADJUDICATION_STATUS = 'Denied') AS denied_claims
FROM Fact_Claims_Adjudication;

CREATE OR REPLACE VIEW kpi_clean_claim_rate_by_provider AS
SELECT
    PROVIDER_ID,
    COUNT(*) AS total_claims,
    COUNT(*) FILTER (WHERE ADJUDICATION_STATUS = 'Paid - First Pass') * 100.0 / COUNT(*) AS clean_claim_rate_pct
FROM Fact_Claims_Adjudication
GROUP BY PROVIDER_ID
HAVING COUNT(*) >= 5
ORDER BY clean_claim_rate_pct ASC;

-- 2. A/R Aging matrix: Provider x Aging Bucket (outstanding = denied claims)
-- NOTE: a denied claim has BILLED_PAID_AMT = 0 by construction (that's the
-- denial trigger), so "$ at risk" can't be read off that field. We proxy it
-- with the average first-pass-paid amount for the same claim type, i.e.
-- "what this claim would likely have been worth had it paid clean."
CREATE OR REPLACE VIEW kpi_ar_aging_matrix AS
WITH avg_paid AS (
    SELECT CLAIM_TYPE, AVG(BILLED_PAID_AMT) AS avg_paid_amt
    FROM Fact_Claims_Adjudication WHERE ADJUDICATION_STATUS = 'Paid - First Pass'
    GROUP BY CLAIM_TYPE
)
SELECT
    f.PROVIDER_ID,
    f.AR_AGING_BUCKET,
    COUNT(*) AS claim_count,
    ROUND(SUM(a.avg_paid_amt), 2) AS at_risk_amt_proxy
FROM Fact_Claims_Adjudication f
JOIN avg_paid a ON f.CLAIM_TYPE = a.CLAIM_TYPE
WHERE f.ADJUDICATION_STATUS = 'Denied'
GROUP BY f.PROVIDER_ID, f.AR_AGING_BUCKET;

-- 3. Top CARC denial reasons by financial loss
CREATE OR REPLACE VIEW kpi_top_carc_denials AS
SELECT
    d.CARC_CODE,
    c.DESCRIPTION,
    c.PREVENTABILITY_BUCKET,
    COUNT(*) AS denial_count,
    -- financial loss proxy: claims paid $0 when denied, so "loss" = what a
    -- similar first-pass-paid claim of that type would have reimbursed
    ROUND(COUNT(*) * (SELECT AVG(BILLED_PAID_AMT) FROM Fact_Claims_Adjudication WHERE ADJUDICATION_STATUS='Paid - First Pass'), 2) AS estimated_financial_loss
FROM Fact_Claims_Adjudication d
JOIN Dim_CARC_Denials c ON d.CARC_CODE = c.CARC_CODE
WHERE d.ADJUDICATION_STATUS = 'Denied'
GROUP BY d.CARC_CODE, c.DESCRIPTION, c.PREVENTABILITY_BUCKET
ORDER BY estimated_financial_loss DESC;

-- 4. Header cards
-- total_expected_revenue: paid amounts + the avg-paid proxy for denied claims
-- (a stand-in for "billed" since SynPUF only carries the amount Medicare paid)
CREATE OR REPLACE VIEW kpi_header_cards AS
WITH avg_paid AS (
    SELECT CLAIM_TYPE, AVG(BILLED_PAID_AMT) AS avg_paid_amt
    FROM Fact_Claims_Adjudication WHERE ADJUDICATION_STATUS = 'Paid - First Pass'
    GROUP BY CLAIM_TYPE
),
enriched AS (
    SELECT f.*, CASE WHEN f.ADJUDICATION_STATUS='Denied' THEN a.avg_paid_amt ELSE f.BILLED_PAID_AMT END AS expected_amt
    FROM Fact_Claims_Adjudication f JOIN avg_paid a ON f.CLAIM_TYPE = a.CLAIM_TYPE
)
SELECT
    SUM(expected_amt) AS total_expected_revenue,
    SUM(BILLED_PAID_AMT) * 100.0 / NULLIF(SUM(expected_amt),0) AS net_collection_ratio_pct,
    (SELECT clean_claim_rate_pct FROM kpi_clean_claim_rate) AS clean_claim_rate_pct,
    ROUND(SUM(expected_amt) FILTER (WHERE ADJUDICATION_STATUS='Denied' AND AR_AGING_BUCKET='90+'), 2) AS overdue_ar_90plus
FROM enriched;

-- Data integrity checks (dbt "not_null" / "unique" / relationship equivalents)
CREATE OR REPLACE VIEW dq_orphaned_claims AS
SELECT f.CLAIM_ID FROM Fact_Claims_Adjudication f
LEFT JOIN Dim_Patient p ON f.PATIENT_ID = p.PATIENT_ID
WHERE p.PATIENT_ID IS NULL;

CREATE OR REPLACE VIEW dq_duplicate_claim_ids AS
SELECT CLAIM_ID, COUNT(*) c FROM Fact_Claims_Adjudication GROUP BY CLAIM_ID HAVING COUNT(*) > 1;

CREATE OR REPLACE VIEW dq_unmapped_diagnosis AS
SELECT f.CLAIM_ID FROM Fact_Claims_Adjudication f
LEFT JOIN Dim_Diagnosis d ON f.DIAGNOSIS_CODE = d.DIAGNOSIS_CODE
WHERE f.DIAGNOSIS_CODE IS NOT NULL AND d.DIAGNOSIS_CODE IS NULL;
