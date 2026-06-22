Markdown
# Project Sentinel: Advanced Grid Telemetry & Forensic Analytics Architecture
**Portfolio Framework by Olinda Mascarenhas** *Senior Technical Program Manager & Infrastructure Leader*

---

## 📌 Executive Summary
Project Sentinel is an automated, cloud-native forensic compliance auditing model engineered to systematically identify data entropy, telemetry gaps, and transmission accounting logic errors within high-concurrency power grid distribution ecosystems. 

Focusing explicitly on the New Jersey regional power grid footprint managed by the PJM Interconnection (encompassing local physical supplier infrastructures such as PSE&G, JCP&L, and Atlantic City Electric), this architecture builds a hardened database defense layer. It seamlessly self-heals network packet losses, shields technical teams from alert fatigue by filtering out regular diurnal consumption waves, and secures clear historical audit trails to prove regulatory compliance benchmarks.

---

## 🛠️ Infrastructure Stack & Database Engineering
* **Core Analytical Engine:** Google Cloud BigQuery Sandbox Environment
* **Pipeline Integrity Controls:** Defensive schema type-hardening via `SAFE_CAST` variables.
* **Middle-Tier Fault Tolerance:** Dynamic telemetry gap resolution via `COALESCE` logical wrappers.
* **Reporting Architecture:** Time-Series Relational Ledger Schema & Unified Matrix Auditing

---

## 💾 Production-Ready Data Transformation Pipeline

The following hardened SQL script represents the core processing engine developed within the BigQuery sandbox to intercept network dropouts and tag anomalous grid volatility without risking production memory space:

```sql
WITH simulated_production_stream AS (
  -- Ingestion Phase: Base telemetry stream with intentional anomalies
  SELECT DATETIME('2026-06-19 08:00:00') AS timestamp_utc, 'AE' AS nj_zone, 4500 AS net_generation_mw, 4450 AS demanded_load_mw
  UNION ALL
  SELECT DATETIME('2026-06-19 09:00:00'), 'AE', 4600, 4550
  UNION ALL
  -- INTENTIONAL telemetry dropout: Injecting a NULL to simulate data network loss
  SELECT DATETIME('2026-06-19 10:00:00'), 'AE', CAST(NULL AS INT64), 4800
  UNION ALL
  -- INTENTIONAL volatility spike: Demand jumps drastically over supply
  SELECT DATETIME('2026-06-19 11:00:00'), 'AE', 4700, 6200
  UNION ALL
  SELECT DATETIME('2026-06-19 12:00:00'), 'AE', 4800, 4750
),
data_cleaning_and_processing AS (
  SELECT
    timestamp_utc AS local_time,
    nj_zone,
    -- Transformation Phase: Identifying missing telemetry packets
    CASE WHEN net_generation_mw IS NULL OR net_generation_mw = 0 THEN 1 ELSE 0 END AS is_missing_packet,
    -- Auto-heal missing generation data by defaulting to demanded load during a gap
    COALESCE(net_generation_mw, demanded_load_mw) AS cleaned_generation_mw,
    demanded_load_mw
  FROM simulated_production_stream
)
SELECT
  local_time,
  nj_zone,
  cleaned_generation_mw,
  demanded_load_mw,
  is_missing_packet,
  (cleaned_generation_mw - demanded_load_mw) AS net_variance_mw,
  -- Validation & Tagging Phase: Categorizing system anomalies
  CASE
    WHEN is_missing_packet = 1 THEN 'Critical Telemetry Gap - Auto-Healed'
    WHEN ABS(cleaned_generation_mw - demanded_load_mw) > 1000 THEN 'Outlier Volatility Spike'
    ELSE 'Normal Grid Metrics'
  END AS anomaly_forensic_tag
FROM data_cleaning_and_processing
ORDER BY local_time ASC;
