-- Raw tables
CREATE TABLE IF NOT EXISTS raw_doctors_data (
    source_system TEXT,
    source_id TEXT,
    name TEXT,
    city TEXT,
    specialty TEXT,
    region TEXT,
    last_updated TEXT,
    normalized_name TEXT,
    normalized_city TEXT,
    quality_score REAL
);

CREATE TABLE IF NOT EXISTS raw_sales_data (
    sale_id INTEGER,
    sale_date TEXT,
    doctor_name TEXT,
    city TEXT,
    product TEXT,
    units INTEGER,
    unit_price REAL,
    target_units INTEGER,
    normalized_name TEXT,
    normalized_city TEXT,
    revenue REAL,
    quality_score REAL
);

-- MDM: golden doctor record (CRM has higher priority than IQVIA)
DROP TABLE IF EXISTS dim_doctor;
CREATE TABLE dim_doctor AS
WITH ranked_candidates AS (
    SELECT
        source_system,
        source_id,
        name,
        city,
        specialty,
        region,
        last_updated,
        normalized_name,
        normalized_city,
        CASE WHEN source_system = 'CRM' THEN 1 ELSE 2 END AS source_priority,
        ROW_NUMBER() OVER (
            PARTITION BY normalized_name, normalized_city
            ORDER BY CASE WHEN source_system = 'CRM' THEN 1 ELSE 2 END, last_updated DESC
        ) AS row_choice,
        DENSE_RANK() OVER (
            ORDER BY normalized_name, normalized_city
        ) AS doctor_rank
    FROM raw_doctors_data
)
SELECT
    'DOC-' || printf('%04d', doctor_rank) AS doctor_id,
    source_system AS master_source,
    source_id AS master_source_id,
    name,
    city,
    specialty,
    region,
    normalized_name,
    normalized_city,
    last_updated
FROM ranked_candidates
WHERE row_choice = 1;

-- Fact table
DROP TABLE IF EXISTS fact_sales;
CREATE TABLE fact_sales AS
SELECT
    s.sale_id,
    s.sale_date,
    d.doctor_id,
    d.name AS doctor_name,
    d.city,
    d.region,
    s.product,
    s.units,
    s.target_units,
    s.unit_price,
    s.revenue,
    CASE
        WHEN s.target_units = 0 THEN 0
        ELSE ROUND((CAST(s.units AS REAL) / s.target_units) * 100, 2)
    END AS attainment_pct,
    CASE
        WHEN s.target_units = 0 THEN 0
        WHEN (CAST(s.units AS REAL) / s.target_units) >= 1.2 THEN 12
        WHEN (CAST(s.units AS REAL) / s.target_units) >= 1.0 THEN 8
        WHEN (CAST(s.units AS REAL) / s.target_units) >= 0.9 THEN 4
        ELSE 0
    END AS bonus_pct
FROM raw_sales_data s
JOIN dim_doctor d
  ON s.normalized_name = d.normalized_name
 AND s.normalized_city = d.normalized_city;
