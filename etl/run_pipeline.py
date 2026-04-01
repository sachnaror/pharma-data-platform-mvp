from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data" / "raw"
SQL_FILE = BASE_DIR / "etl" / "sql" / "models.sql"
LOG_FILE = BASE_DIR / "logs" / "etl_runs.csv"


@dataclass
class PipelineArtifacts:
    doctors_df: pd.DataFrame
    sales_df: pd.DataFrame


def normalize_text(value: str) -> str:
    if pd.isna(value):
        return ""
    return " ".join(str(value).strip().lower().split())


def quality_score(df: pd.DataFrame, required_cols: list[str]) -> float:
    total_cells = len(df) * len(required_cols)
    if total_cells == 0:
        return 0.0
    non_empty = sum(df[col].fillna("").astype(str).str.strip().ne("").sum() for col in required_cols)
    return round((non_empty / total_cells) * 100, 2)


def extract_csv() -> PipelineArtifacts:
    crm = pd.read_csv(DATA_DIR / "doctors_crm.csv")
    iqvia = pd.read_csv(DATA_DIR / "doctors_iqvia.csv")
    sales = pd.read_csv(DATA_DIR / "sales.csv")

    crm["source_system"] = "CRM"
    iqvia["source_system"] = "IQVIA"

    doctors_df = pd.concat([crm, iqvia], ignore_index=True)
    return PipelineArtifacts(doctors_df=doctors_df, sales_df=sales)


def clean_transform(artifacts: PipelineArtifacts) -> PipelineArtifacts:
    doctors_df = artifacts.doctors_df.copy()
    sales_df = artifacts.sales_df.copy()

    doctors_df["normalized_name"] = doctors_df["name"].apply(normalize_text)
    doctors_df["normalized_city"] = doctors_df["city"].apply(normalize_text)
    doctors_df["quality_score"] = quality_score(
        doctors_df,
        ["source_id", "name", "city", "specialty", "region", "last_updated"],
    )

    sales_df["normalized_name"] = sales_df["doctor_name"].apply(normalize_text)
    sales_df["normalized_city"] = sales_df["city"].apply(normalize_text)
    sales_df["revenue"] = sales_df["units"] * sales_df["unit_price"]
    sales_df["quality_score"] = quality_score(
        sales_df,
        ["sale_id", "sale_date", "doctor_name", "city", "product", "units", "unit_price", "target_units"],
    )

    return PipelineArtifacts(doctors_df=doctors_df, sales_df=sales_df)


def get_sqlite_path() -> Path:
    db_name = os.getenv("LOCAL_DB_PATH", "warehouse.db")
    return BASE_DIR / db_name


def load_to_local_sqlite(artifacts: PipelineArtifacts) -> None:
    db_path = get_sqlite_path()
    with sqlite3.connect(db_path) as conn:
        artifacts.doctors_df.to_sql("raw_doctors_data", conn, if_exists="replace", index=False)
        artifacts.sales_df.to_sql("raw_sales_data", conn, if_exists="replace", index=False)

        sql = SQL_FILE.read_text(encoding="utf-8")
        conn.executescript(sql)
        conn.commit()


def snowflake_connection_params() -> dict[str, str]:
    return {
        "account": os.getenv("SNOWFLAKE_ACCOUNT", ""),
        "user": os.getenv("SNOWFLAKE_USER", ""),
        "password": os.getenv("SNOWFLAKE_PASSWORD", ""),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", ""),
        "database": os.getenv("SNOWFLAKE_DATABASE", "PHARMA_MVP"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        "role": os.getenv("SNOWFLAKE_ROLE", ""),
    }


def load_to_snowflake(artifacts: PipelineArtifacts) -> None:
    import snowflake.connector

    params = snowflake_connection_params()
    missing = [k for k, v in params.items() if not v and k not in {"role"}]
    if missing:
        raise ValueError(f"Missing Snowflake config: {', '.join(missing)}")

    conn = snowflake.connector.connect(**params)
    try:
        cs = conn.cursor()
        cs.execute(f"CREATE DATABASE IF NOT EXISTS {params['database']}")
        cs.execute(f"USE DATABASE {params['database']}")
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {params['schema']}")
        cs.execute(f"USE SCHEMA {params['schema']}")

        cs.execute(
            """
            CREATE OR REPLACE TABLE raw_doctors_data (
                source_system STRING,
                source_id STRING,
                name STRING,
                city STRING,
                specialty STRING,
                region STRING,
                last_updated STRING,
                normalized_name STRING,
                normalized_city STRING,
                quality_score FLOAT
            )
            """
        )

        cs.execute(
            """
            CREATE OR REPLACE TABLE raw_sales_data (
                sale_id NUMBER,
                sale_date STRING,
                doctor_name STRING,
                city STRING,
                product STRING,
                units NUMBER,
                unit_price FLOAT,
                target_units NUMBER,
                normalized_name STRING,
                normalized_city STRING,
                revenue FLOAT,
                quality_score FLOAT
            )
            """
        )

        doctor_rows = [
            tuple(row)
            for row in artifacts.doctors_df[
                [
                    "source_system",
                    "source_id",
                    "name",
                    "city",
                    "specialty",
                    "region",
                    "last_updated",
                    "normalized_name",
                    "normalized_city",
                    "quality_score",
                ]
            ].itertuples(index=False, name=None)
        ]

        sales_rows = [
            tuple(row)
            for row in artifacts.sales_df[
                [
                    "sale_id",
                    "sale_date",
                    "doctor_name",
                    "city",
                    "product",
                    "units",
                    "unit_price",
                    "target_units",
                    "normalized_name",
                    "normalized_city",
                    "revenue",
                    "quality_score",
                ]
            ].itertuples(index=False, name=None)
        ]

        cs.executemany(
            """
            INSERT INTO raw_doctors_data (
                source_system, source_id, name, city, specialty, region,
                last_updated, normalized_name, normalized_city, quality_score
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            doctor_rows,
        )

        cs.executemany(
            """
            INSERT INTO raw_sales_data (
                sale_id, sale_date, doctor_name, city, product, units, unit_price,
                target_units, normalized_name, normalized_city, revenue, quality_score
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            sales_rows,
        )

        cs.execute(
            """
            CREATE OR REPLACE TABLE dim_doctor AS
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
                    DENSE_RANK() OVER (ORDER BY normalized_name, normalized_city) AS doctor_rank
                FROM raw_doctors_data
            )
            SELECT
                'DOC-' || LPAD(doctor_rank::string, 4, '0') AS doctor_id,
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
            WHERE row_choice = 1
            """
        )

        cs.execute(
            """
            CREATE OR REPLACE TABLE fact_sales AS
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
                IFF(s.target_units = 0, 0, ROUND((s.units / s.target_units) * 100, 2)) AS attainment_pct,
                CASE
                    WHEN s.target_units = 0 THEN 0
                    WHEN (s.units / s.target_units) >= 1.2 THEN 12
                    WHEN (s.units / s.target_units) >= 1.0 THEN 8
                    WHEN (s.units / s.target_units) >= 0.9 THEN 4
                    ELSE 0
                END AS bonus_pct
            FROM raw_sales_data s
            JOIN dim_doctor d
              ON s.normalized_name = d.normalized_name
             AND s.normalized_city = d.normalized_city
            """
        )

    finally:
        conn.close()


def log_run(status: str, details: str, mode: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    row = pd.DataFrame(
        [
            {
                "run_at_utc": now,
                "status": status,
                "mode": mode,
                "details": details,
            }
        ]
    )

    if LOG_FILE.exists():
        existing = pd.read_csv(LOG_FILE)
        row = pd.concat([existing, row], ignore_index=True)

    row.to_csv(LOG_FILE, index=False)


def main() -> None:
    mode = os.getenv("WAREHOUSE_MODE", "local").strip().lower()
    try:
        artifacts = extract_csv()
        transformed = clean_transform(artifacts)

        if mode == "snowflake":
            load_to_snowflake(transformed)
            log_run("success", "Loaded CRM + IQVIA + sales into Snowflake", mode)
        else:
            load_to_local_sqlite(transformed)
            log_run("success", "Loaded CRM + IQVIA + sales into local SQLite warehouse", mode)

        print(f"ETL finished successfully in '{mode}' mode")

    except Exception as exc:
        log_run("failure", str(exc), mode)
        raise


if __name__ == "__main__":
    main()
