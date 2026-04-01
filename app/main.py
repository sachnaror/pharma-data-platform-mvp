from __future__ import annotations

from pathlib import Path
from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.db import get_conn

app = FastAPI(title="Pharma Data Platform MVP", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).resolve().parents[1]
STATIC_DIR = BASE_DIR / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
def home() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "pharma-data-platform-mvp"}


@app.get("/doctors")
def get_doctors(segment: Optional[str] = None, region: Optional[str] = None) -> list[dict]:
    where = []
    params: list = []

    if region:
        where.append("region = ?")
        params.append(region)

    base_query = """
    WITH doctor_metrics AS (
        SELECT
            d.doctor_id,
            d.name,
            d.city,
            d.region,
            d.specialty,
            d.master_source,
            COALESCE(SUM(f.units), 0) AS total_units,
            COALESCE(SUM(f.revenue), 0) AS total_revenue
        FROM dim_doctor d
        LEFT JOIN fact_sales f ON f.doctor_id = d.doctor_id
        GROUP BY d.doctor_id, d.name, d.city, d.region, d.specialty, d.master_source
    )
    SELECT
        doctor_id,
        name,
        city,
        region,
        specialty,
        master_source,
        total_units,
        total_revenue,
        CASE
            WHEN total_units >= 220 THEN 'High'
            WHEN total_units >= 170 THEN 'Medium'
            ELSE 'Low'
        END AS segment
    FROM doctor_metrics
    """

    if where:
        base_query += " WHERE " + " AND ".join(where)

    if segment:
        prefix = " WHERE " if not where else " AND "
        base_query += prefix + "(CASE WHEN total_units >= 220 THEN 'High' WHEN total_units >= 170 THEN 'Medium' ELSE 'Low' END) = ?"
        params.append(segment)

    base_query += " ORDER BY total_units DESC"

    with get_conn() as conn:
        rows = conn.execute(base_query, params).fetchall()
    return [dict(row) for row in rows]


@app.get("/sales")
def get_sales(region: Optional[str] = None, product: Optional[str] = None) -> list[dict]:
    where = []
    params: list = []

    if region:
        where.append("region = ?")
        params.append(region)
    if product:
        where.append("product = ?")
        params.append(product)

    query = """
    SELECT
        sale_date,
        region,
        product,
        SUM(units) AS total_units,
        ROUND(SUM(revenue), 2) AS total_revenue,
        ROUND(AVG(attainment_pct), 2) AS avg_attainment_pct,
        ROUND(AVG(bonus_pct), 2) AS avg_bonus_pct
    FROM fact_sales
    """
    if where:
        query += " WHERE " + " AND ".join(where)

    query += " GROUP BY sale_date, region, product ORDER BY sale_date"

    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


@app.get("/call-plan")
def get_call_plan(
    region: Optional[str] = None,
    product: Optional[str] = None,
    limit: int = 5,
) -> list[dict]:
    where = []
    params: list = []

    if region:
        where.append("region = ?")
        params.append(region)
    if product:
        where.append("product = ?")
        params.append(product)

    query = """
    SELECT
        doctor_id,
        doctor_name,
        city,
        region,
        product,
        SUM(units) AS rx_volume,
        ROUND(SUM(revenue), 2) AS revenue,
        ROUND(AVG(attainment_pct), 2) AS avg_attainment_pct,
        CASE
            WHEN SUM(units) >= 220 THEN 'Priority A'
            WHEN SUM(units) >= 170 THEN 'Priority B'
            ELSE 'Priority C'
        END AS call_priority
    FROM fact_sales
    """
    if where:
        query += " WHERE " + " AND ".join(where)

    query += " GROUP BY doctor_id, doctor_name, city, region, product"
    query += " ORDER BY rx_volume DESC LIMIT ?"
    params.append(limit)

    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


@app.get("/incentives")
def get_incentives(region: Optional[str] = None) -> list[dict]:
    where = []
    params: list = []

    if region:
        where.append("region = ?")
        params.append(region)

    query = """
    SELECT
        doctor_id,
        doctor_name,
        region,
        ROUND(SUM(units), 2) AS actual_units,
        ROUND(SUM(target_units), 2) AS target_units,
        ROUND((SUM(units) * 100.0) / NULLIF(SUM(target_units), 0), 2) AS attainment_pct,
        ROUND(AVG(bonus_pct), 2) AS recommended_bonus_pct
    FROM fact_sales
    """
    if where:
        query += " WHERE " + " AND ".join(where)

    query += " GROUP BY doctor_id, doctor_name, region ORDER BY recommended_bonus_pct DESC, attainment_pct DESC"

    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]
