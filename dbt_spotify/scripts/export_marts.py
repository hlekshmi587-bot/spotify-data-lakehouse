"""Export the star schema and analytics marts from the dbt DuckDB warehouse to Parquet files.

Member 5 (dashboard) can read these directly (Power BI / Tableau / Streamlit / pandas) without
running dbt.  Output: exports/core/*.parquet and exports/analytics/*.parquet

Usage (from the dbt project folder, after `dbt build`):
    python scripts/export_marts.py
"""
from __future__ import annotations

from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "target" / "spotify_warehouse.duckdb"
OUT = ROOT / "exports"


def main() -> None:
    if not DB.exists():
        raise SystemExit("Run `dbt build` first.")
    con = duckdb.connect(str(DB), read_only=True)
    tables = con.execute(
        "select table_schema, table_name from information_schema.tables "
        "where table_schema in ('core', 'analytics') order by 1, 2").fetchall()
    for schema, name in tables:
        target = OUT / schema
        target.mkdir(parents=True, exist_ok=True)
        path = (target / f"{name}.parquet").as_posix()
        con.execute(f"copy (select * from {schema}.{name}) to '{path}' (format parquet, compression zstd)")
        n = con.execute(f"select count(*) from {schema}.{name}").fetchone()[0]
        print(f"{schema}.{name:<28} {n:>8,} rows -> exports/{schema}/{name}.parquet")


if __name__ == "__main__":
    main()
