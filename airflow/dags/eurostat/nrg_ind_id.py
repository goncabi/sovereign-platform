import csv
from datetime import datetime, timezone
from pathlib import Path

import requests
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DATASET = "nrg_ind_id"
URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/"
    f"{DATASET}?format=SDMX-CSV"
)
TMP_DIR = Path("/tmp/eurostat")
TABLE = f"raw.eurostat_{DATASET}"


@dag(
    dag_id="eurostat_nrg_ind_id",
    schedule=None,
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["eurostat", "raw"],
)
def eurostat_nrg_ind_id():

    @task
    def extract() -> str:
        """Downloads the complete dataset and saves it as a temporary CSV."""
        TMP_DIR.mkdir(parents=True, exist_ok=True)
        response = requests.get(URL, timeout=120)
        response.raise_for_status()
        path = TMP_DIR / f"{DATASET}.csv"
        path.write_text(response.text, encoding="utf-8")
        return str(path)

    @task
    def load(path: str) -> int:
        """Complete load on the raw layer: deletes and recreates the table."""
        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            rows = list(reader)

        columns = [h.strip().lower().replace(" ", "_") for h in header]
        columns_sql = ", ".join(f'"{c}" TEXT' for c in columns)

        hook = PostgresHook(postgres_conn_id="warehouse_db")
        hook.run([
            "CREATE SCHEMA IF NOT EXISTS raw;",
            f"DROP TABLE IF EXISTS {TABLE};",
            f"CREATE TABLE {TABLE} ({columns_sql}, "
            "_loaded_at TIMESTAMPTZ DEFAULT now());",
        ])
        hook.insert_rows(
            table=TABLE,
            rows=rows,
            target_fields=[f'"{c}"' for c in columns],
        )
        return len(rows)

    load(extract())


eurostat_nrg_ind_id()