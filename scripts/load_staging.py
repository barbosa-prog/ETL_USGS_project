#!/usr/bin/env python3
import os
import json
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Charger les variables d'environnement depuis .env si présent
load_dotenv()

# Chemin vers le JSON extrait
DATA_FILE = os.path.join(os.path.dirname(__file__), "../data/raw_usgs_month.json")

# Paramètres de connexion PostgreSQL depuis les variables d'environnement
DB_CONN_PARAMS = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5433)),
    "dbname": os.getenv("DB_NAME", "earthquakes"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}


def load_staging(conn, features):
    rows = [(f["id"], json.dumps(f, ensure_ascii=False)) for f in features]
    print("DEBUG: Prepared", len(rows), "rows for staging insert.")
    sql = """
INSERT INTO public.staging_usgs_raw (event_id, raw_payload)
VALUES %s
ON CONFLICT (event_id) DO UPDATE
  SET raw_payload = EXCLUDED.raw_payload,
      fetched_at  = now()
"""
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=100)
        print("DEBUG: execute_values succeeded.")
    conn.commit()
    print("DEBUG: commit done.")


def main():
    print("load_staging.py starting...")
    if not os.path.exists(DATA_FILE):
        print("ERROR: JSON file not found at", DATA_FILE)
        return

    try:
        with open(DATA_FILE, encoding="utf-8", errors="replace") as f:
            raw = f.read().replace("\ufffd", " ")
            data = json.loads(raw)
    except Exception as e:
        print("ERROR: Could not load JSON:", e)
        return

    features = data.get("features", [])
    print("Found", len(features), "events to load into staging_usgs_raw")

    try:
        conn = psycopg2.connect(**DB_CONN_PARAMS)
        print("DEBUG: connected with parameters", conn.get_dsn_parameters())
    except Exception as e:
        print("ERROR: Database connection failed:", e)
        return

    try:
        load_staging(conn, features)
        print("load_staging.py completed successfully.")
    except Exception as e:
        print("ERROR: Exception during staging load:", e)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
