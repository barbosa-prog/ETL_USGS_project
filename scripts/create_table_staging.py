#!/usr/bin/env python3
import psycopg2
import traceback
from dotenv import load_dotenv
import os

# --- CONFIGURATION DE LA CONNEXION ---
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS public.staging_usgs_raw (
  event_id    TEXT        PRIMARY KEY,
  raw_payload JSONB       NOT NULL,
  fetched_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_staging_fetched_at
  ON public.staging_usgs_raw (fetched_at);
"""

load_dotenv()


def recreate_staging():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
        cur = conn.cursor()

        print("✔ Création de la table staging_usgs_raw…")
        cur.execute(CREATE_SQL)

        conn.commit()
        print("✅ Table staging_usgs_raw recréée avec succès.")
    except Exception as e:
        print("❌ Erreur lors de la recréation de la table :")
        traceback.print_exc()
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    recreate_staging()
