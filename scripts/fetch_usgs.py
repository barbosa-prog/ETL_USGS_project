#!/usr/bin/env python3
import requests
import json
import os

# URL de l’API USGS
USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"

# Répertoire & fichier de sortie
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "../data")
OUTPUT_FILE = os.path.join(DATA_DIR, "raw_usgs_month.json")


def fetch_and_save():
    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"Requesting data from {USGS_URL} …")
    resp = requests.get(USGS_URL, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    count = len(data.get("features", []))
    print(f"Retrieved {count} events.")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    fetch_and_save()
