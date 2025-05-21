#!/usr/bin/env python3
import os
import psycopg2
import traceback
from dotenv import load_dotenv

# Charger les variables d'environnement depuis .env si présent
load_dotenv()

# Paramètres de connexion PostgreSQL depuis les variables d'environnement
DB_PARAMS = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5433)),
    "dbname": os.getenv("DB_NAME", "earthquakes"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# Chemin vers le SQL de transformation
SQL_FILE = os.path.join(os.path.dirname(__file__), "../sql/transform.sql")


def main():
    print("transform.py starting...")
    try:
        # Charger tout le contenu du fichier SQL
        with open(SQL_FILE, "r", encoding="utf-8") as f:
            content = f.read()
        # Séparer les commandes par point-virgule
        commands = [cmd.strip() for cmd in content.split(";") if cmd.strip()]

        # Connexion
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()

        # Exécution de chaque commande
        for cmd in commands:
            # affiche la première ligne de la commande
            first_line = cmd.splitlines()[0] if cmd.splitlines() else ""
            print("Running SQL command:", first_line)
            cur.execute(cmd)

        conn.commit()
        cur.close()
        conn.close()
        print("transform.py completed successfully.")
    except Exception:
        print("Error during transformation:")
        traceback.print_exc()


if __name__ == "__main__":
    main()
