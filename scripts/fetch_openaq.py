import os
import json
import requests
import datetime

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)) + "/../"

def fetch_air_quality(**context):
    """
    Appelle l’API OpenAQ v3 pour PM2.5 à New York (2025-06-07)
    Sauvegarde le JSON brut dans data/raw/air_quality/YYYY-MM-DD/measurements.json
    """
    today = datetime.date.today().isoformat()
    raw_dir = os.path.join(PROJECT_ROOT, f"data/raw/air_quality/{today}")
    os.makedirs(raw_dir, exist_ok=True)

    url = "https://api.openaq.org/v3/measurements"
    params = {
        "location": "US Diplomatic Post: New York",
        "parameter": "pm25",
        "date_from": today,
        "date_to": today,
        "limit": 1000
    }
    response = requests.get(url, params=params, timeout=30)
    print(f"Status: {response.status_code}")
    if response.status_code != 200:
        print("Erreur API OpenAQ:", response.text)
        return

    data = response.json()
    if not data.get("results"):
        print("⚠️ Aucune donnée retournée par OpenAQ.")
        return

    out_path = os.path.join(raw_dir, "measurements.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"✅ Données enregistrées dans {out_path}")