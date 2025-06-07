### 4. Script `scripts/fetch_cdc.py`

import os
import json
import requests
import datetime

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)) + "/../"

def fetch_cdc_hospitalizations(**context):
    """
    Appelle l’API Socrata CDC pour le comté de New York, année en cours
    Sauvegarde le JSON brut dans data/raw/hospitalizations/YYYY/asthma_rate.json
    """
    year = datetime.date.today().year
    raw_dir = os.path.join(PROJECT_ROOT, f"data/raw/hospitalizations/{year}")
    os.makedirs(raw_dir, exist_ok=True)

    url = "https://data.cdc.gov/resource/8cqb-5saa.json"
    params = {
        "$select": "year,location_name,value",
        "location_name": "New York",
        "$limit": 1000
    }
    response = requests.get(url, params=params, timeout=30)
    data = response.json()

    out_path = os.path.join(raw_dir, "asthma_rate.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)