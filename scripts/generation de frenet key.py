
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())


###ancierasthmaairquality
#---
# -*- coding: utf-8 -*-
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import os
import json
import requests
import pandas as pd

# --- Variables globales ---

# Répertoire du projet (adapte si besoin)
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)) + "/../"

# Fonctions auxiliaires (on remplira ensuite)
def fetch_air_quality(**context):
    """
    1) Appel à l’API OpenAQ pour NYC PM2.5 du jour
    2) Sauvegarde du JSON brut dans data/raw/air_quality/YYYY-MM-DD/measurements.json
    """
    import datetime

    # 1. Préparer la date
    today = datetime.date.today().isoformat()  # ex. "2024-10-05"
    raw_dir = os.path.join(PROJECT_ROOT, "data/raw/air_quality", today)
    os.makedirs(raw_dir, exist_ok=True)

    # 2. Requête OpenAQ
    url = f"https://api.openaq.org/v2/measurements"
    params = {
        "city": "New York",
        "parameter": "pm25",
        "date_from": today + "T00:00:00Z",
        "date_to": today + "T23:59:59Z",
        "limit": 10000
    }
    response = requests.get(url, params=params, timeout=30)
    data = response.json()

    # 3. Sauvegarder JSON brut
    with open(os.path.join(raw_dir, "measurements.json"), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def fetch_cdc_hospitalizations(**context):
    """
    1) Appel à l’API Socrata CDC pour le comté de New York, année en cours
    2) Sauvegarde du JSON brut dans data/raw/hospitalizations/YYYY/asthma_rate.json
    """
    import datetime

    # 1. Préparer l’année en cours
    year = datetime.date.today().year
    raw_dir = os.path.join(PROJECT_ROOT, f"data/raw/hospitalizations/{year}")
    os.makedirs(raw_dir, exist_ok=True)

    # 2. Requête CDC Socrata
    # Filtre sur le comté de New York (location_name = "New York")
    url = "https://data.cdc.gov/resource/8cqb-5saa.json"
    params = {
        "$select": "year,location_name,value",
        "location_name": "New York",
        "$limit": 1000
    }
    response = requests.get(url, params=params, timeout=30)
    data = response.json()

    # 3. Sauvegarder JSON brut
    with open(os.path.join(raw_dir, "asthma_rate.json"), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def transform_airquality(**context):
    """
    1) Lecture du JSON brut OpenAQ
    2) Normalisation / nettoyage (conserver timestamp + valeur PM2.5)
    3) Calculer la moyenne journalière de PM2.5
    4) Stocker au format Parquet dans data/formatted/air_quality/air_quality_YYYY-MM-DD.parquet
    """
    import datetime
    import pyarrow.parquet as pq
    from pathlib import Path

    today = datetime.date.today().isoformat()         # ex. "2024-10-05"
    raw_path = Path(PROJECT_ROOT) / f"data/raw/air_quality/{today}/measurements.json"
    formatted_dir = Path(PROJECT_ROOT) / "data/formatted/air_quality"
    formatted_dir.mkdir(parents=True, exist_ok=True)

    # 1. Charger le JSON
    df = pd.read_json(raw_path, orient="records")

    # 2. Extraire uniquement l’essentiel
    #    On suppose que la structure d’OpenAQ a un champ "results" qui est la liste
    if "results" in df.columns:
        df = pd.json_normalize(df["results"])
    # Colonnes importantes : date.utc, city, parameter, value
    df = df[["date.utc", "city", "value"]].rename(columns={"date.utc": "utc_datetime", "value": "pm25"})

    # 3. Convertir utc_datetime en datetime et calculer la moyenne journalière
    df["utc_datetime"] = pd.to_datetime(df["utc_datetime"])
    df["date"] = df["utc_datetime"].dt.date
    daily_mean = df.groupby("date", as_index=False)["pm25"].mean()

    # 4. Sauvegarder en Parquet
    out_path = formatted_dir / f"air_quality_{today}.parquet"
    daily_mean.to_parquet(out_path, index=False)

def transform_hospitalizations(**context):
    """
    1) Lecture du JSON brut CDC pour l’année en cours
    2) Normalisation (conserver year + valeur taux d’asthme)
    3) Sauvegarder en Parquet dans data/formatted/hospitalizations/asthma_YYYY.parquet
    """
    import datetime
    from pathlib import Path

    year = datetime.date.today().year
    raw_path = Path(PROJECT_ROOT) / f"data/raw/hospitalizations/{year}/asthma_rate.json"
    formatted_dir = Path(PROJECT_ROOT) / "data/formatted/hospitalizations"
    formatted_dir.mkdir(parents=True, exist_ok=True)

    # 1. Charger JSON
    df = pd.read_json(raw_path, orient="records")

    # 2. Normaliser : colonnes essentielles = year (int), value (float)
    df = df[["year", "value"]].rename(columns={"value": "asthma_rate"})

    # 3. Sauvegarder en Parquet
    out_path = formatted_dir / f"asthma_{year}.parquet"
    df.to_parquet(out_path, index=False)

def combine_datasets(**context):
    """
    1) Charger air_quality_{today}.parquet et asthma_{year}.parquet
    2) Filtrer l’année pour laquelle on a les deux (ici, on rapproche date.year = year CDC)
    3) Pour chaque date, joindre la moyenne PM2.5 du jour avec le taux d’asthme de l’année
    4) Sauvegarder le résultat dans data/final/combined_YYYY-MM-DD.parquet
    """
    import datetime
    from pathlib import Path

    today = datetime.date.today().isoformat()   # ex. "2024-10-05"
    year = datetime.date.today().year

    # 1. Chemins
    aq_path = Path(PROJECT_ROOT) / f"data/formatted/air_quality/air_quality_{today}.parquet"
    hosp_path = Path(PROJECT_ROOT) / f"data/formatted/hospitalizations/asthma_{year}.parquet"
    final_dir = Path(PROJECT_ROOT) / "data/final"
    final_dir.mkdir(parents=True, exist_ok=True)

    # 2. Charger
    df_aq = pd.read_parquet(aq_path)         # colonnes = ["date", "pm25"]
    df_hosp = pd.read_parquet(hosp_path)     # colonnes = ["year", "asthma_rate"]

    # 3. Ajouter la colonne 'year' à df_aq pour join
    df_aq["year"] = pd.to_datetime(df_aq["date"]).dt.year

    # 4. Jointure : inner join sur 'year'
    df_combined = pd.merge(df_aq, df_hosp, on="year", how="inner")

    # 5. Sauvegarder résultat
    out_path = final_dir / f"combined_{today}.parquet"
    df_combined.to_parquet(out_path, index=False)

def index_to_elasticsearch(**context):
    """
    1) Charger combined_{today}.parquet
    2) Convertir en JSON ligne par ligne
    3) Indexer chaque document dans Elasticsearch (index nommé "air_asthma")
    """
    from elasticsearch import Elasticsearch, helpers
    import datetime
    from pathlib import Path

    # Config ES (localhost:9200 par défaut)
    es = Elasticsearch(hosts=["http://localhost:9200"])

    today = datetime.date.today().isoformat()
    combined_path = Path(PROJECT_ROOT) / f"data/final/combined_{today}.parquet"
    df = pd.read_parquet(combined_path)

    # 1. Préparer les documents pour bulk
    actions = []
    for _, row in df.iterrows():
        doc = {
            "_index": "air_asthma",
            "_source": {
                "date": row["date"].isoformat(),
                "pm25": float(row["pm25"]),
                "asthma_rate": float(row["asthma_rate"]),
                "year": int(row["year"])
            }
        }
        actions.append(doc)

    # 2. Créer l’index s’il n’existe pas
    if not es.indices.exists(index="air_asthma"):
        es.indices.create(index="air_asthma", body={
            "mappings": {
                "properties": {
                    "date": {"type": "date"},
                    "pm25": {"type": "float"},
                    "asthma_rate": {"type": "float"},
                    "year": {"type": "integer"}
                }
            }
        })

    # 3. Bulk indexing
    helpers.bulk(es, actions)

# --- Définition du DAG ---

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1
}

with DAG(
    dag_id="asthma_airquality_pipeline",
    default_args=default_args,
    schedule_interval="@daily",  # Exécution quotidienne
    catchup=False
) as dag:

    t_fetch_air = PythonOperator(
        task_id="fetch_air_quality",
        python_callable=fetch_air_quality
    )

    t_fetch_cdc = PythonOperator(
        task_id="fetch_cdc_hospitalizations",
        python_callable=fetch_cdc_hospitalizations
    )

    t_transform_aq = PythonOperator(
        task_id="transform_airquality",
        python_callable=transform_airquality
    )

    t_transform_hosp = PythonOperator(
        task_id="transform_hospitalizations",
        python_callable=transform_hospitalizations
    )

    t_combine = PythonOperator(
        task_id="combine_datasets",
        python_callable=combine_datasets
    )

    t_index = PythonOperator(
        task_id="index_to_elasticsearch",
        python_callable=index_to_elasticsearch
    )

    # → Ordonnancement des tâches
    # 1) On ingère d’abord les deux sources en parallèle
    # 2) Puis on transforme chacune
    # 3) Puis on combine
    # 4) Puis on indexe
    [t_fetch_air, t_fetch_cdc] >> [t_transform_aq, t_transform_hosp] >> t_combine >> t_index

