
import os
import pandas as pd
import datetime
from pathlib import Path

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)) + "/../"

def transform_airquality(**context):
    """
    - Lit le JSON brut OpenAQ
    - Normalise et garde date.utc + valeur pm25
    - Calcule la moyenne journalière de PM2.5
    - Sauvegarde en Parquet dans data/formatted/air_quality
    """
    today = datetime.date.today().isoformat()
    raw_path = Path(PROJECT_ROOT) / f"data/raw/air_quality/{today}/measurements.json"
    formatted_dir = Path(PROJECT_ROOT) / "data/formatted/air_quality"
    formatted_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_json(raw_path, orient="records")
    if "results" in df.columns:
        df = pd.json_normalize(df["results"])
    df = df[["date.utc", "city", "value"]].rename(columns={"date.utc": "utc_datetime", "value": "pm25"})

    df["utc_datetime"] = pd.to_datetime(df["utc_datetime"])
    df["date"] = df["utc_datetime"].dt.date
    daily_mean = df.groupby("date", as_index=False)["pm25"].mean()

    out_path = formatted_dir / f"air_quality_{today}.parquet"
    daily_mean.to_parquet(out_path, index=False)