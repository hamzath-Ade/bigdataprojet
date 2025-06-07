
import os
import pandas as pd
import datetime
from pathlib import Path

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)) + "/../"

def transform_hospitalizations(**context):
    """
    - Lit le JSON brut CDC pour l’année en cours
    - Garde colonnes year + value (renommée asthma_rate)
    - Sauvegarde en Parquet dans data/formatted/hospitalizations
    """
    year = datetime.date.today().year
    raw_path = Path(PROJECT_ROOT) / f"data/raw/hospitalizations/{year}/asthma_rate.json"
    formatted_dir = Path(PROJECT_ROOT) / "data/formatted/hospitalizations"
    formatted_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_json(raw_path, orient="records")
    df = df[["year", "value"]].rename(columns={"value": "asthma_rate"})

    out_path = formatted_dir / f"asthma_{year}.parquet"
    df.to_parquet(out_path, index=False)