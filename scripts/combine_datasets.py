
import os
import pandas as pd
import datetime
from pathlib import Path

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)) + "/../"

def combine_datasets(**context):
    """
    - Lit air_quality_YYYY-MM-DD.parquet et asthma_YYYY.parquet
    - Ajoute colonne year à df_aq, fait un merge inner sur year
    - Sauvegarde en Parquet dans data/final/combined_YYYY-MM-DD.parquet
    """
    today = datetime.date.today().isoformat()
    year = datetime.date.today().year

    aq_path = Path(PROJECT_ROOT) / f"data/formatted/air_quality/air_quality_{today}.parquet"
    hosp_path = Path(PROJECT_ROOT) / f"data/formatted/hospitalizations/asthma_{year}.parquet"
    final_dir = Path(PROJECT_ROOT) / "data/final"
    final_dir.mkdir(parents=True, exist_ok=True)

    df_aq = pd.read_parquet(aq_path)
    df_hosp = pd.read_parquet(hosp_path)

    df_aq["year"] = pd.to_datetime(df_aq["date"]).dt.year
    df_combined = pd.merge(df_aq, df_hosp, on="year", how="inner")

    out_path = final_dir / f"combined_{today}.parquet"
    df_combined.to_parquet(out_path, index=False)