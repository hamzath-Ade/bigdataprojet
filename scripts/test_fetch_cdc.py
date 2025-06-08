import os
import shutil
import datetime

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)) + "/../"

def fetch_cdc_hospitalizations():
    """
    Copie un fichier CSV local (fourni manuellement) dans le dossier raw du projet.
    """
    year = datetime.date.today().year
    raw_dir = os.path.join(PROJECT_ROOT, f"data/raw/hospitalizations/{year}")
    os.makedirs(raw_dir, exist_ok=True)

    # Chemin complet vers le fichier CSV source que tu as téléchargé toi-même
    local_csv_path = os.path.join(PROJECT_ROOT, "NHIS_Child_Summary_Health_Statistics.csv")  # à adapter si nécessaire

    if not os.path.exists(local_csv_path):
        print(f"❌ Fichier introuvable : {local_csv_path}")
        return

    out_path = os.path.join(raw_dir, "asthma_data.csv")
    shutil.copy(local_csv_path, out_path)

    print(f"✅ Fichier CSV local copié dans {out_path}")
