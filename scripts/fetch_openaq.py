import os
import json
import requests
import datetime
import logging
import time
from pathlib import Path

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)) + "/../"

# Clé API WAQI
WAQI_API_KEY = "6e208379fb02d29e37c66504b7ec9bb2477b0d92"

def test_connection():
    """Test rapide de l'API WAQI via feed/here"""
    logger.info("🧪 Test de connexion WAQI…")
    url = f"https://api.waqi.info/feed/nyc/?token={WAQI_API_KEY}"
    try:
        resp = requests.get(url, timeout=10)
        logger.info(f"📊 Status: {resp.status_code}")
        data = resp.json()
        if resp.status_code == 200 and data.get("status") == "ok":
            logger.info(f"✅ API accessible — Station: {data['data']['city']['name']}")
        else:
            logger.error(f"❌ Erreur WAQI: {data.get('data')}")
    except Exception as e:
        logger.error(f"❌ Test échoué: {e}")

def fetch_air_quality(**context):
    """
    Appelle l'API WAQI pour les données PM2.5 à la localisation 'here'
    Sauvegarde le JSON brut dans data/raw/air_quality/YYYY-MM-DD/waqi.json
    """
    # Date d'hier pour éviter les données non encore publiées
    target_date = datetime.date.today() - datetime.timedelta(days=1)
    date_iso = target_date.isoformat()

    raw_dir = Path(PROJECT_ROOT) / f"data/raw/air_quality/{date_iso}"
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Construction de l'URL feed/here (ou remplacer 'here' par une ville: e.g. 'beijing')
    url = f"https://api.waqi.info/feed/nyc/?token={WAQI_API_KEY}"

    logger.info(f"🔄 Téléchargement des données WAQI pour {date_iso}")
    logger.info(f"📂 Destination: {raw_dir}")

    max_retries = 3
    for attempt in range(1, max_retries+1):
        try:
            logger.info(f"🚀 Tentative {attempt}/{max_retries}")
            resp = requests.get(url, timeout=30)
            logger.info(f"📊 Status: {resp.status_code}")
            resp.raise_for_status()

            data = resp.json()
            if data.get("status") != "ok":
                raise ValueError(f"WAQI returned status {data.get('status')}")

            # Extraire PM2.5 si présent
            iaqi = data["data"].get("iaqi", {})
            pm25_val = iaqi.get("pm25", {}).get("v")
            if pm25_val is not None:
                logger.info(f"📈 PM2.5: {pm25_val}")
            else:
                logger.warning("⚠️  Pas de PM2.5 dans la réponse")

            # Sauvegarde
            out_path = raw_dir / "waqi.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            logger.info(f"💾 Données sauvegardées: {out_path}")
            return

        except requests.exceptions.Timeout:
            logger.error(f"⏰ Timeout (tentative {attempt})")
        except requests.exceptions.HTTPError as e:
            logger.error(f"🚫 Erreur HTTP: {e}")
            if resp.status_code in (400, 401, 403, 404):
                break
        except Exception as e:
            logger.error(f"❌ Erreur inattendue: {e}")

        if attempt < max_retries:
            delay = 5 * 2**(attempt-1)
            logger.info(f"⏳ Attente {delay}s avant retry…")
            time.sleep(delay)

    raise Exception("Échec après plusieurs tentatives")

if __name__ == "__main__":
    logger.info("🚀 Mode test")
    test_connection()
    logger.info("=" * 40)
    fetch_air_quality()
