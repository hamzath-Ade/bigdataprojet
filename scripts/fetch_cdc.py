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


def fetch_cdc_hospitalizations(**context):
    """
    Appelle l'API Socrata CDC pour les données d'asthme
    Sauvegarde le JSON brut dans data/raw/hospitalizations/YYYY/asthma_rate.json

    Améliorations:
    - Gestion d'erreurs robuste
    - Retry automatique
    - Validation des données
    - Headers HTTP appropriés
    - Logging détaillé
    """
    year = datetime.date.today().year
    raw_dir = Path(PROJECT_ROOT) / f"data/raw/hospitalizations/{year}"
    raw_dir.mkdir(parents=True, exist_ok=True)

    # URL et paramètres pour l'API CDC Socrata
    #base_url = "https://data.cdc.gov/resource/swc5-untb.json" #ça aussi fonctionne https://data.cdc.gov/resource/wxz7-ekz9.json
    base_url = "https://data.cdc.gov/resource/wxz7-ekz9.json"

    # Headers pour éviter les blocages
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    # Paramètres de requête plus spécifiques
    params = {
        "$select": "year,location_name,value,unit,data_value_type",
        "$where": f"location_name='New York' AND year >= '{year - 5}'",  # Dernières 5 années
        "$limit": 1000,
        "$order": "year DESC"
    }

    logger.info(f"🔄 Début du téléchargement des données CDC pour l'année {year}")
    logger.info(f"📂 Dossier de destination: {raw_dir}")
    logger.info(f"🌐 URL: {base_url}")
    logger.info(f"📋 Paramètres: {params}")

    max_retries = 3
    retry_delay = 5  # secondes

    for attempt in range(max_retries):
        try:
            logger.info(f"🚀 Tentative {attempt + 1}/{max_retries}")

            # Faire la requête avec timeout plus long
            response = requests.get(
                base_url#,
                #params=params,
                #headers=headers,
                #timeout=60,  # 60 secondes timeout
                #verify=True
            )

            logger.info(f"📊 Status Code: {response.status_code}")
            logger.info(f"📏 Taille de la réponse: {len(response.content)} bytes")

            # Vérifier le statut HTTP
            response.raise_for_status()

            # Vérifier le content-type
            content_type = response.headers.get('content-type', '')
            logger.info(f"📄 Content-Type: {content_type}")

            if 'application/json' not in content_type:
                logger.warning(f"⚠️  Content-Type inattendu: {content_type}")

            # Parser le JSON
            try:
                data = response.json()
            except json.JSONDecodeError as e:
                logger.error(f"❌ Erreur de parsing JSON: {e}")
                logger.error(f"🔍 Début de la réponse: {response.text[:500]}")
                raise

            # Validation des données
            if not data:
                logger.warning("⚠️  Réponse vide de l'API CDC")
                if attempt < max_retries - 1:
                    logger.info(f"⏳ Attente de {retry_delay} secondes avant retry...")
                    time.sleep(retry_delay)
                    continue
                else:
                    raise ValueError("Aucune donnée retournée par l'API CDC après tous les essais")

            if not isinstance(data, list):
                logger.error(f"❌ Format de données inattendu: {type(data)}")
                logger.error(f"🔍 Données reçues: {str(data)[:200]}")
                raise ValueError(f"Format de données inattendu: attendu list, reçu {type(data)}")

            logger.info(f"✅ {len(data)} enregistrements récupérés")

            # Afficher quelques exemples pour debug
            if data:
                logger.info("🔍 Exemple d'enregistrement:")
                logger.info(f"   {data[0]}")

                # Vérifier les champs nécessaires
                required_fields = ['year', 'location_name', 'value']
                sample_record = data[0]
                missing_fields = [field for field in required_fields if field not in sample_record]

                if missing_fields:
                    logger.warning(f"⚠️  Champs manquants dans les données: {missing_fields}")
                    logger.info(f"🔍 Champs disponibles: {list(sample_record.keys())}")

            # Sauvegarder les données
            out_path = raw_dir / "asthma_rate.json"

            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            logger.info(f"💾 Données sauvegardées avec succès dans: {out_path}")
            logger.info(f"📊 Taille du fichier: {out_path.stat().st_size} bytes")

            # Validation du fichier sauvegardé
            if out_path.exists() and out_path.stat().st_size > 0:
                logger.info("✅ Fichier créé et non vide")

                # Test de relecture pour validation
                try:
                    with open(out_path, "r", encoding="utf-8") as f:
                        test_data = json.load(f)
                    logger.info(f"✅ Validation: {len(test_data)} enregistrements dans le fichier")
                except Exception as e:
                    logger.error(f"❌ Erreur lors de la validation du fichier: {e}")
                    raise
            else:
                raise ValueError("Le fichier créé est vide ou n'existe pas")

            # Succès - sortir de la boucle de retry
            return

        except requests.exceptions.Timeout:
            logger.error(f"⏰ Timeout lors de la requête (tentative {attempt + 1})")
            if attempt < max_retries - 1:
                logger.info(f"⏳ Attente de {retry_delay} secondes avant retry...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Backoff exponentiel
            else:
                raise

        except requests.exceptions.ConnectionError as e:
            logger.error(f"🌐 Erreur de connexion: {e}")
            if attempt < max_retries - 1:
                logger.info(f"⏳ Attente de {retry_delay} secondes avant retry...")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                raise

        except requests.exceptions.HTTPError as e:
            logger.error(f"🚫 Erreur HTTP: {e}")
            logger.error(f"🔍 Réponse: {response.text[:500] if 'response' in locals() else 'Pas de réponse'}")

            # Pour certaines erreurs HTTP, pas la peine de retry
            if response.status_code in [400, 401, 403, 404]:
                logger.error("❌ Erreur non récupérable, abandon")
                raise
            elif attempt < max_retries - 1:
                logger.info(f"⏳ Attente de {retry_delay} secondes avant retry...")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                raise

        except Exception as e:
            logger.error(f"❌ Erreur inattendue: {type(e).__name__}: {e}")
            if attempt < max_retries - 1:
                logger.info(f"⏳ Attente de {retry_delay} secondes avant retry...")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                raise


def test_api_connection():
    """
    Fonction de test pour vérifier la connectivité à l'API CDC
    """
    logger.info("🧪 Test de connexion à l'API CDC...")

    url = "https://data.cdc.gov/resource/8cqb-5saa.json"
    params = {"$limit": 1}

    try:
        response = requests.get(url, params=params, timeout=30)
        logger.info(f"📊 Status: {response.status_code}")
        logger.info(f"📄 Content-Type: {response.headers.get('content-type', 'Non spécifié')}")

        if response.status_code == 200:
            data = response.json()
            logger.info(f"✅ API accessible, {len(data)} enregistrement(s) de test récupéré(s)")
            if data:
                logger.info(f"🔍 Exemple: {data[0]}")
        else:
            logger.error(f"❌ API non accessible: {response.status_code}")

    except Exception as e:
        logger.error(f"❌ Erreur de test: {e}")


if __name__ == "__main__":
    # Test en standalone
    logger.info("🚀 Exécution en mode test")
    test_api_connection()
    logger.info("=" * 50)
    fetch_cdc_hospitalizations()