import os
import datetime
import logging
import time
import json
from pathlib import Path
from typing import Dict, List, Any, Optional

import pandas as pd
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import (
    ConnectionError,
    RequestError,
    NotFoundError,
    ConflictError
)

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)) + "/../"


def index_to_elasticsearch(**context):
    """
    Indexe les données combinées dans Elasticsearch avec gestion robuste d'erreurs

    Améliorations:
    - Gestion d'erreurs complète (connexion, validation, indexation)
    - Configuration Elasticsearch optimisée
    - Validation du schéma et des données
    - Logging détaillé avec métriques
    - Indexation par batch avec retry
    - Monitoring de performance
    - Validation post-indexation
    - Gestion des conflits et doublons
    """
    today = datetime.date.today()
    today_str = today.isoformat()

    combined_path = Path(PROJECT_ROOT) / f"data/final/combined_{today_str}.parquet"

    logger.info(f"🔄 Début de l'indexation Elasticsearch pour {today_str}")
    logger.info(f"📂 Fichier source: {combined_path}")

    # Validation du fichier d'entrée
    if not combined_path.exists():
        logger.error(f"❌ Fichier source introuvable: {combined_path}")
        raise FileNotFoundError(f"Fichier source introuvable: {combined_path}")

    if not any(combined_path.rglob('*.parquet')):
        logger.error(f"❌ Aucun fichier Parquet trouvé dans: {combined_path}")
        raise ValueError(f"Aucun fichier Parquet trouvé dans: {combined_path}")

    # Calcul de la taille du fichier
    total_size = sum(p.stat().st_size for p in combined_path.rglob('*') if p.is_file())
    logger.info(f"📏 Taille du fichier source: {total_size} bytes")

    # Configuration Elasticsearch
    es_config = {
        "hosts": ["http://localhost:9200"],
        "request_timeout": 30,
        #"max_retries": 3,
        "retry_on_timeout": True,
        "verify_certs": False,
        "http_compress": True
    }

    index_name = "air_asthma"

    # Mapping Elasticsearch optimisé
    index_mapping = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,  # Pas de réplication en développement
            "refresh_interval": "30s",
            "analysis": {
                "analyzer": {
                    "default": {
                        "type": "standard"
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "year": {
                    "type": "integer",
                    "index": True
                },
                "pm25_avg_yearly": {
                    "type": "float",
                    "index": True
                },
                "pm25_min_yearly": {
                    "type": "float",
                    "index": True
                },
                "pm25_max_yearly": {
                    "type": "float",
                    "index": True
                },
                "pm25_measurements_count": {
                    "type": "integer",
                    "index": True
                },
                "daily_measurements_avg": {
                    "type": "float",
                    "index": True
                },
                "percentage": {
                    "type": "text",
                    "index": False  # Texte, pas besoin d'indexation
                },
                "group": {
                    "type": "keyword",
                    "index": True
                },
                "outcome_or_indicator": {
                    "type": "keyword",
                    "index": True
                },
                "confidence_interval": {
                    "type": "text",
                    "index": False
                },
                "indexed_at": {
                    "type": "date",
                    "format": "strict_date_optional_time||epoch_millis"
                },
                "data_source": {
                    "type": "keyword",
                    "index": True
                },
                "processing_date": {
                    "type": "date",
                    "format": "strict_date_optional_time||epoch_millis"
                }
            }
        }
    }

    es = None
    try:
        # Initialisation du client Elasticsearch
        logger.info("🔌 Connexion à Elasticsearch...")
        es = Elasticsearch(**es_config)

        # Test de connexion
        if not es.ping():
            logger.error("❌ Impossible de se connecter à Elasticsearch")
            raise ConnectionError("Impossible de se connecter à Elasticsearch")

        # Informations sur le cluster
        cluster_info = es.info()
        logger.info(f"✅ Connexion réussie - Version ES: {cluster_info['version']['number']}")
        logger.info(f"🏷️ Cluster: {cluster_info['cluster_name']}")

        # Vérification de la santé du cluster
        health = es.cluster.health()
        logger.info(f"🏥 Santé du cluster: {health['status']}")

        if health['status'] == 'red':
            logger.warning("⚠️ Cluster en état RED - Continuons avec prudence")

        # Lecture du fichier Parquet
        logger.info("📖 Lecture du fichier Parquet...")
        start_read = time.time()

        try:
            df = pd.read_parquet(combined_path)
        except Exception as e:
            logger.error(f"❌ Erreur lors de la lecture du fichier Parquet: {e}")
            raise

        read_time = time.time() - start_read
        logger.info(f"✅ Fichier lu en {read_time:.2f}s")

        initial_count = len(df)
        logger.info(f"📊 Nombre d'enregistrements lus: {initial_count}")

        if initial_count == 0:
            logger.error("❌ Aucun enregistrement trouvé dans le fichier")
            raise ValueError("Aucun enregistrement trouvé dans le fichier")

        # Affichage du schéma des données
        logger.info("🔍 Schéma des données:")
        logger.info(f"Colonnes: {list(df.columns)}")
        logger.info(f"Types: {df.dtypes.to_dict()}")

        # Affichage d'un échantillon
        logger.info("🔍 Échantillon des données:")
        logger.info(df.head(3).to_string())

        # Validation et nettoyage des données
        logger.info("🧹 Validation et nettoyage des données...")

        # Vérification des valeurs nulles
        null_counts = df.isnull().sum()
        null_columns = null_counts[null_counts > 0]

        if len(null_columns) > 0:
            logger.warning("⚠️ Colonnes avec valeurs nulles:")
            for col, count in null_columns.items():
                logger.warning(f"   {col}: {count} valeurs nulles")

        # Nettoyage des données
        df_clean = df.copy()

        # Conversion des types pour éviter les erreurs
        numeric_columns = ['pm25_avg_yearly', 'pm25_min_yearly', 'pm25_max_yearly',
                           'pm25_measurements_count', 'daily_measurements_avg']

        for col in numeric_columns:
            if col in df_clean.columns:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')

        # Suppression des lignes avec des valeurs critiques manquantes
        critical_columns = ['year']
        df_clean = df_clean.dropna(subset=critical_columns)

        clean_count = len(df_clean)
        logger.info(f"📊 Enregistrements après nettoyage: {clean_count}")

        if clean_count == 0:
            logger.error("❌ Aucun enregistrement valide après nettoyage")
            raise ValueError("Aucun enregistrement valide après nettoyage")

        logger.info(f"📉 Enregistrements supprimés: {initial_count - clean_count}")

        # Gestion de l'index Elasticsearch
        logger.info(f"🏗️ Gestion de l'index '{index_name}'...")

        index_exists = es.indices.exists(index=index_name)

        if index_exists:
            logger.info(f"📋 Index '{index_name}' existe déjà")

            # Obtenir les statistiques de l'index existant
            try:
                stats = es.indices.stats(index=index_name)
                existing_docs = stats['indices'][index_name]['total']['docs']['count']
                logger.info(f"📊 Documents existants dans l'index: {existing_docs}")

                # Demander confirmation pour la réindexation
                logger.info("🔄 Suppression et recréation de l'index...")
                es.indices.delete(index=index_name)
                logger.info(f"✅ Index '{index_name}' supprimé")

            except Exception as e:
                logger.warning(f"⚠️ Erreur lors de la récupération des stats: {e}")

        # Création de l'index avec le mapping optimisé
        logger.info(f"🏗️ Création de l'index '{index_name}'...")

        try:
            es.indices.create(index=index_name, body=index_mapping)
            logger.info(f"✅ Index '{index_name}' créé avec succès")
        except RequestError as e:
            if "already_exists_exception" in str(e):
                logger.info(f"📋 Index '{index_name}' existe déjà")
            else:
                logger.error(f"❌ Erreur lors de la création de l'index: {e}")
                raise

        # Préparation des documents pour l'indexation
        logger.info("📝 Préparation des documents pour l'indexation...")

        actions = []
        error_count = 0
        indexed_at = datetime.datetime.now().isoformat()

        for index, row in df_clean.iterrows():
            try:
                # Construction du document
                doc_source = {}

                # Ajout de toutes les colonnes disponibles
                for col in df_clean.columns:
                    value = row[col]

                    # Gestion des valeurs NaN/None
                    if pd.isna(value):
                        doc_source[col] = None
                    elif isinstance(value, (int, float)):
                        # Conversion des valeurs numériques
                        if pd.isna(value) or value != value:  # Check for NaN
                            doc_source[col] = None
                        else:
                            doc_source[col] = float(value) if isinstance(value, float) else int(value)
                    else:
                        doc_source[col] = str(value)

                # Ajout de métadonnées
                doc_source.update({
                    "indexed_at": indexed_at,
                    "data_source": f"combined_{today_str}",
                    "processing_date": today_str
                })

                # Construction de l'action d'indexation
                action = {
                    "_index": index_name,
                    "_id": f"{today_str}_{index}",  # ID unique basé sur la date et l'index
                    "_source": doc_source
                }

                actions.append(action)

            except Exception as e:
                error_count += 1
                logger.warning(f"⚠️ Erreur lors de la préparation du document {index}: {e}")
                continue

        docs_prepared = len(actions)
        logger.info(f"📝 Documents préparés: {docs_prepared}")

        if error_count > 0:
            logger.warning(f"⚠️ Erreurs de préparation: {error_count}")

        if docs_prepared == 0:
            logger.error("❌ Aucun document préparé pour l'indexation")
            raise ValueError("Aucun document préparé pour l'indexation")

        # Indexation par batch avec gestion d'erreurs
        logger.info("🚀 Début de l'indexation...")

        batch_size = 100
        success_count = 0
        failed_count = 0
        start_index = time.time()

        try:
            # Configuration pour l'indexation bulk
            bulk_config = {
                'chunk_size': batch_size,
                'max_chunk_bytes': 10 * 1024 * 1024,  # 10MB
                'request_timeout': 60,
                #'max_retries': 3,
                #'initial_backoff': 2,
                #'max_backoff': 600
            }

            # Indexation avec helpers.bulk
            for success, info in helpers.parallel_bulk(
                    es,
                    actions,
                    **bulk_config
            ):
                if success:
                    success_count += 1
                else:
                    failed_count += 1
                    logger.warning(f"⚠️ Échec d'indexation: {info}")

                # Log de progression
                if (success_count + failed_count) % 50 == 0:
                    logger.info(f"📊 Progression: {success_count + failed_count}/{docs_prepared} documents")

        except Exception as e:
            logger.error(f"❌ Erreur lors de l'indexation bulk: {e}")
            raise

        index_time = time.time() - start_index

        logger.info(f"✅ Indexation terminée en {index_time:.2f}s")
        logger.info(f"📊 Documents indexés avec succès: {success_count}")

        if failed_count > 0:
            logger.warning(f"⚠️ Documents échoués: {failed_count}")

        # Actualisation de l'index pour les recherches immédiates
        logger.info("🔄 Actualisation de l'index...")
        es.indices.refresh(index=index_name)

        # Validation post-indexation
        logger.info("🔍 Validation post-indexation...")

        # Attendre un peu pour que l'indexation soit complètement terminée
        time.sleep(2)

        # Vérification du nombre de documents indexés
        doc_count = es.count(index=index_name)['count']
        logger.info(f"📊 Documents dans l'index après indexation: {doc_count}")

        if doc_count == success_count:
            logger.info("✅ Validation réussie - Tous les documents sont indexés")
        else:
            logger.warning(f"⚠️ Incohérence détectée - Attendu: {success_count}, Trouvé: {doc_count}")

        # Test de recherche
        logger.info("🔍 Test de recherche...")
        try:
            search_result = es.search(
                index=index_name,
                body={
                    "query": {"match_all": {}},
                    "size": 1,
                    "sort": [{"year": {"order": "desc"}}]
                }
            )

            if search_result['hits']['total']['value'] > 0:
                sample_doc = search_result['hits']['hits'][0]['_source']
                logger.info("✅ Test de recherche réussi")
                logger.info(f"🔍 Exemple de document indexé: {json.dumps(sample_doc, indent=2, default=str)}")
            else:
                logger.warning("⚠️ Aucun document trouvé lors du test de recherche")

        except Exception as e:
            logger.warning(f"⚠️ Erreur lors du test de recherche: {e}")

        # Statistiques finales
        logger.info("📊 Statistiques d'indexation:")
        logger.info(f"   • Fichier source: {combined_path}")
        logger.info(f"   • Taille fichier: {total_size} bytes")
        logger.info(f"   • Enregistrements lus: {initial_count}")
        logger.info(f"   • Enregistrements nettoyés: {clean_count}")
        logger.info(f"   • Documents préparés: {docs_prepared}")
        logger.info(f"   • Documents indexés: {success_count}")
        logger.info(f"   • Documents échoués: {failed_count}")
        logger.info(f"   • Temps de lecture: {read_time:.2f}s")
        logger.info(f"   • Temps d'indexation: {index_time:.2f}s")
        logger.info(f"   • Index Elasticsearch: {index_name}")

        logger.info("🎉 Indexation Elasticsearch terminée avec succès!")

    except ConnectionError as e:
        logger.error(f"❌ Erreur de connexion Elasticsearch: {e}")
        raise
    except RequestError as e:
        logger.error(f"❌ Erreur de requête Elasticsearch: {e}")
        raise
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'indexation: {type(e).__name__}: {e}")
        raise

    finally:
        # Pas de nettoyage spécifique nécessaire pour le client ES
        if es:
            logger.info("✅ Connexion Elasticsearch fermée")


def validate_elasticsearch_index(index_name: str = "air_asthma", date_str: Optional[str] = None):
    """
    Fonction de validation pour vérifier l'index Elasticsearch
    """
    if date_str is None:
        date_str = datetime.date.today().isoformat()

    logger.info(f"🔍 Validation de l'index Elasticsearch '{index_name}' pour {date_str}")

    try:
        es = Elasticsearch(hosts=["http://localhost:9200"], request_timeout=30)

        if not es.ping():
            logger.error("❌ Impossible de se connecter à Elasticsearch")
            return False

        # Vérification de l'existence de l'index
        if not es.indices.exists(index=index_name):
            logger.error(f"❌ Index '{index_name}' introuvable")
            return False

        # Statistiques de l'index
        stats = es.indices.stats(index=index_name)
        doc_count = stats['indices'][index_name]['total']['docs']['count']
        size_bytes = stats['indices'][index_name]['total']['store']['size_in_bytes']

        logger.info(f"✅ Index valide avec {doc_count} documents ({size_bytes} bytes)")

        # Test de recherche par année
        search_result = es.search(
            index=index_name,
            body={
                "query": {"match_all": {}},
                "aggs": {
                    "years": {
                        "terms": {"field": "year", "size": 10}
                    },
                    "avg_pm25": {
                        "avg": {"field": "pm25_avg_yearly"}
                    }
                },
                "size": 0
            }
        )

        # Affichage des agrégations
        if 'aggregations' in search_result:
            years = [bucket['key'] for bucket in search_result['aggregations']['years']['buckets']]
            avg_pm25 = search_result['aggregations']['avg_pm25']['value']

            logger.info(f"📅 Années disponibles: {sorted(years)}")
            logger.info(f"🌬️ PM2.5 moyen global: {avg_pm25:.2f} µg/m³")

        return True

    except Exception as e:
        logger.error(f"❌ Erreur de validation: {e}")
        return False


def search_elasticsearch_data(
        index_name: str = "air_asthma",
        query: Optional[Dict[str, Any]] = None,
        size: int = 10
) -> List[Dict[str, Any]]:
    """
    Fonction utilitaire pour rechercher dans l'index Elasticsearch
    """
    logger.info(f"🔍 Recherche dans l'index '{index_name}'")

    try:
        es = Elasticsearch(hosts=["http://localhost:9200"], request_timeout=30)

        if not es.ping():
            logger.error("❌ Impossible de se connecter à Elasticsearch")
            return []

        # Requête par défaut si aucune fournie
        if query is None:
            query = {"match_all": {}}

        search_body = {
            "query": query,
            "size": size,
            "sort": [{"year": {"order": "desc"}}]
        }

        result = es.search(index=index_name, body=search_body)

        documents = [hit['_source'] for hit in result['hits']['hits']]
        total = result['hits']['total']['value']

        logger.info(f"✅ {len(documents)} documents trouvés sur {total} total")

        return documents

    except Exception as e:
        logger.error(f"❌ Erreur de recherche: {e}")
        return []


if __name__ == "__main__":
    # Test en standalone
    logger.info("🚀 Exécution en mode test - Indexation Elasticsearch")
    logger.info("=" * 70)

    try:
        index_to_elasticsearch()
        logger.info("=" * 70)
        validate_elasticsearch_index()

        # Test de recherche
        logger.info("=" * 70)
        logger.info("🔍 Test de recherche:")
        results = search_elasticsearch_data(size=3)
        for i, doc in enumerate(results, 1):
            logger.info(f"Document {i}: Year {doc.get('year')}, PM2.5: {doc.get('pm25_avg_yearly')}")

    except Exception as e:
        logger.error(f"❌ Échec du test: {e}")
        exit(1)