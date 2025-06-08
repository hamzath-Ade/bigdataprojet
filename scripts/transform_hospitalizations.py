import os
import datetime
import logging
import json
from pathlib import Path

from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, isnull, regexp_replace, trim
from pyspark.sql.types import DoubleType, IntegerType

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
#spark.sparkContext._jsc.hadoopConfiguration().set("hadoop.native.io", "false")


PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)) + "/../"
#PROJECT_ROOT = "D:/MonProjetBigData"

def transform_hospitalizations(**context):
    """
    Transforme les données brutes d'hospitalisations CDC au format Parquet

    Améliorations:
    - Gestion d'erreurs robuste
    - Validation des données d'entrée et de sortie
    - Nettoyage et standardisation des données
    - Logging détaillé
    - Gestion des valeurs manquantes
    - Optimisation Spark
    """
    year = datetime.date.today().year
    raw_path = Path(PROJECT_ROOT) / f"data/raw/hospitalizations/{year}/asthma_rate.json"
    formatted_dir = Path(PROJECT_ROOT) / "data/formatted/hospitalizations"
    formatted_dir.mkdir(parents=True, exist_ok=True)

    out_path = formatted_dir / f"asthma_{year}.parquet"

    logger.info(f"🔄 Début de la transformation des données d'hospitalisation pour {year}")
    logger.info(f"📂 Fichier source: {raw_path}")
    logger.info(f"📂 Dossier destination: {formatted_dir}")

    # Validation du fichier d'entrée
    if not raw_path.exists():
        logger.error(f"❌ Fichier source introuvable: {raw_path}")
        raise FileNotFoundError(f"Fichier source introuvable: {raw_path}")

    if raw_path.stat().st_size == 0:
        logger.error(f"❌ Fichier source vide: {raw_path}")
        raise ValueError(f"Fichier source vide: {raw_path}")

    logger.info(f"📏 Taille du fichier source: {raw_path.stat().st_size} bytes")

    # Validation du contenu JSON
    try:
        with open(raw_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        if not raw_data:
            logger.error("❌ Fichier JSON vide ou invalide")
            raise ValueError("Fichier JSON vide ou invalide")

        if not isinstance(raw_data, list):
            logger.error(f"❌ Format JSON inattendu: attendu list, reçu {type(raw_data)}")
            raise ValueError(f"Format JSON inattendu: attendu list, reçu {type(raw_data)}")

        logger.info(f"✅ Fichier JSON valide avec {len(raw_data)} enregistrements")

    except json.JSONDecodeError as e:
        logger.error(f"❌ Erreur de parsing JSON: {e}")
        raise
    except Exception as e:
        logger.error(f"❌ Erreur lors de la validation du fichier: {e}")
        raise

    # Configuration Spark optimisée
    spark_config = {
        "spark.app.name": "TransformHospitalizations",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.repl.eagerEval.enabled": "true",
        "spark.sql.repl.eagerEval.maxNumRows": 20
    }

    spark = None
    try:
        logger.info("🚀 Initialisation de Spark Session...")

        # Construction de la session Spark avec configuration
        builder = SparkSession.builder
        for key, value in spark_config.items():
            builder = builder.config(key, value)

        #spark = builder.getOrCreate()
        #spark.sparkContext.setLogLevel("WARN")  # Réduire les logs Spark
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")  # Réduire les logs Spark
        spark.sparkContext._jsc.hadoopConfiguration().set("hadoop.native.io", "false")

        logger.info(f"✅ Spark Session créée - Version: {spark.version}")

        # Lecture du fichier JSON
        logger.info("📖 Lecture du fichier JSON...")
        df = spark.read.option("multiline", "true").json(str(raw_path))

        initial_count = df.count()
        logger.info(f"📊 Nombre d'enregistrements lus: {initial_count}")

        if initial_count == 0:
            logger.error("❌ Aucun enregistrement trouvé dans le DataFrame")
            raise ValueError("Aucun enregistrement trouvé dans le DataFrame")

        # Affichage du schéma pour debug
        logger.info("🔍 Schéma des données:")
        df.printSchema()

        # Affichage d'exemples d'enregistrements
        logger.info("🔍 Exemples d'enregistrements:")
        df.show(5, truncate=False)

        # Vérification des colonnes nécessaires
        required_columns = ['year', 'percentage']
        available_columns = df.columns
        missing_columns = [col for col in required_columns if col not in available_columns]

        if missing_columns:
            logger.error(f"❌ Colonnes manquantes: {missing_columns}")
            logger.info(f"🔍 Colonnes disponibles: {available_columns}")
            raise ValueError(f"Colonnes manquantes: {missing_columns}")

        logger.info(f"✅ Toutes les colonnes requises sont présentes")

        # Nettoyage et transformation des données
        logger.info("🧹 Nettoyage et transformation des données...")

        # Sélection et renommage des colonnes
        df_clean = df.select(
            col("year").cast(IntegerType()).alias("year"),
            col("confidence_interval").alias("confidence_interval"),
            col("description").alias("description"),
            col("group").alias("group"),
            #col("unit").alias("unit"),
            col("percentage").alias("percentage"),
            col("outcome_or_indicator").alias("outcome_or_indicator")
        )

        # Nettoyage de la colonne value
        df_clean = df_clean.withColumn(
            "asthma_rate",
            when(
                col("percentage").rlike("^[0-9]+\\.?[0-9]*$"),  # Regex pour nombres valides
                col("percentage").cast(DoubleType())
            ).otherwise(None)
        )

        # Nettoyage des chaînes de caractères
        string_columns = ["group", "outcome_or_indicator", "percentage"]
        for column in string_columns:
            if column in df_clean.columns:
                df_clean = df_clean.withColumn(
                    column,
                    trim(regexp_replace(col(column), "[\\r\\n\\t]", " "))
                )

        # Filtrage des enregistrements valides
        df_final = df_clean.filter(
            col("year").isNotNull() &
            col("asthma_rate").isNotNull() &
            (col("year") >= 2000) &  # Années réalistes
            (col("asthma_rate") >= 0)  # Taux positifs
        ).select("year", "percentage", "group", "outcome_or_indicator", "confidence_interval")

        # Vérification après nettoyage
        final_count = df_final.count()
        logger.info(f"📊 Enregistrements après nettoyage: {final_count}")

        if final_count == 0:
            logger.error("❌ Aucun enregistrement valide après nettoyage")
            raise ValueError("Aucun enregistrement valide après nettoyage")

        logger.info(f"📉 Enregistrements supprimés: {initial_count - final_count}")

        # Affichage des statistiques
        logger.info("📈 Statistiques des données nettoyées:")
        df_final.describe().show()

        # Sauvegarde en Parquet
        logger.info(f"💾 Sauvegarde en cours vers: {out_path}")

        df_final.coalesce(1).write.mode("overwrite").parquet(str(out_path))

        logger.info(f"✅ Données sauvegardées avec succès dans: {out_path}")

        # Validation du fichier de sortie
        if out_path.exists():
            # Calcul de la taille du dossier Parquet
            total_size = sum(p.stat().st_size for p in out_path.rglob('*') if p.is_file())
            logger.info(f"📏 Taille du fichier Parquet: {total_size} bytes")

            # Test de relecture pour validation
            logger.info("🔍 Validation du fichier de sortie...")
            df_test = spark.read.parquet(str(out_path))
            test_count = df_test.count()

            if test_count == final_count:
                logger.info(f"✅ Validation réussie: {test_count} enregistrements")

                # Affichage d'un échantillon des données finales
                logger.info("🔍 Échantillon des données finales:")
                df_test.show(5)

            else:
                logger.error(f"❌ Erreur de validation: attendu {final_count}, trouvé {test_count}")
                raise ValueError("Erreur lors de la validation du fichier de sortie")
        else:
            logger.error("❌ Le fichier de sortie n'a pas été créé")
            raise ValueError("Le fichier de sortie n'a pas été créé")

        logger.info("🎉 Transformation terminée avec succès!")

    except Exception as e:
        logger.error(f"❌ Erreur lors de la transformation: {type(e).__name__}: {e}")
        raise

    finally:
        # Nettoyage des ressources Spark
        if spark:
            logger.info("🧹 Fermeture de la session Spark...")
            spark.stop()
            logger.info("✅ Session Spark fermée")


def validate_transformed_data(year=None):
    """
    Fonction de validation pour vérifier les données transformées
    """
    if year is None:
        year = datetime.date.today().year

    formatted_path = Path(PROJECT_ROOT) / "data/formatted/hospitalizations" / f"asthma_{year}.parquet"

    logger.info(f"🔍 Validation des données transformées pour {year}")
    logger.info(f"📂 Fichier: {formatted_path}")

    if not formatted_path.exists():
        logger.error(f"❌ Fichier transformé introuvable: {formatted_path}")
        return False

    try:
        spark = SparkSession.builder.appName("ValidateTransformedData").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        df = spark.read.parquet(str(formatted_path))
        count = df.count()

        logger.info(f"✅ Fichier valide avec {count} enregistrements")

        # Statistiques rapides
        logger.info("📊 Statistiques:")
        df.agg(
            {"percentage": "min", "percentage": "max", "percentage": "avg"}
        ).show()

        spark.stop()
        return True

    except Exception as e:
        logger.error(f"❌ Erreur de validation: {e}")
        return False


if __name__ == "__main__":
    # Test en standalone
    logger.info("🚀 Exécution en mode test")
    logger.info("=" * 50)

    try:
        transform_hospitalizations()
        logger.info("=" * 50)
        validate_transformed_data()

    except Exception as e:
        logger.error(f"❌ Échec du test: {e}")
        exit(1)