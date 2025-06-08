import os
import datetime
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    year as spark_year, col, when, isnan, isnull,
    count as spark_count, avg as spark_avg,
    min as spark_min, max as spark_max,
    sum as spark_sum, round as spark_round
)
from pyspark.sql.types import IntegerType

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)) + "/../"


def combine_datasets(**context):
    """
    Combine les données de qualité de l'air et d'hospitalisation pour asthme

    Améliorations:
    - Gestion d'erreurs robuste
    - Validation des fichiers d'entrée et de sortie
    - Logging détaillé avec métriques
    - Optimisation Spark
    - Gestion des valeurs manquantes
    - Calcul de statistiques de corrélation
    - Validation de la cohérence des données
    """
    today = datetime.date.today()
    today_str = today.isoformat()
    year = today.year

    # Calcul de la date d'hier pour les données de qualité de l'air
    yesterday = today - datetime.timedelta(days=1)
    yesterday_str = yesterday.isoformat()

    # Chemins des fichiers
    aq_path = Path(PROJECT_ROOT) / f"data/formatted/air_quality/air_quality_{yesterday_str}.parquet"
    hosp_path = Path(PROJECT_ROOT) / f"data/formatted/hospitalizations/asthma_{year}.parquet"
    final_dir = Path(PROJECT_ROOT) / "data/final"
    final_dir.mkdir(parents=True, exist_ok=True)

    out_path = final_dir / f"combined_{today_str}.parquet"

    logger.info(f"🔄 Début de la combinaison des datasets pour {today_str}")
    logger.info(f"📂 Qualité de l'air: {aq_path}")
    logger.info(f"📂 Hospitalisations: {hosp_path}")
    logger.info(f"📂 Sortie finale: {out_path}")

    # Validation des fichiers d'entrée
    missing_files = []
    file_sizes = {}

    if not aq_path.exists():
        missing_files.append(f"Qualité de l'air: {aq_path}")
    else:
        file_sizes['air_quality'] = sum(p.stat().st_size for p in aq_path.rglob('*') if p.is_file())
        logger.info(f"📏 Taille fichier qualité de l'air: {file_sizes['air_quality']} bytes")

    if not hosp_path.exists():
        missing_files.append(f"Hospitalisations: {hosp_path}")
    else:
        file_sizes['hospitalizations'] = sum(p.stat().st_size for p in hosp_path.rglob('*') if p.is_file())
        logger.info(f"📏 Taille fichier hospitalisations: {file_sizes['hospitalizations']} bytes")

    if missing_files:
        logger.error(f"❌ Fichiers source manquants:")
        for file in missing_files:
            logger.error(f"   - {file}")
        raise FileNotFoundError(f"Fichiers source manquants: {missing_files}")

    # Vérification des tailles de fichier
    for name, size in file_sizes.items():
        if size == 0:
            logger.error(f"❌ Fichier {name} vide")
            raise ValueError(f"Fichier {name} vide")

    logger.info("✅ Tous les fichiers source sont présents et non vides")

    # Configuration Spark optimisée
    spark_config = {
        "spark.app.name": "CombineDatasets",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.repl.eagerEval.enabled": "true",
        "spark.sql.repl.eagerEval.maxNumRows": 20,
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.adaptive.skewJoin.enabled": "true"
    }

    spark = None
    try:
        logger.info("🚀 Initialisation de Spark Session...")

        # Construction de la session Spark avec configuration
        builder = SparkSession.builder
        for key, value in spark_config.items():
            builder = builder.config(key, value)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")  # Réduire les logs Spark
        spark.sparkContext._jsc.hadoopConfiguration().set("hadoop.native.io", "false")

        logger.info(f"✅ Spark Session créée - Version: {spark.version}")

        # Lecture des données de qualité de l'air
        logger.info("📖 Lecture des données de qualité de l'air...")
        df_aq = spark.read.parquet(str(aq_path))

        aq_count = df_aq.count()
        logger.info(f"📊 Enregistrements qualité de l'air: {aq_count}")

        if aq_count == 0:
            logger.error("❌ Aucun enregistrement dans les données de qualité de l'air")
            raise ValueError("Aucun enregistrement dans les données de qualité de l'air")

        # Affichage du schéma et échantillon des données AQ
        logger.info("🔍 Schéma des données de qualité de l'air:")
        df_aq.printSchema()

        logger.info("🔍 Échantillon des données de qualité de l'air:")
        df_aq.show(5, truncate=False)

        # Lecture des données d'hospitalisation
        logger.info("📖 Lecture des données d'hospitalisation...")
        df_hosp = spark.read.parquet(str(hosp_path))

        hosp_count = df_hosp.count()
        logger.info(f"📊 Enregistrements hospitalisation: {hosp_count}")

        if hosp_count == 0:
            logger.error("❌ Aucun enregistrement dans les données d'hospitalisation")
            raise ValueError("Aucun enregistrement dans les données d'hospitalisation")

        # Affichage du schéma et échantillon des données hospitalisation
        logger.info("🔍 Schéma des données d'hospitalisation:")
        df_hosp.printSchema()

        logger.info("🔍 Échantillon des données d'hospitalisation:")
        df_hosp.show(5, truncate=False)

        # Préparation des données de qualité de l'air
        logger.info("🔄 Préparation des données de qualité de l'air...")

        # Ajout de la colonne année et nettoyage
        df_aq_prepared = df_aq.withColumn("year", spark_year(col("date")).cast(IntegerType()))

        # Vérification des années présentes
        years_aq = df_aq_prepared.select("year").distinct().collect()
        years_aq_list = [row['year'] for row in years_aq if row['year'] is not None]
        logger.info(f"📅 Années présentes dans les données AQ: {sorted(years_aq_list)}")

        # Filtrage des données valides
        df_aq_clean = df_aq_prepared.filter(
            col("year").isNotNull() &
            col("pm25").isNotNull() &
            (col("pm25") >= 0)
        )

        aq_clean_count = df_aq_clean.count()
        logger.info(f"📊 Enregistrements AQ après nettoyage: {aq_clean_count}")

        if aq_clean_count == 0:
            logger.error("❌ Aucun enregistrement valide après nettoyage des données AQ")
            raise ValueError("Aucun enregistrement valide après nettoyage des données AQ")

        # Préparation des données d'hospitalisation
        logger.info("🔄 Préparation des données d'hospitalisation...")

        # Vérification des colonnes nécessaires
        hosp_columns = df_hosp.columns
        logger.info(f"🔍 Colonnes hospitalisation disponibles: {hosp_columns}")

        required_hosp_columns = ['year']
        missing_hosp_columns = [col for col in required_hosp_columns if col not in hosp_columns]

        if missing_hosp_columns:
            logger.error(f"❌ Colonnes manquantes dans les données hospitalisation: {missing_hosp_columns}")
            raise ValueError(f"Colonnes manquantes dans les données hospitalisation: {missing_hosp_columns}")

        # Nettoyage des données d'hospitalisation
        df_hosp_clean = df_hosp.filter(
            col("year").isNotNull() &
            (col("year") >= 2000) &  # Années réalistes
            (col("year") <= datetime.date.today().year + 1)
        )

        hosp_clean_count = df_hosp_clean.count()
        logger.info(f"📊 Enregistrements hospitalisation après nettoyage: {hosp_clean_count}")

        if hosp_clean_count == 0:
            logger.error("❌ Aucun enregistrement valide après nettoyage des données hospitalisation")
            raise ValueError("Aucun enregistrement valide après nettoyage des données hospitalisation")

        # Vérification des années communes
        years_hosp = df_hosp_clean.select("year").distinct().collect()
        years_hosp_list = [row['year'] for row in years_hosp if row['year'] is not None]
        logger.info(f"📅 Années présentes dans les données hospitalisation: {sorted(years_hosp_list)}")

        common_years = list(set(years_aq_list) & set(years_hosp_list))
        logger.info(f"📅 Années communes: {sorted(common_years)}")

        if not common_years:
            logger.error("❌ Aucune année commune entre les deux datasets")
            raise ValueError("Aucune année commune entre les deux datasets")

        # Agrégation des données de qualité de l'air par année
        logger.info("📊 Agrégation des données de qualité de l'air par année...")

        df_aq_agg = df_aq_clean.groupBy("year").agg(
            spark_avg("pm25").alias("pm25_avg_yearly"),
            spark_min("pm25").alias("pm25_min_yearly"),
            spark_max("pm25").alias("pm25_max_yearly"),
            spark_count("pm25").alias("pm25_measurements_count"),
            spark_avg("measurements_count").alias("daily_measurements_avg")
        ).withColumn("pm25_avg_yearly", spark_round(col("pm25_avg_yearly"), 2))

        agg_count = df_aq_agg.count()
        logger.info(f"📊 Années avec données AQ agrégées: {agg_count}")

        logger.info("🔍 Aperçu des données AQ agrégées:")
        df_aq_agg.orderBy("year").show(10)

        # Jointure des datasets
        logger.info("🔗 Jointure des datasets...")

        # Jointure interne sur l'année
        df_combined = df_aq_agg.join(df_hosp_clean, on="year", how="inner")

        combined_count = df_combined.count()
        logger.info(f"📊 Enregistrements après jointure: {combined_count}")

        if combined_count == 0:
            logger.error("❌ Aucun enregistrement après jointure")
            raise ValueError("Aucun enregistrement après jointure")

        # Sélection et réorganisation des colonnes finales
        final_columns = [
            "year",
            "pm25_avg_yearly",
            "pm25_min_yearly",
            "pm25_max_yearly",
            "pm25_measurements_count",
            "daily_measurements_avg"
        ]

        # Ajout des colonnes d'hospitalisation disponibles
        hosp_specific_cols = [col for col in df_hosp_clean.columns if col != "year"]
        final_columns.extend(hosp_specific_cols)

        df_final = df_combined.select(*final_columns)

        logger.info("🔍 Schéma des données combinées finales:")
        df_final.printSchema()

        # Statistiques des données combinées
        logger.info("📈 Statistiques des données combinées:")
        df_final.describe().show()

        # Affichage d'un échantillon des données finales
        logger.info("🔍 Échantillon des données finales:")
        df_final.orderBy("year").show(10, truncate=False)

        # Calculs de corrélation si possible
        numeric_columns = [field.name for field in df_final.schema.fields
                           if field.dataType.simpleString() in ['double', 'float', 'int', 'bigint']]

        if len(numeric_columns) >= 2:
            logger.info("📊 Analyse de corrélation:")
            try:
                # Tentative de calcul de corrélation entre PM2.5 et données hospitalisation
                pm25_col = "pm25_avg_yearly"

                for col_name in numeric_columns:
                    if col_name != pm25_col and col_name != "year":
                        try:
                            correlation = df_final.stat.corr(pm25_col, col_name)
                            logger.info(f"   Corrélation {pm25_col} vs {col_name}: {correlation:.4f}")
                        except Exception as e:
                            logger.warning(f"   Impossible de calculer la corrélation pour {col_name}: {e}")
            except Exception as e:
                logger.warning(f"Erreur lors du calcul de corrélation: {e}")

        # Sauvegarde des données combinées
        logger.info(f"💾 Sauvegarde en cours vers: {out_path}")

        df_final.coalesce(1).write.mode("overwrite").parquet(str(out_path))

        logger.info(f"✅ Données combinées sauvegardées avec succès dans: {out_path}")

        # Validation du fichier de sortie
        if out_path.exists():
            # Calcul de la taille du dossier Parquet
            total_size = sum(p.stat().st_size for p in out_path.rglob('*') if p.is_file())
            logger.info(f"📏 Taille du fichier final: {total_size} bytes")

            # Test de relecture pour validation
            logger.info("🔍 Validation du fichier de sortie...")
            df_test = spark.read.parquet(str(out_path))
            test_count = df_test.count()

            if test_count == combined_count:
                logger.info(f"✅ Validation réussie: {test_count} enregistrements")

                # Résumé final
                years_range = df_test.agg(
                    spark_min("year").alias("min_year"),
                    spark_max("year").alias("max_year")
                ).collect()[0]

                logger.info(f"📅 Période couverte: {years_range['min_year']} - {years_range['max_year']}")

                # Statistiques finales PM2.5
                pm25_stats = df_test.agg(
                    spark_avg("pm25_avg_yearly").alias("overall_avg"),
                    spark_min("pm25_avg_yearly").alias("overall_min"),
                    spark_max("pm25_avg_yearly").alias("overall_max")
                ).collect()[0]

                logger.info(f"🌬️ PM2.5 moyen sur la période: {pm25_stats['overall_avg']:.2f} µg/m³")
                logger.info(
                    f"🌬️ PM2.5 min/max: {pm25_stats['overall_min']:.2f} - {pm25_stats['overall_max']:.2f} µg/m³")

            else:
                logger.error(f"❌ Erreur de validation: attendu {combined_count}, trouvé {test_count}")
                raise ValueError("Erreur lors de la validation du fichier de sortie")
        else:
            logger.error("❌ Le fichier de sortie n'a pas été créé")
            raise ValueError("Le fichier de sortie n'a pas été créé")

        logger.info("🎉 Combinaison des datasets terminée avec succès!")

        # Métriques de résumé
        logger.info("=" * 60)
        logger.info("📊 RÉSUMÉ DE LA COMBINAISON")
        logger.info("=" * 60)
        logger.info(f"📅 Date de traitement: {today_str}")
        logger.info(f"📊 Enregistrements AQ initiaux: {aq_count}")
        logger.info(f"📊 Enregistrements hospitalisation initiaux: {hosp_count}")
        logger.info(f"📊 Enregistrements finaux combinés: {combined_count}")
        logger.info(f"📅 Années traitées: {len(common_years)}")
        logger.info(f"💾 Fichier de sortie: {out_path}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"❌ Erreur lors de la combinaison: {type(e).__name__}: {e}")
        raise

    finally:
        # Nettoyage des ressources Spark
        if spark:
            logger.info("🧹 Fermeture de la session Spark...")
            spark.stop()
            logger.info("✅ Session Spark fermée")


def validate_combined_data(date_str=None):
    """
    Fonction de validation pour vérifier les données combinées
    """
    if date_str is None:
        date_str = datetime.date.today().isoformat()

    combined_path = Path(PROJECT_ROOT) / "data/final" / f"combined_{date_str}.parquet"

    logger.info(f"🔍 Validation des données combinées pour {date_str}")
    logger.info(f"📂 Fichier: {combined_path}")

    if not combined_path.exists():
        logger.error(f"❌ Fichier combiné introuvable: {combined_path}")
        return False

    try:
        spark = SparkSession.builder.appName("ValidateCombinedData").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        df = spark.read.parquet(str(combined_path))
        count = df.count()

        logger.info(f"✅ Fichier valide avec {count} enregistrements")

        # Vérification de l'intégrité des données
        null_counts = {}
        for column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            null_counts[column] = null_count
            if null_count > 0:
                logger.warning(f"⚠️ Colonne '{column}': {null_count} valeurs nulles")

        # Statistiques rapides
        logger.info("📊 Statistiques des données combinées:")
        df.describe().show()

        # Vérification des années
        year_range = df.agg(
            spark_min("year").alias("min_year"),
            spark_max("year").alias("max_year"),
            spark_count("year").alias("year_count")
        ).collect()[0]

        logger.info(
            f"📅 Années: {year_range['min_year']} - {year_range['max_year']} ({year_range['year_count']} années)")

        spark.stop()
        return True

    except Exception as e:
        logger.error(f"❌ Erreur de validation: {e}")
        return False


if __name__ == "__main__":
    # Test en standalone
    logger.info("🚀 Exécution en mode test - Combinaison des Datasets")
    logger.info("=" * 70)

    try:
        combine_datasets()
        logger.info("=" * 70)
        validate_combined_data()

    except Exception as e:
        logger.error(f"❌ Échec du test: {e}")
        exit(1)