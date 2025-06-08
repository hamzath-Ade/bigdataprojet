import os
import datetime
import logging
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, regexp_replace, trim,
    to_date, avg as spark_avg, count as spark_count,
    min as spark_min, max as spark_max
)
from pyspark.sql.types import DoubleType

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)) + "/../"


def transform_airquality(**context):
    """
    Transforme les données brutes WAQI (format WAQI JSON) au format Parquet
    """
    # Date d’hier
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    date_str = yesterday.isoformat()

    raw_path = Path(PROJECT_ROOT) / f"data/raw/air_quality/{date_str}/waqi.json"
    formatted_dir = Path(PROJECT_ROOT) / "data/formatted/air_quality"
    formatted_dir.mkdir(parents=True, exist_ok=True)
    out_path = formatted_dir / f"air_quality_{date_str}.parquet"

    logger.info(f"🔄 Début de la transformation WAQI pour {date_str}")
    logger.info(f"📂 Fichier source: {raw_path}")
    logger.info(f"📂 Dossier destination: {formatted_dir}")

    # 1) Validation du JSON WAQI
    if not raw_path.exists():
        raise FileNotFoundError(f"Fichier source introuvable: {raw_path}")
    if raw_path.stat().st_size == 0:
        raise ValueError(f"Fichier source vide: {raw_path}")

    with open(raw_path, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    # WAQI renvoie un dict avec clef "status" et "data"
    if not isinstance(raw_data, dict) or "data" not in raw_data:
        raise ValueError(f"Format JSON inattendu : clef 'data' manquante, on trouve {list(raw_data.keys())}")

    logger.info("✅ JSON WAQI valide")

    # 2) Configuration Spark
    spark = (SparkSession.builder
             .appName("TransformAirQuality")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
             .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext._jsc.hadoopConfiguration().set("hadoop.native.io", "false")
    logger.info(f"✅ Spark Session initialisée (v{spark.version})")

    # 3) Lecture du JSON WAQI
    df = spark.read.option("multiline", "true").json(str(raw_path))
    logger.info(f"📊 Schéma brut WAQI :")
    df.printSchema()

    # 4) Extraction directe des champs WAQI
    df_extracted = df.select(
        col("data.time.s").alias("utc_datetime"),
        col("data.city.name").alias("city"),
        col("data.iaqi.pm25.v").alias("pm25_raw")
    )

    count0 = df_extracted.count()
    logger.info(f"📊 Lignes extraites : {count0}")
    if count0 == 0:
        raise ValueError("❌ Aucun enregistrement WAQI trouvé après extraction")

    # 5) Nettoyage et transformation
    df_clean = df_extracted.withColumn(
        "pm25",
        when(col("pm25_raw").isNotNull(), col("pm25_raw").cast(DoubleType()))
        .otherwise(None)
    )

    # Nettoyage des strings
    for c in ["city", "utc_datetime"]:
        df_clean = df_clean.withColumn(
            c,
            trim(regexp_replace(col(c), "[\\r\\n\\t]", " "))
        )

    # Conversion en date
    df_clean = df_clean.withColumn("date", to_date(col("utc_datetime")))

    # Filtrer les valeurs valides
    df_filtered = df_clean.filter(
        col("utc_datetime").isNotNull() &
        col("pm25").isNotNull() &
        col("date").isNotNull() &
        (col("pm25") >= 0) &
        (col("pm25") <= 1000)
    )

    count1 = df_filtered.count()
    logger.info(f"📊 Après filtrage : {count1} enregistrements")
    if count1 == 0:
        raise ValueError("❌ Aucun enregistrement valide après filtrage")

    # 6) Agrégation journalière
    df_daily = df_filtered.groupBy("date").agg(
        spark_avg("pm25").alias("pm25_avg"),
        spark_count("pm25").alias("count"),
        spark_min("pm25").alias("pm25_min"),
        spark_max("pm25").alias("pm25_max")
    )

    df_final = df_daily.select(
        col("date"),
        col("pm25_avg").cast(DoubleType()).alias("pm25"),
        col("count").alias("measurements_count"),
        col("pm25_min"),
        col("pm25_max")
    )

    # 7) Affichage de quelques stats
    logger.info("🔍 Échantillon des données agrégées :")
    df_final.orderBy("date").show(5)

    # 8) Sauvegarde en Parquet
    logger.info(f"💾 Sauvegarde vers : {out_path}")
    df_final.coalesce(1).write.mode("overwrite").parquet(str(out_path))
    logger.info("✅ Sauvegarde terminée")

    # 9) Validation
    df_check = spark.read.parquet(str(out_path))
    if df_check.count() != df_final.count():
        raise ValueError("❌ Validation échouée : compte mismatch")
    logger.info("✅ Validation réussie")

    spark.stop()


def validate_transformed_airquality_data(date_str=None):
    if date_str is None:
        date_str = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    formatted_path = Path(PROJECT_ROOT) / "data/formatted/air_quality" / f"air_quality_{date_str}.parquet"
    if not formatted_path.exists():
        logger.error(f"❌ Fichier introuvable : {formatted_path}")
        return False
    spark = SparkSession.builder.appName("ValidateAirQualityData").getOrCreate()
    df = spark.read.parquet(str(formatted_path))
    logger.info(f"ℹ️ Jours en base : {df.count()}")
    spark.stop()
    return True


if __name__ == "__main__":
    logger.info("🚀 Mode test - Transform WAQI")
    try:
        transform_airquality()
        validate_transformed_airquality_data()
    except Exception as e:
        logger.error(f"❌ Échec : {e}")
        exit(1)
