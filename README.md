### 9. README.md (Exemple minimal)
```markdown
# Projet Big Data Santé – Pollution & Asthme

## Architecture
```
/ton_projet_bigdata/
│
├── docker-compose.yml
├── dags/
│   └── asthma_airquality_dag.py
├── scripts/
│   ├── fetch_openaq.py
│   ├── fetch_cdc.py
│   ├── transform_airquality.py
│   ├── transform_hospitalizations.py
│   ├── combine_datasets.py
│   └── index_to_es.py
├── data/
│   ├── raw/
│   ├── formatted/
│   └── final/
├── logs/
└── plugins/
```

## Prérequis
- Docker & Docker Compose installé
- Clé Fernet générée pour Airflow (REMPLACE `YOUR_FERNET_KEY` dans `docker-compose.yml`)

## Lancement
1. `docker-compose up -d`
   - Déploie : Postgres, Airflow (init, scheduler, webserver), Elasticsearch, Kibana.
2. Vérifie Airflow UI → http://localhost:8080 (login Admin / admin)
3. Dashboard Kibana → http://localhost:5601 → crée index pattern `air_asthma*`.

## Structure Data Lake
- `data/raw/air_quality/YYYY-MM-DD/measurements.json`
- `data/raw/hospitalizations/YYYY/asthma_rate.json`
- `data/formatted/air_quality/air_quality_YYYY-MM-DD.parquet`
- `data/formatted/hospitalizations/asthma_YYYY.parquet`
- `data/final/combined_YYYY-MM-DD.parquet`

## Execution
- Le DAG `asthma_airquality_pipeline` s’exécutera quotidiennement.
- Vérifie la création progressive des dossiers et fichiers.

## Dépendances Python (dans l’environnement d’Airflow)
- pandas
- requests
- pyarrow
- elasticsearch

## Vérification manuelle (facultative)
- Scripts `python scripts/fetch_openaq.py`, etc., pour tester indépendamment.
