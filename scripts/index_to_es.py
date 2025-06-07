
import os
import datetime
import pandas as pd
from pathlib import Path
from elasticsearch import Elasticsearch, helpers

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)) + "/../"

def index_to_elasticsearch(**context):
    """
    - Lit combined_YYYY-MM-DD.parquet
    - Transforme chaque ligne en document JSON
    - Indexe dans Elasticsearch index 'air_asthma'
    """
    es = Elasticsearch(hosts=["http://elasticsearch:9200"])

    today = datetime.date.today().isoformat()
    combined_path = Path(PROJECT_ROOT) / f"data/final/combined_{today}.parquet"
    df = pd.read_parquet(combined_path)

    index_name = "air_asthma"
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body={
            "mappings": {
                "properties": {
                    "date":        {"type": "date"},
                    "pm25":        {"type": "float"},
                    "asthma_rate": {"type": "float"},
                    "year":        {"type": "integer"}
                }
            }
        })

    actions = []
    for _, row in df.iterrows():
        doc = {
            "_index": index_name,
            "_source": {
                "date": row["date"].isoformat(),
                "pm25": float(row["pm25"]),
                "asthma_rate": float(row["asthma_rate"]),
                "year": int(row["year"])
            }
        }
        actions.append(doc)

    helpers.bulk(es, actions)