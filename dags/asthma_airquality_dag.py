from airflow.decorators import dag, task
from datetime import datetime, timedelta
import subprocess

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.now() - timedelta(days=1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1
}

@dag(
    dag_id="asthma_airquality_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)
def asthma_pipeline():

    @task
    def fetch_air_quality():
        subprocess.run(["python", "/opt/airflow/scripts/fetch_openaq.py"], check=True)

    @task
    def fetch_cdc_hospitalizations():
        subprocess.run(["python", "/opt/airflow/scripts/fetch_cdc.py"], check=True)

    @task
    def transform_airquality():
        subprocess.run(["python", "/opt/airflow/scripts/transform_airquality.py"], check=True)

    @task
    def transform_hospitalizations():
        subprocess.run(["python", "/opt/airflow/scripts/transform_hospitalizations.py"], check=True)

    @task
    def combine_datasets():
        subprocess.run(["python", "/opt/airflow/scripts/combine_datasets.py"], check=True)

    @task
    def index_to_elasticsearch():
        subprocess.run(["python", "/opt/airflow/scripts/index_to_es.py"], check=True)

    # Orchestration
    aq = fetch_air_quality()
    hosp = fetch_cdc_hospitalizations()
    taq = transform_airquality()
    thosp = transform_hospitalizations()
    combined = combine_datasets()
    indexed = index_to_elasticsearch()

    aq >> taq
    hosp >> thosp
    [taq, thosp] >> combined >> indexed

dag = asthma_pipeline()
