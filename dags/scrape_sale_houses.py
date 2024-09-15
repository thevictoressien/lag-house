from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from dags.scripts.bq_utils import load_schema
from dags.scripts.house_scrapper import scrape_and_upload
import time

execution_date = "{{ ds_nodash }}"
category = "for_sale"
base_url = Variable.get("base_url")
city = Variable.get("city")
dag_name = f"{city}_{category}_listings_full_load"
description = f"ELT DAG for scraping and loading {category} listings from {base_url} loading to GCS and then to BigQuery"
schedule_interval = "0 0 31 * *" # the 31st day of every month
tags = [f"{category}", "scrape", "raw", "full load"]
file_name = f"{category}_listings{execution_date}"
start_page = Variable.get("start_page")
end_page = Variable.get("end_page")

# GCS vars
# GCP_CONNECTION_ID = Variable.get("google_conn")
BQ_PROJECT_ID =  Variable.get("project_id") 
BQ_DATASET_ID = Variable.get("dataset_id")
BQ_TABLE_ID = f"{city}_{category}_listings_raw"
GCS_BUCKET_NAME = Variable.get("bucket_name")
bq_schema = Variable.get(f"lag_house_schema") # schema stored in gcs bucket



default_args = {
    "owner": "VEE",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=4),
}


dag = DAG(
    dag_id=dag_name,
    default_args=default_args,
    description=description,
    schedule_interval=schedule_interval,
    start_date=days_ago(1),
    catchup=False,
    tags=tags,
)


start = EmptyOperator(dag=dag, task_id="start")


scrape_to_gcs = PythonOperator(
    task_id='scrape_to_gcs',
    python_callable=scrape_and_upload,
    op_kwargs={
        'bucket_name': GCS_BUCKET_NAME,
        'file_name': file_name,
        'base_url': base_url,
        'category': category,
        'city': city,
        'start_page': 1,
        'end_page': 2858,
    },
    dag=dag,
)


gcs_to_bigquery = GCSToBigQueryOperator(
    task_id=f'gcs_{category}_bigquery',
    bucket=GCS_BUCKET_NAME,
    # source_objects=[f"{file_name.split('.')[0]}*.csv"],
    source_objects = ['for_sale_listings14-09-2024*.csv'],
    destination_project_dataset_table=f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}",
    schema_fields= load_schema(bq_schema),
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

start >> scrape_to_gcs >> gcs_to_bigquery