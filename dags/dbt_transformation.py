from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable

# Get the city variable
city = Variable.get("city")


# dag args
dag_name = "dbt_transformation"
schedule_interval = timedelta(days=1)
description = "DAG that triggers dbt transformations of BigQuery tables"
tags = ["dbt", "transformation", "model"]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
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

wait_for_sale_houses = ExternalTaskSensor(
    task_id='wait_for_sale_houses',
    external_dag_id=f"{city}_for_sale_listings_full_load",
    external_task_id=None,
    mode='reschedule',
    dag=dag
)

wait_for_rental_houses = ExternalTaskSensor(
    task_id='wait_for_rental_houses',
    external_dag_id=f"{city}_for_rent_listings_full_load",
    external_task_id=None,
    mode='reschedule',
    dag=dag
)

run_dbt_models = BashOperator(
    task_id='run_dbt_models',
    bash_command= 'dbt run --profiles-dir /home/airflow/gcs/data/lag_house_dbt --project-dir /home/airflow/gcs/data/lag_house_dbt',
    dag=dag
)

start >> [wait_for_sale_houses, wait_for_rental_houses] >> run_dbt_models