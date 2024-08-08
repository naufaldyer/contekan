import pandas as pd
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from production_pipeline_processed.pipeline_master_item_d365.function_to_check_hpp import (
    main,
)

default_args = {
    "owner": "System Developer",
    "start_date": datetime.datetime(2023, 1, 1),
}

with DAG(
    default_args=default_args,
    dag_id="processed_master_item_d365",
    schedule_interval=None,
    # schedule_interval="0 23 * * *",
    catchup=False,
    tags=["Processed", "D365"],
) as dag:
    task1 = PythonOperator(
        task_id="process_master_item_to_processed",
        python_callable=main,
        provide_context=True,
    )
