import pandas as pd
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "System Developer",
    "start_date": datetime.datetime(2023, 1, 1),
}

