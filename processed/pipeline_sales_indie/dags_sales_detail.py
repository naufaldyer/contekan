import pandas as pd
import numpy as np
import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


import sys

# Import Function
sys.path.append("/home/ubuntu/dags/SALES_DETAIL/")
from production_pipeline.processed.pipeline_sales_indie.function_sales_detail import main_process
from production_pipeline.processed.pipeline_sales_indie.function_sales_report import get_sales_report


def automated_date():
    today = pd.to_datetime(datetime.datetime.today())
    today_year = today.strftime("%Y")
    today_month = today.strftime("%m")
    last = today - pd.DateOffset(months=1)
    last_year = last.strftime("%Y")
    last_month = last.strftime("%m")

    if today.day == 1:
        dict_month = {"list_month": [last_year, last_month.zfill(2)]}
    elif today.day > 1 and today.day <= 5:
        dict_month = {
            "list_last_month": [last_year, last_month.zfill(2)],
            "list_month": [today_year, today_month.zfill(2)],
        }
    else:
        dict_month = {"list_month": [today_year, today_month.zfill(2)]}
    return dict_month


def generate_sales_detail():
    dict_month = automated_date()
    # dict_month = {"list_month": ["2024", "04"]}

    # Looping dict_month to get a year and month
    for key in dict_month.keys():
        year = dict_month[key][0]
        month = dict_month[key][1]
        main_process(year, month)


def generate_sales_report():
    dict_month = automated_date()
    # dict_month = {"list_month": ["2024", "04"]}

    # Looping dict_month to get a year and month
    for key in dict_month.keys():
        year = dict_month[key][0]
        month = dict_month[key][1]
        filter_string = "istransaction != '0' & status_order != 7 & issettled != False"

        print("start generate")
        df = pd.read_parquet(
            f"s3://mega-dev-lake/ProcessedData/Sales/sales_detail_indie/{year}/{month}/data.parquet"
        ).query(filter_string)

        get_sales_report(year, month, df)
        # main_process_daily_closing(year, month)

        # get_nosales_keyaccount(year, month, "mpr", df)
        # get_input_check(year, month, "KEY ACCOUNT")
        # get_input_check(year, month, "ONLINE")
        # get_no_sales(year, month)
        # get_daily_closing(year, month)


with DAG(
    dag_id="sales_detail_indie_V2",
    start_date=datetime.datetime(2022, 1, 1),
    schedule_interval="@once",
    catchup=False,
    tags=["sales", "accumulation"],
) as dags:
    sales_detail_indie_pos = PythonOperator(
        task_id="sales_detail_indie_pos",
        python_callable=generate_sales_detail,
        provide_context=True,
    )
    report_sales_daily = PythonOperator(
        task_id="report_sales_daily",
        python_callable=generate_sales_report,
        provide_context=True,
    )

    # Trigger Dags
    trigger_main = DummyOperator(
        task_id="trigger_main",
    )

    trigger_sales_accumulation = TriggerDagRunOperator(
        task_id="trigger_sales_accumulation", trigger_dag_id="sales_accumulation"
    )

    trigger_sales_accumulation_db = TriggerDagRunOperator(
        task_id="trigger_sales_accumulation_v2",
        trigger_dag_id="process_report_accumulation_to_db",
    )

    (
        sales_detail_indie_pos
        >> trigger_main
        >> [
            report_sales_daily,
            trigger_sales_accumulation,
            trigger_sales_accumulation_db,
        ]
    )
