from datetime import datetime, timedelta

from airflow import DAG
from pipeline.build_analytics import build_analytics
from pipeline.extract import extract
from pipeline.transform import transform
from pipeline.load import load
from pipeline.quality import quality_check
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


default_args = {
    'owner': 'levi',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

with DAG(
    dag_id="crypto_platform_daily",
    start_date=datetime(2024, 1, 1),
    schedule ="@daily",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:
    
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
        op_kwargs={"run_date": "{{ ds }}"},
    )

    transform_batch = PythonOperator(
        task_id="transform_batch",
        python_callable=transform,
        op_kwargs={"run_date": "{{ ds }}"},

    )

    load_warehouse = PythonOperator(
        task_id = "load_warehouse",
        python_callable=load,
        op_kwargs={"run_date": "{{ ds }}"},
    )

    quality_check_task = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check,
        op_kwargs={"run_date": "{{ ds }}"},
    )

    build_analytics_task = PythonOperator(
        task_id="build_analytics",
        python_callable=build_analytics,
        op_kwargs={"run_date": "{{ ds }}"},
    )


    dbt_run = EmptyOperator(task_id="dbt_run")
    dbt_test = EmptyOperator(task_id="dbt_test")
    refresh_superset_dataset = EmptyOperator(task_id="refresh_superset_dataset")

    extract_task >> transform_batch >> load_warehouse >> quality_check_task >> build_analytics_task >> dbt_run >> dbt_test >>  refresh_superset_dataset

