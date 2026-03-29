from datetime import datetime, timedelta

from airflow import DAG
from pipeline.extract import extract
from pipeline.transform import transform
from pipeline.load import load
from pipeline.quality import quality_check
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from pipeline.analytics_quality import analytics_quality_check


default_args = {
    'owner': 'levi',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

def notify_failure(context):
    task_instance = context["task_instance"]
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    run_id = context["run_id"]

    print(
        f"[ALERT] DAG failure detected: dag_id={dag_id}, task_id={task_id}, run_id={run_id}"
    )

with DAG(
    dag_id="crypto_platform_daily",
    description="Daily crypto batch pipeline with warehouse loading, partition-level quality checks, and dbt-based analytics modeling.",
    start_date=datetime(2024, 1, 1),
    schedule ="@daily",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["crypto", "data-engineering", "airflow", "dbt"],
    on_failure_callback=notify_failure,
) as dag:
    
    start = EmptyOperator(task_id="start")

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

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --target docker --profiles-dir /opt/airflow/dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --target docker --profiles-dir /opt/airflow/dbt",
    )

    analytics_quality_check_task = PythonOperator(
        task_id="analytics_quality_check",
        python_callable=analytics_quality_check,
        op_kwargs={"run_date": "{{ ds }}"},
    )

    refresh_superset_dataset = EmptyOperator(task_id="refresh_superset_dataset")

    end = EmptyOperator(task_id="end")

    (
        start
        >> extract_task
        >> transform_batch
        >> load_warehouse
        >> quality_check_task
        >> dbt_run
        >> dbt_test
        >> analytics_quality_check_task
        >> refresh_superset_dataset
        >> end
    )



    
