# from datetime import datetime

# from airflow import DAG
# from airflow.operators.python import PythonOperator

# from pipeline.build_analytics import build_analytics
# from pipeline.extract import extract
# from pipeline.load import load
# from pipeline.transform import transform

# with DAG(
#     dag_id="crypto_pipeline",
#     start_date=datetime(2024, 1, 1),
#     schedule="@daily",
#     catchup=False,
#     tags=["crypto", "data-engineering"],
# ) as dag:
#     t1 = PythonOperator(
#         task_id="extract",
#         python_callable=extract,
#     )

#     t2 = PythonOperator(
#         task_id="transform",
#         python_callable=transform,
#     )

#     t3 = PythonOperator(
#         task_id="load",
#         python_callable=load,
#     )

#     t4 = PythonOperator(
#         task_id="build_analytics",
#         python_callable=build_analytics,
#     )

#     t1 >> t2 >> t3 >> t4
