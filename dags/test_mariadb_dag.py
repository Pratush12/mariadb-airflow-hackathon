# File: dags/test_mariadb_dag.py
from datetime import datetime
from airflow import DAG
from airflow_mariadb_provider.operators.mariadb_operator import MariaDBOperator

with DAG(
    dag_id="test_mariadb_connection",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test", "mariadb"],
) as dag:

    test_query = MariaDBOperator(
        task_id="test_query",
        mariadb_conn_id="mariadb_default",  # your connection ID
        sql="SELECT 1;",  # simple query to test connection
    )