import json
import os
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow_mariadb_provider.operators.mariadb_operator import MariaDBOperator
from airflow_mariadb_provider.operators.mariadb_cpimport_operator import MariaDBCpImportOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow_mariadb_provider.hooks.mariadb_hook import MariaDBHook

import requests

CONFIG_PATH = Path(__file__).parent / "config" / "datasets.json"


def drop_table_if_exists(schema, table, mariadb_conn_id='maria_db_default'):
    hook = MariaDBHook(mariadb_conn_id=mariadb_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"""
    drop table if exists {schema}.{table};  
    """)
    cursor.close()
    conn.close()


def create_table(ddl_content, mariadb_conn_id='maria_db_default'):
    hook = MariaDBHook(mariadb_conn_id=mariadb_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(ddl_content)
    cursor.close()
    conn.close()


def download_file(url: str, local_path: str):
    response = requests.get(url)
    with open(local_path, 'wb') as file:
        file.write(response.content)


with DAG(
        dag_id="OpenFlightsDataIngestion",
        description="Download OpenFlights data and load into MariaDB ColumnStore using cpimport",
        start_date=days_ago(1),
        schedule_interval=None,
        catchup=False,
) as dag:
    start_task = DummyOperator(
        task_id='start_workflow',
        dag=dag,  # Assuming 'dag' is your DAG object
    )

    task_createdb = MariaDBOperator(
        task_id='create_database',
        sql="""
        CREATE DATABASE IF NOT EXISTS open_flights_data;
        """,
        mariadb_conn_id='maria_db_default',
        dag=dag,
    )

    end_task = DummyOperator(
        task_id='end_workflow',
        dag=dag,
    )

    with open(CONFIG_PATH) as f:
        datasets = json.load(f)
    task_groups = []

    for d in datasets:
        with TaskGroup(group_id=f"group_{d['name']}") as tg:
            download = PythonOperator(
                task_id=f"download_{d['name']}",
                python_callable=download_file,
                op_kwargs={"url": d["url"], "local_path": d["local_path"]},
            )

            drop_table = PythonOperator(
                task_id=f"drop_table_{d['name']}",
                python_callable=drop_table_if_exists,
                op_kwargs={"schema": d["schema"], "table": d["table"]},
            )

            create_table_task = PythonOperator(
                task_id=f"create_table_{d['name']}",
                python_callable=create_table,
                op_kwargs={"ddl_content": d["ddl_content"]},
            )

            copy_to_docker = BashOperator(
                task_id=f"copy_to_docker_{d['name']}",
                bash_command=f"docker cp {d['local_path']} mcs1:{d['docker_path']}"
            )

            cpimport_task = MariaDBCpImportOperator(
                task_id=f"cpimport_{d['name']}",
                table_name=d["table"],
                file_path=d["docker_path"],
                schema=d["schema"],
                mariadb_conn_id='maria_db_default',
                cpimport_options={
                    '-E': '"',  # Quotes
                    '-n': r'\N'  # nulls
                },
                dag=dag,
            )

            download >> drop_table >> create_table_task >> copy_to_docker >> cpimport_task
        task_groups.append(tg)
    start_task>> task_createdb >> task_groups >> end_task