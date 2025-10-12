"""
OpenFlights Data Ingestion DAG

This DAG downloads OpenFlights data and loads it into MariaDB ColumnStore using cpimport.

SSH Connection Setup Required:
1. Create an SSH connection in Airflow UI with connection ID: 'mariadb_ssh_connection'
2. Configure the connection to point to your MariaDB ColumnStore server
3. Ensure the SSH user has access to execute cpimport commands

The DAG uses SSH-based execution for better security and scalability.
"""

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
from airflow.providers.sftp.operators.sftp import SFTPOperator

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
        description="Download OpenFlights data and load into MariaDB ColumnStore using cpimport with SSH",
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


            copy_via_ssh = SFTPOperator(
                task_id=f"copy_via_ssh_{d['name']}",
                ssh_conn_id="mariadb_ssh_connection",
                local_filepath=d['local_path'],
                remote_filepath=d['ssh_path'],
                operation="put",
                create_intermediate_dirs=True
                    )

            cpimport_task = MariaDBCpImportOperator(
                task_id=f"cpimport_{d['name']}",
                table_name=d["table"],
                file_path=d["ssh_path"],
                schema=d["schema"],
                mariadb_conn_id='maria_db_default',
                ssh_conn_id='mariadb_ssh_connection',  # Use SSH connection for cpimport
                cpimport_options={
                    '-E': '"',  # Quotes
                    '-n': r'\N'  # nulls
                },
                dag=dag,
            )

            download >> drop_table >> create_table_task >> copy_via_ssh >> cpimport_task
        task_groups.append(tg)
    start_task>> task_createdb >> task_groups >> end_task