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
import requests

CONFIG_PATH = Path(__file__).parent / "config" / "datasets.json"


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

            truncate_table = MariaDBOperator(
                task_id=f'truncate_table_{d['name']}',
                sql=f"""
                Truncate table {d["schema"]}.{d["table"]}
                """,
                mariadb_conn_id='maria_db_default',
                autocommit=True,
                dag=dag,
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

            download >> truncate_table >> copy_to_docker >> cpimport_task
        task_groups.append(tg)
    start_task >> task_groups >> end_task