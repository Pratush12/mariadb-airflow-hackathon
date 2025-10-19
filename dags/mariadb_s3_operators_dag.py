"""
Example DAG demonstrating advanced MariaDB Provider features
including ColumnStore cpimport, S3 integration, and data export.

SSH Connection Setup Required:
1. Create an SSH connection in Airflow UI with connection ID: 'mariadb_ssh_connection'
2. Configure the connection to point to your MariaDB ColumnStore server
3. Ensure the SSH user has access to execute cpimport commands and file operations

This DAG uses SSH-based execution for secure and scalable operations.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow_mariadb_provider.operators.mariadb_operator import MariaDBOperator
from airflow_mariadb_provider.operators.mariadb_cpimport_operator import MariaDBCpImportOperator
from airflow_mariadb_provider.operators.mariadb_s3_load_operator import MariaDBS3LoadOperator
from airflow_mariadb_provider.operators.mariadb_s3_dump_operator import MariaDBS3DumpOperator
from airflow_mariadb_provider.hooks.mariadb_hook import MariaDBHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Dag_MariaDB_S3_Operators',
    default_args=default_args,
    description='MariaDB S3 Dump and Load Operators with SSH-based execution',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['s3', 'mariadb', 'ssh'],
)


# Custom task to demonstrate hook usage

task_createdb = MariaDBOperator(
        task_id='create_database',
        sql="""
        CREATE DATABASE IF NOT EXISTS open_flights_data;
        """,
        mariadb_conn_id='maria_db_default',
        dag=dag,
    )

task_create_table = MariaDBOperator(
        task_id='create_tables_for_S3',
        sql="""
        CREATE or replace TABLE open_flights_data.`customers_s3_import` (
          `name` varchar(100) DEFAULT NULL,
          `email` varchar(100) DEFAULT NULL,
          `age` int(11) DEFAULT NULL,
          `balance` decimal(10,2) DEFAULT NULL,
          `created_at` datetime DEFAULT NULL,
          `status` varchar(20) DEFAULT NULL
        ) ENGINE=InnoDB
        """,
        mariadb_conn_id='maria_db_default',
        dag=dag,
    )


s3_load = MariaDBS3LoadOperator(task_id='load_data_from_s3',
                                s3_bucket='test',
                                s3_key='test_bulk_data.csv',
                                table_name='customers_s3_import',
                                schema='open_flights_data',
                                mariadb_conn_id='maria_db_default',
                                aws_conn_id='aws_default',
                                local_temp_dir='/tmp',
                                ssh_conn_id='mariadb_ssh_connection',  # Required for SSH-based execution
                                dag=dag)

s3_dump = MariaDBS3DumpOperator(task_id='dump_to_s3',
                                s3_bucket='test',
                                s3_key='s3_export_customers.csv',
                                table_name='customers_mariadb_cpimport',
                                schema='open_flights_data',
                                mariadb_conn_id='maria_db_default',
                                aws_conn_id='aws_default',
                                local_temp_dir='/tmp',  # Fixed: use absolute path
                                file_format='csv',
                                query = "select * from open_flights_data.customers_s3_import  where age>55",
                                ssh_conn_id='mariadb_ssh_connection',  # Required for SSH-based execution
                                dag=dag)
# create_columnstore_table >>
task_createdb >> task_create_table >> s3_load >> s3_dump