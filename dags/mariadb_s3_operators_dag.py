"""
Example DAG demonstrating advanced MariaDB Provider features
including ColumnStore cpimport, S3 integration, and data export.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task

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
    'MariaDBS3Operators',
    default_args=default_args,
    description='Mariadb S3 Load and Dump Operators',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['s3', 'mariadb'],
)

# Custom task to demonstrate hook usage

s3_load = MariaDBS3LoadOperator(task_id='load_data_from_s3',
                                s3_bucket='test',
                                s3_key='test_bulk_data.csv',
                                table_name='customers_mariadb_cpimport',
                                schema='test_bulk',
                                mariadb_conn_id='maria_db_default',
                                aws_conn_id='aws_default',
                                local_temp_dir='/tmp',
                                dag=dag)

s3_dump = MariaDBS3DumpOperator(task_id='dump_to_s3',
                                s3_bucket='test',
                                s3_key='test_bulk_data_1.csv',
                                table_name='customers_mariadb_cpimport',
                                schema='test_bulk',
                                mariadb_conn_id='maria_db_default',
                                aws_conn_id='aws_default',
                                local_temp_dir='tmp',
                                file_format='csv',
                                dag=dag)
# create_columnstore_table >>
s3_load >> s3_dump