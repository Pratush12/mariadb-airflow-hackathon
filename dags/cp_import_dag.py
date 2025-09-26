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
    'MariadbCpimport',
    default_args=default_args,
    description='Mariadb Cpimport',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['columnstore'],
)
'''
# Create ColumnStore table for cpimport testing
create_columnstore_table = MariaDBOperator(
    task_id='create_columnstore_table',
    sql="""
    CREATE TABLE test_bulk.customers_mariadb_cpimport (
            name VARCHAR(100),
            email VARCHAR(100),
            age INT,
            balance DECIMAL(10,2),
            created_at DATETIME,
            status VARCHAR(20)
    ) ENGINE=ColumnStore;
    """,
    mariadb_conn_id='maria_db_default',
    autocommit=True,
    dag=dag,
)
'''
# test_bulk customers_mariadb
# Example 3: Use cpimport directly with local file
'''
cpimport_local_file = MariaDBCpImportOperator(
    task_id='cpimport_local_file',
    table_name='customers_mariadb_cpimport',
    file_path='/var/data/test_bulk_data.csv',
    schema='test_bulk',
    mariadb_conn_id='maria_db_default',
    dag=dag,
)
'''
#cpimport -s ',' -e 10 -E '"' -n '\N' open_flights_data airlines /var/openflights_data/airlines.dat

cpimport_local_file = MariaDBCpImportOperator(
    task_id='cpimport_local_file',
    table_name='airlines',
    file_path='/var/openflights_data/airlines.dat',
    schema='open_flights_data',
    mariadb_conn_id='maria_db_default',
    cpimport_options={
        '-E': '"',  # table
        '-n': r'\N'  # error limit
    },
    dag=dag,
)
# Custom task to demonstrate hook usage


# create_columnstore_table >>
cpimport_local_file