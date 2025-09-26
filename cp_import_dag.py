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
cpimport_local_file = MariaDBCpImportOperator(
    task_id='cpimport_local_file',
    table_name='customers_mariadb_cpimport',
    file_path='/var/data/test_bulk_data.csv',
    schema='test_bulk',
    mariadb_conn_id='maria_db_default',
    dag=dag,
)


# Custom task to demonstrate hook usage
@task
def validate_columnstore_engine():
    """Validate that ColumnStore table exists and has correct engine."""
    hook = MariaDBHook(mariadb_conn_id='mariadb_default')

    try:
        # This will raise an exception if table doesn't use ColumnStore
        hook.validate_columnstore_engine('sales_data', 'analytics')
        return "✓ ColumnStore validation passed"
    except Exception as e:
        return f"❌ ColumnStore validation failed: {e}"


validation_task = validate_columnstore_engine()
cpimport_options={
        '-s': ',',  # separator
        '-t': 'customers_mariadb_cpimport',  # table
        '-e': '1'  # error limit
    },
# create_columnstore_table >>
cpimport_local_file