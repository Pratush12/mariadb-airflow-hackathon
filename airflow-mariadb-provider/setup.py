from setuptools import setup, find_packages

setup(
    name='airflow-mariadb-provider',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'apache-airflow>=2.10.2',
        'mariadb',
    ],
    entry_points={
        'apache_airflow_provider': [
            'provider_info = airflow_mariadb_provider.__init__:get_provider_info'
        ]
    }
)
