This README file provides instructions for setting up and using the airflow-mariadb-provider, which adds custom hooks and operators for interacting with a MariaDB database.
1. Provider overview
This custom provider extends Airflow's functionality to include native integration with MariaDB. It includes:
MariaDBHook: Encapsulates the logic for connecting to and interacting with a MariaDB database.
MariaDBOperator: An operator for executing standard SQL queries against a MariaDB database.
MariaDBCpimportOperator: An operator for performing secure bulk data loading using the cpimport utility.
Custom Connection Type: Adds a "MariaDB" connection type to the Airflow UI, streamlining connection management.
2. Provider package structure
The provider's code should be organized in the following standard Python package structure.
sh
airflow-mariadb-provider/
├── setup.py
└── airflow_mariadb_provider/
    ├── __init__.py
    ├── hooks/
    │   ├── __init__.py
    │   └── mariadb_hook.py
    ├── operators/
    │   ├── __init__.py
    │   └── mariadb_operator.py
    └── mariadb_cpimport_operator.py
Use code with caution.

3. Setup and installation
Prerequisites
A running Airflow environment (e.g., in WSL).
A MariaDB database accessible from your Airflow environment.
The cpimport utility installed and available in the PATH if you plan to use bulk loading.
Installation in a virtual environment
Activate your virtual environment.
sh
source /path/to/your/airflow-venv/bin/activate
Use code with caution.

Clone or place the provider package files in a separate directory (e.g., ~/airflow-mariadb-provider).
Navigate to the provider's root directory.
sh
cd ~/airflow-mariadb-provider
Use code with caution.

Install the package in editable mode. This allows for immediate updates when you modify the source files.
sh
pip install -e .
Use code with caution.

Install the native MariaDB connector for Python.
sh
pip install mariadb
Use code with caution.

4. Configuration
Restart Airflow
After installing the provider, restart the Airflow webserver and scheduler to discover the new connection type and operators.
sh
# Restart webserver
airflow webserver -p 8080

# Restart scheduler
airflow scheduler
