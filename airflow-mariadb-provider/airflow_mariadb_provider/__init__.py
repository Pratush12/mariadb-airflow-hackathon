from __future__ import annotations

from typing import Any

def get_provider_info() -> dict[str, Any]:
    return {
        "package-name": "airflow-mariadb-provider",
        "name": "MariaDB Provider",
        "description": "A provider to interact with MariaDB databases using the native connector.",
        "versions": ["0.1.0"],
        "hooks": [
            {
                "class-name": "airflow_mariadb_provider.hooks.mariadb_hook.MariaDBHook",
                "hook-name": "MariaDBHook",
            },
        ],
        "operators": [
            {
                "class-name": "airflow_mariadb_provider.operators.mariadb_operator.MariaDBOperator",
                "operator-name": "MariaDBOperator",
            },
            {
                "class-name": "airflow_mariadb_provider.operators.mariadb_cpimport_operator.MariaDBCpImportOperator",
                "operator-name": "MariaDBCpImportOperator",
            },
            {
                "class-name": "airflow_mariadb_provider.operators.mariadb_s3_load_operator.MariaDBS3LoadOperator",
                "operator-name": "MariaDBS3LoadOperator",
            },
            {
                "class-name": "airflow_mariadb_provider.operators.mariadb_s3_dump_operator.MariaDBS3DumpOperator",
                "operator-name": "MariaDBS3DumpOperator",
            },
        ],
        # Add this section
        "connection-types": [
            {
                "connection-type": "mariadb",
                "hook-class-name": "airflow_mariadb_provider.hooks.mariadb_hook.MariaDBHook",
            }
        ],
    }