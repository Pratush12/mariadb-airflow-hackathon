#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from typing import Optional, Sequence
from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.exceptions import AirflowException

from airflow_mariadb_provider.hooks.mariadb_hook import MariaDBHook


class MariaDBS3DumpOperator(BaseOperator):
    """
    Export MariaDB table data to AWS S3.

    This operator exports data from a MariaDB table and uploads it to S3.
    It supports multiple export formats including CSV, JSON, and SQL.

    :param table_name: Source MariaDB table name
    :param s3_bucket: S3 bucket name
    :param s3_key: S3 object key for the exported file
    :param schema: Database schema name (optional, uses connection schema if not provided)
    :param mariadb_conn_id: Airflow connection ID for MariaDB
    :param aws_conn_id: Airflow connection ID for AWS
    :param local_temp_dir: Local temporary directory for file export
    :param file_format: Export file format (csv, json, sql)
    """

    template_fields: Sequence[str] = ("table_name", "s3_bucket", "s3_key", "schema")
    template_fields_renderers = {
        "s3_bucket": "bash",
        "s3_key": "bash",
    }
    ui_color = "#98fb98"
    ui_fgcolor = "#000000"

    def __init__(
            self,
            *,
            table_name: str,
            s3_bucket: str,
            s3_key: str,
            query: Optional[str] = None,
            schema: Optional[str] = None,
            mariadb_conn_id: str = "mariadb_default",
            aws_conn_id: str = "aws_default",
            local_temp_dir: Optional[str] = None,

            file_format: str = "csv",
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.query = query
        self.schema = schema
        self.mariadb_conn_id = mariadb_conn_id
        self.aws_conn_id = aws_conn_id
        self.local_temp_dir = local_temp_dir
        self.file_format = file_format.lower()

    def execute(self, context: Context) -> bool:
        """
        Execute the MariaDB to S3 dump operation.

        Args:
            context: Airflow task context

        Returns:
            bool: True if dump was successful

        Raises:
            AirflowException: If dump operation fails
        """
        self.log.info(f"Starting MariaDB to S3 dump operation")
        self.log.info(f"Source table: {self.schema}.{self.table_name}")
        self.log.info(f"S3 destination: s3://{self.s3_bucket}/{self.s3_key}")
        self.log.info(f"Export format: {self.file_format}")

        # Validate file format
        if self.file_format not in ['csv', 'json', 'sql']:
            raise AirflowException(f"Unsupported file format: {self.file_format}. Supported formats: csv, json, sql")

        # Initialize MariaDB hook
        hook = MariaDBHook(mariadb_conn_id=self.mariadb_conn_id)

        try:
            # Dump data from MariaDB to S3
            result = hook.dump_to_s3(
                table_name=self.table_name,
                s3_bucket=self.s3_bucket,
                s3_key=self.s3_key,
                query=self.query,
                schema=self.schema,
                aws_conn_id=self.aws_conn_id,
                local_temp_dir=self.local_temp_dir,
                file_format=self.file_format
            )

            if result:
                self.log.info(f"✅ Successfully dumped data from {self.table_name} to S3")
                return True
            else:
                raise AirflowException("S3 dump operation returned False")

        except Exception as e:
            self.log.error(f"❌ S3 dump operation failed: {e}")
            raise AirflowException(f"S3 dump operation failed: {e}")

    def on_kill(self) -> None:
        """Called when the task is killed."""
        self.log.warning("S3 dump operation was killed")
