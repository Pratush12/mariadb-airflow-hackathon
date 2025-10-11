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


class MariaDBS3LoadOperator(BaseOperator):
    """
    Load data from AWS S3 to MariaDB table.

    This operator downloads data from S3 and loads it into a MariaDB table.
    It automatically detects if the table uses ColumnStore engine and uses
    cpimport for optimal performance, or falls back to LOAD DATA INFILE
    for regular tables.

    :param s3_bucket: S3 bucket name
    :param s3_key: S3 object key
    :param table_name: Target MariaDB table name
    :param schema: Database schema name (optional, uses connection schema if not provided)
    :param mariadb_conn_id: Airflow connection ID for MariaDB
    :param aws_conn_id: Airflow connection ID for AWS
    :param local_temp_dir: Local temporary directory for file download
    """

    template_fields: Sequence[str] = ("s3_bucket", "s3_key", "table_name", "schema")
    template_fields_renderers = {
        "s3_bucket": "bash",
        "s3_key": "bash",
    }
    ui_color = "#ffd700"
    ui_fgcolor = "#000000"

    def __init__(
            self,
            *,
            s3_bucket: str,
            s3_key: str,
            table_name: str,
            schema: Optional[str] = None,
            mariadb_conn_id: str = "mariadb_default",
            aws_conn_id: str = "aws_default",
            local_temp_dir: Optional[str] = None,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table_name = table_name
        self.schema = schema
        self.mariadb_conn_id = mariadb_conn_id
        self.aws_conn_id = aws_conn_id
        self.local_temp_dir = local_temp_dir

    def execute(self, context: Context) -> bool:
        """
        Execute the S3 to MariaDB load operation.

        Args:
            context: Airflow task context

        Returns:
            bool: True if load was successful

        Raises:
            AirflowException: If load operation fails
        """
        self.log.info(f"Starting S3 to MariaDB load operation")
        self.log.info(f"S3 source: s3://{self.s3_bucket}/{self.s3_key}")
        self.log.info(f"Target table: {self.schema}.{self.table_name}")

        # Initialize MariaDB hook
        hook = MariaDBHook(mariadb_conn_id=self.mariadb_conn_id)

        try:
            # Load data from S3 to MariaDB
            result = hook.load_from_s3(
                s3_bucket=self.s3_bucket,
                s3_key=self.s3_key,
                table_name=self.table_name,
                schema=self.schema,
                aws_conn_id=self.aws_conn_id,
                local_temp_dir=self.local_temp_dir
            )

            if result:
                self.log.info(f"✅ Successfully loaded data from S3 to {self.table_name}")
                return True
            else:
                raise AirflowException("S3 load operation returned False")

        except Exception as e:
            self.log.error(f"❌ S3 load operation failed: {e}")
            raise AirflowException(f"S3 load operation failed: {e}")

    def on_kill(self) -> None:
        """Called when the task is killed."""
        self.log.warning("S3 load operation was killed")
