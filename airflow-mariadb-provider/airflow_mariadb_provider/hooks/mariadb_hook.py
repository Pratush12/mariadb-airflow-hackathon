import mariadb
import boto3
import json
import subprocess
import tempfile
import os
from typing import Optional, Dict, Any, List
from airflow.hooks.dbapi import DbApiHook
from airflow.exceptions import AirflowException
from airflow.providers.ssh.hooks.ssh import SSHHook


class MariaDBHook(DbApiHook):
    """
    Interact with MariaDB database using the native MariaDB client library.
    """
    conn_name_attr = 'mariadb_conn_id'
    default_conn_name = 'mariadb_default'
    conn_type = 'mariadb'
    hook_name = 'MariaDB'

    def get_conn(self):
        """Returns a connection object."""
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        conn_config = {
            'host': conn.host,
            'port': conn.port,
            'user': conn.login,
            'password': conn.password,
            'database': conn.schema,
        }
        # Add any extras from the connection if necessary
        if conn.extra:
            import json
            extra_options = json.loads(conn.extra)
            conn_config.update(extra_options)

        self.log.info(f'Connecting to MariaDB database: {conn.schema}')
        try:
            return mariadb.connect(**conn_config)
        except mariadb.Error as e:
            self.log.error(f"Error connecting to MariaDB: {e}")
            raise e

    def validate_columnstore_engine(self, table_name: str, schema: Optional[str] = None) -> bool:
        """
        Validate that a table uses ColumnStore engine.

        Args:
            table_name: Name of the table to check
            schema: Database schema name (optional, uses connection schema if not provided)

        Returns:
            bool: True if table uses ColumnStore engine, False otherwise

        Raises:
            AirflowException: If table doesn't exist or uses wrong engine
        """
        if not schema:
            conn = self.get_connection(getattr(self, self.conn_name_attr))
            schema = conn.schema

        check_sql = """
        SELECT ENGINE 
        FROM information_schema.TABLES 
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """

        try:
            with self.get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(check_sql, (schema, table_name))
                result = cursor.fetchone()

                if not result:
                    raise AirflowException(f"Table {schema}.{table_name} does not exist")

                engine = result[0]
                self.log.info(f"Table {schema}.{table_name} uses engine: {engine}")

                if engine.upper() != 'COLUMNSTORE':
                    error_msg = f"Table {schema}.{table_name} uses {engine} engine, but ColumnStore is required for cpimport"
                    self.log.error(error_msg)
                    raise AirflowException(error_msg)

                self.log.info(f"✓ Table {schema}.{table_name} uses ColumnStore engine")
                return True

        except mariadb.Error as e:
            self.log.error(f"Error checking table engine: {e}")
            raise AirflowException(f"Failed to check table engine: {e}")

    def execute_cpimport(self, table_name: str, file_path: str, schema: Optional[str] = None,
                         options: Optional[Dict[str, Any]] = None, ssh_conn_id: str = None) -> bool:
        """
        Execute cpimport command for ColumnStore tables via SSH.

        Args:
            table_name: Name of the target table
            file_path: Path to the data file to import
            schema: Database schema name (optional)
            options: Additional cpimport options
            ssh_conn_id: SSH connection ID for remote execution (required)

        Returns:
            bool: True if import was successful

        Raises:
            AirflowException: If validation fails, SSH connection is missing, or cpimport command fails
        """
        if not schema:
            conn = self.get_connection(getattr(self, self.conn_name_attr))
            schema = conn.schema

        # Validate ColumnStore engine first
        self.validate_columnstore_engine(table_name, schema)

        # Execute cpimport command via SSH
        if not ssh_conn_id:
            raise AirflowException("SSH connection ID is required for cpimport execution")
        
        return self._execute_cpimport_ssh(table_name, file_path, schema, options, ssh_conn_id)

    def _execute_cpimport_ssh(self, table_name: str, file_path: str, schema: str, 
                             options: Optional[Dict[str, Any]], ssh_conn_id: str) -> bool:
        """Execute cpimport command via SSH."""
        try:
            ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
            
            # Build cpimport command for SSH execution using the working format
            cmd_parts = ["cpimport"]
            
            # Add separator option if not provided
            if not options or '-s' not in options:
                cmd_parts.extend(['-s', "','"])
            
            # Add additional options if provided
            if options:
                for key, value in options.items():
                    if key.startswith('-'):
                        # Handle special characters with proper escaping
                        if key == '-E' and value == '"':
                            cmd_parts.extend([key, '\\"'])
                        elif key == '-n' and value == r'\N':
                            cmd_parts.extend([key, '\\\\N'])
                        else:
                            cmd_parts.extend([key, str(value)])
                    else:
                        cmd_parts.extend([f"-{key}", str(value)])
            
            # Add schema, table, and file path at the end
            cmd_parts.extend([schema, table_name, file_path])
            
            # Join command parts with spaces
            cmd = " ".join(cmd_parts)
            self.log.info(f"Executing cpimport command via SSH: {cmd}")
            
            # Execute command via SSH
            with ssh_hook.get_conn() as ssh_client:
                stdin, stdout, stderr = ssh_client.exec_command(cmd)
                exit_status = stdout.channel.recv_exit_status()
                
                # Read output and error messages
                stdout_content = stdout.read().decode('utf-8')
                stderr_content = stderr.read().decode('utf-8')
                
                if exit_status == 0:
                    self.log.info(f"cpimport completed successfully via SSH")
                    if stdout_content:
                        self.log.info(f"Output: {stdout_content}")
                    return True
                else:
                    error_msg = f"cpimport failed via SSH with exit code {exit_status}: {stderr_content}"
                    self.log.error(error_msg)
                    raise AirflowException(error_msg)
                
        except Exception as e:
            error_msg = f"SSH execution failed: {e}"
            self.log.error(error_msg)
            raise AirflowException(error_msg)

    def _execute_cpimport_docker(self, table_name: str, file_path: str, schema: str, 
                                options: Optional[Dict[str, Any]]) -> bool:
        """Execute cpimport command via direct docker execution (backward compatibility)."""
        # Build cpimport command
        cmd = ["docker", "exec", "mcs1", "cpimport", schema, table_name, file_path]

        if '-s' not in cmd and not options:
            cmd = ["docker", "exec", "mcs1", "cpimport", '-s', ",", schema, table_name, file_path]

        # Add additional options if provided
        if options:
            if '-s' not in options.values():
                cmd.extend(['-s', ","])
            for key, value in options.items():
                if key.startswith('-'):
                    cmd.extend([key, str(value)])
                else:
                    cmd.extend([f"-{key}", str(value)])

        self.log.info(f"Executing cpimport command via Docker: {' '.join(cmd)}")

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            self.log.info(f"cpimport completed successfully via Docker")
            return True

        except subprocess.CalledProcessError as e:
            error_msg = f"cpimport failed with return code {e.returncode}: {e.stderr}"
            self.log.error(error_msg)
            raise AirflowException(error_msg)
        except FileNotFoundError:
            error_msg = "cpimport command not found. Please ensure MariaDB ColumnStore tools are installed"
            self.log.error(error_msg)
            raise AirflowException(error_msg)

    def _copy_file_via_ssh(self, local_file_path: str, remote_file_path: str, ssh_conn_id: str) -> None:
        """Copy file to remote server via SSH."""
        try:
            ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
            
            # Use SFTP to copy the file
            with ssh_hook.get_conn() as ssh_client:
                sftp_client = ssh_client.open_sftp()
                sftp_client.put(local_file_path, remote_file_path)
                sftp_client.close()
                
            self.log.info(f"Successfully copied {local_file_path} to {remote_file_path} via SSH")
            
        except Exception as e:
            error_msg = f"Failed to copy file via SSH: {e}"
            self.log.error(error_msg)
            raise AirflowException(error_msg)

    def _copy_file_from_ssh(self, remote_file_path: str, local_file_path: str, ssh_conn_id: str) -> None:
        """Copy file from remote server via SSH."""
        try:
            ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
            
            # Use SFTP to copy the file
            with ssh_hook.get_conn() as ssh_client:
                sftp_client = ssh_client.open_sftp()
                sftp_client.get(remote_file_path, local_file_path)
                sftp_client.close()
                
            self.log.info(f"Successfully copied {remote_file_path} to {local_file_path} via SSH")
            
        except Exception as e:
            error_msg = f"Failed to copy file from SSH: {e}"
            self.log.error(error_msg)
            raise AirflowException(error_msg)

    def get_s3_client(self, aws_conn_id: str = 'aws_default'):
        """
        Get S3 client using AWS connection.

        Args:
            aws_conn_id: Airflow connection ID for AWS

        Returns:
            boto3 S3 client
        """
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        s3_hook = S3Hook(aws_conn_id)
        return s3_hook.get_client_type('s3')

    def load_from_s3(self, s3_bucket: str, s3_key: str, table_name: str,
                     schema: Optional[str] = None, aws_conn_id: str = 'aws_default',
                     local_temp_dir: Optional[str] = None, ssh_conn_id: str = None) -> bool:
        """
        Load data from S3 to MariaDB table.

        Args:
            s3_bucket: S3 bucket name
            s3_key: S3 object key
            table_name: Target MariaDB table name
            schema: Database schema name (optional)
            aws_conn_id: Airflow connection ID for AWS
            local_temp_dir: Local temporary directory for file download
            ssh_conn_id: SSH connection ID for remote execution (required)

        Returns:
            bool: True if load was successful
        """
        if not schema:
            conn = self.get_connection(getattr(self, self.conn_name_attr))
            schema = conn.schema

        # Create temporary file for S3 download
        if not local_temp_dir:
            local_temp_dir = tempfile.gettempdir()

        local_file_path = os.path.join(local_temp_dir, f"{table_name}_s3_import_{os.getpid()}.csv")

        try:
            # Download file from S3
            self.log.info(f"Downloading {s3_bucket}/{s3_key} to {local_file_path}")
            s3_client = self.get_s3_client(aws_conn_id)
            self.log.info(os.listdir())
            s3_client.download_file(s3_bucket, s3_key, local_file_path)
            self.log.info(f's3 file downloaded : {local_file_path}')
            
            # Copy file to remote server via SSH
            if not ssh_conn_id:
                raise AirflowException("SSH connection ID is required for S3 load operation")
            
            self._copy_file_via_ssh(local_file_path, f"/var/data/{table_name}_s3_import_{os.getpid()}.csv", ssh_conn_id)

            # Load data into MariaDB
            self.log.info(f"Loading data from {local_file_path} to {schema}.{table_name}")

            # Check if table uses ColumnStore engine
            try:
                self.validate_columnstore_engine(table_name, schema)
                # Use cpimport for ColumnStore tables
                return self.execute_cpimport(table_name, f"/var/data/{table_name}_s3_import_{os.getpid()}.csv", schema, ssh_conn_id=ssh_conn_id)
            except AirflowException:
                # Fall back to regular LOAD DATA for non-ColumnStore tables
                self.log.info("Table is not ColumnStore, using LOAD DATA INFILE")
                return self._load_data_infile(f"/var/data/{table_name}_s3_import_{os.getpid()}.csv", table_name, schema)

        except Exception as e:
            self.log.error(f"Error loading data from S3: {e}")
            raise AirflowException(f"Failed to load data from S3: {e}")
        finally:
            # Clean up temporary file
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
                self.log.info(f"Cleaned up temporary file: {local_file_path}")

    def dump_to_s3(self, table_name: str, s3_bucket: str, s3_key: str,query: Optional[str]=None,
                   schema: Optional[str] = None, aws_conn_id: str = 'aws_default',
                   local_temp_dir: Optional[str] = None, file_format: str = 'csv', 
                   ssh_conn_id: str = None) -> bool:
        """
        Export MariaDB table data to S3.

        Args:
            table_name: Source MariaDB table name
            s3_bucket: S3 bucket name
            s3_key: S3 object key for the exported file
            query: Custom query for export (optional)
            schema: Database schema name (optional)
            aws_conn_id: Airflow connection ID for AWS
            local_temp_dir: Local temporary directory for file export
            file_format: Export file format (csv, json, sql)
            ssh_conn_id: SSH connection ID for remote execution (required)

        Returns:
            bool: True if export was successful
        """
        if not schema:
            conn = self.get_connection(getattr(self, self.conn_name_attr))
            schema = conn.schema

        # Create temporary file for export
        if not local_temp_dir:
            local_temp_dir = tempfile.gettempdir()

        file_extension = file_format.lower()
        local_file_path=os.path.join(local_temp_dir, f"{table_name}_export_{os.getpid()}.{file_extension}")
        #local_file_path = f"{table_name}_export_{os.getpid()}.{file_extension}"

        try:
            # Export data from MariaDB
            self.log.info(f"Exporting data from {schema}.{table_name} to {local_file_path}")

            if file_format.lower() == 'csv':
                self._export_to_csv(table_name,query,f"/var/outfiles/{table_name}_export_{os.getpid()}.{file_extension}", schema)
            elif file_format.lower() == 'json':
                self._export_to_json(table_name,query, f"/var/outfiles/{table_name}_export_{os.getpid()}.{file_extension}", schema)
            elif file_format.lower() == 'sql':
                self._export_to_sql(table_name,query,f"/var/outfiles/{table_name}_export_{os.getpid()}.{file_extension}", schema)
            else:
                raise AirflowException(f"Unsupported file format: {file_format}")

            # Upload to S3
            self.log.info(f"Uploading {local_file_path} to s3://{s3_bucket}/{s3_key}")
            
            # Copy file from remote server via SSH
            if not ssh_conn_id:
                raise AirflowException("SSH connection ID is required for S3 dump operation")
            
            self._copy_file_from_ssh(f"/var/outfiles/{table_name}_export_{os.getpid()}.{file_extension}", local_file_path, ssh_conn_id)
                
            s3_client = self.get_s3_client(aws_conn_id)
            s3_client.upload_file(local_file_path, s3_bucket, s3_key)

            self.log.info(f"Successfully exported {schema}.{table_name} to s3://{s3_bucket}/{s3_key}")
            return True

        except Exception as e:
            self.log.error(f"Error exporting data to S3: {e}")
            raise AirflowException(f"Failed to export data to S3: {e}")
        finally:
            # Clean up temporary file
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
                self.log.info(f"Cleaned up temporary file: {local_file_path}")

    def _load_data_infile(self, file_path: str, table_name: str, schema: str) -> bool:
        """Load data using LOAD DATA INFILE for non-ColumnStore tables."""
        full_table_name = f"{schema}.{table_name}"
        load_sql = f"""
        LOAD DATA INFILE '{file_path}'
        INTO TABLE {full_table_name}
        FIELDS TERMINATED BY ','
        ENCLOSED BY '"'
        LINES TERMINATED BY '\\n'
        IGNORE 1 ROWS
        """

        try:
            with self.get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(load_sql)
                conn.commit()
                self.log.info(f"Successfully loaded data into {full_table_name}")
                return True
        except mariadb.Error as e:
            self.log.error(f"Error loading data with LOAD DATA INFILE: {e}")
            raise AirflowException(f"Failed to load data: {e}")

    def _export_to_csv(self, table_name: str,query:str, file_path: str, schema: str) -> None:
        """Export table data to CSV format."""
        if query:
            export_sql = f"""
                {query}
                INTO OUTFILE '{file_path}'
                FIELDS TERMINATED BY ','
                ENCLOSED BY '"'
                LINES TERMINATED BY '\\n'
                """
        else:
            full_table_name = f"{schema}.{table_name}"
            export_sql = f"""
            SELECT * FROM {full_table_name}
            INTO OUTFILE '{file_path}'
            FIELDS TERMINATED BY ','
            ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            """
        self.log.info(export_sql)

        with self.get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(export_sql)

    def _export_to_json(self, table_name: str,query:str, file_path: str, schema: str) -> None:
        """Export table data to JSON format."""
        if query:
            select_sql = query
        else:
            full_table_name = f"{schema}.{table_name}"
            select_sql = f"SELECT * FROM {full_table_name}"

        with self.get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(select_sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            data = []
            for row in rows:
                data.append(dict(zip(columns, row)))

            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)

    def _export_to_sql(self, table_name: str,query:str, file_path: str, schema: str) -> None:
        """Export table data to SQL format using mysqldump."""
        conn = self.get_connection(getattr(self, self.conn_name_attr))

        # Build mysqldump command
        cmd = [
            'mysqldump',
            f'--host={conn.host}',
            f'--port={conn.port}',
            f'--user={conn.login}',
            f'--password={conn.password}',
            '--single-transaction',
            '--routines',
            '--triggers',
            schema,
            table_name
        ]

        try:
            with open(file_path, 'w') as f:
                subprocess.run(cmd, stdout=f, check=True)
        except subprocess.CalledProcessError as e:
            raise AirflowException(f"mysqldump failed: {e}")

    def insert_many(self, table_name: str, rows: List[Any], columns: Optional[List[str]] = None, schema: Optional[str] = None) -> bool:
        """
        Bulk insert rows into a MariaDB table using executemany.

        Args:
            table_name: Target table name
            rows: List of row tuples or dicts to insert
            columns: List of column names (optional)
            schema: Database schema name (optional)

        Returns:
            bool: True if insert was successful
        """
        if not rows:
            self.log.warning("No rows provided for insert_many.")
            return False
        if not schema:
            conn = self.get_connection(getattr(self, self.conn_name_attr))
            schema = conn.schema
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        if columns:
            cols = ', '.join([f'`{col}`' for col in columns])
            placeholders = ', '.join(['%s'] * len(columns))
            sql = f"INSERT INTO {full_table_name} ({cols}) VALUES ({placeholders})"
        else:
            placeholders = ', '.join(['%s'] * len(rows[0]))
            sql = f"INSERT INTO {full_table_name} VALUES ({placeholders})"
        self.log.info(f"Executing bulk insert: {sql} with {len(rows)} rows")
        try:
            with self.get_conn() as conn:
                cursor = conn.cursor()
                cursor.executemany(sql, rows)
                conn.commit()
            self.log.info(f"Successfully inserted {len(rows)} rows into {full_table_name}")
            return True
        except mariadb.Error as e:
            self.log.error(f"Error in insert_many: {e}")
            raise AirflowException(f"Failed to insert rows: {e}")
