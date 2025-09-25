import mariadb
from airflow.hooks.dbapi import DbApiHook

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
