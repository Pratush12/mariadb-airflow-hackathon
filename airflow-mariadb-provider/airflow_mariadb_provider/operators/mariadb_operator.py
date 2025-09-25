from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context

class MariaDBOperator(SQLExecuteQueryOperator):
    """
    Executes sql code against a MariaDB database.
    """

    template_fields: Sequence[str] = ('sql', 'params')
    template_ext: Sequence[str] = ('.sql', '.json')
    template_fields_renderers = {"sql": "sql"}
    ui_color = '#e0f2f7'

    def __init__(
        self,
        *,
        mariadb_conn_id: str = "mariadb_default",
        **kwargs,
    ):
        super().__init__(
            conn_id=mariadb_conn_id,
            **kwargs,
        )

    def execute(self, context: Context):
        self.log.info(f"Executing SQL query against MariaDB with conn_id: {self.conn_id}")
        super().execute(context)
