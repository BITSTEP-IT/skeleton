from typing import Any, Dict, List, Optional, Union

from airflow.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CustomPostgresOperator(BaseOperator):
    """
    Custom operator to execute PostgreSQL queries using psycopg2 via PostgresHook.
    
    :param sql: SQL query or list of SQL queries to execute
    :param postgres_conn_id: reference to a specific postgres database
    :param parameters: parameters to pass to the SQL query
    :param autocommit: if True, each command is automatically committed
    :param database: name of database to use (overrides the one defined in the connection)
    """
    
    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
        self,
        *,
        sql: Union[str, List[str]],
        postgres_conn_id: str = 'postgres_default',
        parameters: Optional[Union[Dict[str, Any], List[Any]]] = None,
        autocommit: bool = False,
        database: Optional[str] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.parameters = parameters
        self.autocommit = autocommit
        self.database = database
        self.hook = None

    def execute(self, context: Dict[str, Any]) -> None:
        """
        Execute the PostgreSQL query using PostgresHook.
        """
        self.hook = PostgresHook(
            postgres_conn_id=self.postgres_conn_id,
            schema=self.database
        )

        if isinstance(self.sql, str):
            sql_statements = [self.sql]
        else:
            sql_statements = self.sql

        for sql_statement in sql_statements:
            self.log.info('Executing SQL: %s', sql_statement)
            
            if self.parameters:
                self.hook.run(
                    sql_statement,
                    parameters=self.parameters,
                    autocommit=self.autocommit
                )
            else:
                self.hook.run(
                    sql_statement,
                    autocommit=self.autocommit
                )
