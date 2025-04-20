from typing import Any, Dict, List, Optional, Union

from airflow.models import BaseOperator
from airflow.providers.trino.hooks.trino import TrinoHook


class TrinoOperator(BaseOperator):
    """
    Executes SQL code in a Trino database

    :param sql: the SQL code to be executed. (templated)
    :param trino_conn_id: reference to a specific Trino database
    :param parameters: (optional) the parameters to render the SQL query with.
    :param handler: (optional) the function that will be applied to the cursor (if any)
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    template_fields_renderers = {'sql': 'sql'}
    ui_color = '#ededed'

    def __init__(
        self,
        *,
        sql: Union[str, List[str]],
        trino_conn_id: str = 'trino_default',
        parameters: Optional[Union[Dict, List]] = None,
        handler: Optional[Any] = None,
        autocommit: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.trino_conn_id = trino_conn_id
        self.parameters = parameters
        self.handler = handler
        self.autocommit = autocommit

    def execute(self, context: Dict[str, Any]) -> Any:
        """
        Executes the SQL code in the Trino database
        """
        self.log.info('Executing: %s', self.sql)
        hook = TrinoHook(trino_conn_id=self.trino_conn_id)
        
        if isinstance(self.sql, str):
            result = hook.get_records(self.sql, self.parameters)
        else:
            result = []
            for query in self.sql:
                result.extend(hook.get_records(query, self.parameters))

        if self.handler is not None:
            result = self.handler(result)

        return result