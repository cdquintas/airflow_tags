from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.providers.trino.hooks.trino import TrinoHook
import logging


class SEPDBOperator(BaseOperator):

    @apply_defaults

    def __init__(self, trino_conn_id: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.trino_conn_id = trino_conn_id
    
    def execute(self, context):

        # Set up the hook object
        hook = TrinoHook(trino_conn_id=self.trino_conn_id)
        message = str(result)
        logging.info('Start Message if you want')
        ###
        ### Change this SQL to something valid in your environment
        ###
        sql = "select * from tpch.sf1.customer where id in ( ? )"
        ###
        ### This is the parameter that will be substituted
        ### in the SQL above.
        ### If you dont want to parameterise . the do not include ?
        ### in the SQL statement. Do not set this and do not pass
        ### parameters in the hook call below.
        prm=[100]
        ###
        ### Dont use hook.run - it does not execute the statement
        ### If you are not using parameters dont pass them
        ###
        result = hook.get_first(sql,parameters=prm)
        logging.info('End Message if you want')
        print(message)
        return 0
