import datetime as dt
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.utils.dates import days_ago
from example_plugin_trino import SEPDBOperator

with DAG( dag_id='SEP01', schedule_interval=dt.timedelta(hours=4), start_date=days_ago(2), tags=['example2', 'example3']) as dag:

tr_task = SEPDBOperator(task_id='db_task', trino_conn_id='trino_local_conn_id')
task1 = DummyOperator(task_id='task1')
task1 >> db_task
