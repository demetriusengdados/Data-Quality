from airflow import DAG
from airflow.providers.microsoft.azure.operators.adx import AzureDataExplorerQueryOperator
from datetime import datetime, timedelta

adx_query = '''StormEvents
| sort by StartTime desc
| take 10'''

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

with DAG('azure_data_explorer',
        start_date=datetime(2020, 12, 1),
        max_active_runs=1,
        schedule_interval='@daily',
        default_args=default_args,
        catchup=False
        ) as dag:

    opr_adx_query = AzureDataExplorerQueryOperator(
        task_id='adx_query',
        query=adx_query,
        database='storm_demo',
        azure_data_explorer_conn_id='adx'
    )