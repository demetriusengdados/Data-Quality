from fileinput import filename
import requests
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import time
import requests

def api_function(**kwargs):
    url = 'https://covidtracking.com/api/v1/states'
    filename = '{0}{1}.csv'.format('wa', '2022-06-20')
    res = requests.get(url+filename)
    
with DAG('pool_unimportant_dag',
        start_date = datetime(2022, 06, 20),
        schedule_interval = timedelta(minutes=30),
        catchup=False,
        default_args={
            'retries': 1
            'retry_delay': timedelta(minutes=5)
        }
        ) as DAG:
    
    task_w = DummyOperator(
        task_id='start'
    )
    
    task_x = PythonOperator(
        task_id = 'task_x',
        python_callable = api_function,
        pool = 'api_pool',
        priority_weight = 2
    )
    
    task_y = PythonOperator(
        task_id = 'task_y',
        python_callable = 'api_function',
        pool = 'api_pool'
    )
    
    task_z = DummyOperator(
        task_id = 'end'
    )
    
    task_w >> [task_x, task_y] >> task_z