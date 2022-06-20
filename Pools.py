from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import time
import requests
from multiprocessing import Pool, pool
from time import sleep


task_a = PythonOperator(
    task_id="task_a",
    python_callable = (sleep_function),
    pool = "single_task_pool"
    # op_kwargs: Optional[Dict] = None,
    # op_args: Optional[List] = None,
    # templates_dict: Optional[Dict] = None
    # templates_exts: Optional[List] = None
)

with DAG('pool_dag',
    start_date= datetime(2022-06-20),
    schedule__interval = timedelta(minutes=30),
    catchup = False,
    default_args = default_args
) as dag:
    
    task_a = PythonOperator(
        task_id="task_a",
        python_callable=(sleep_function),
        pool = "single_task_pool"
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )
    
    task_b = python_task = PythonOperator(
        task_id="python_b",
        python_callable=sleep_function,
        pool='single_task_pool',
        priority_weigth = 2
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )
    
    
def api_function(**kwargs):
    url = "https://covidtracking.com/api/v1/states"
    filename = "{0}{1}.csv".format("wa", '2022-06-20')
    res = requests.get(url+filename)
        
with DAG('pool_priority_dag',
        start_date = datetime(2022-06-20),
        schedule_interval = timedelta(minutes=30),
        catchup=False,
        default_args={
            'pool': 'api_pool',
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            'priority_weight': 3
}
) as dag:
    task_a = PythonOperator(
        task_id = "task_a",
        python_callable = api_function
)
    
task_b = PythonOperator(
    task_id = 'task_b',
    python_callable = api_function
)

task_c = PythonOperator(
    task_id = 'task_c',
    python_callable = api_function
)
