import pytest
from airflow.models import DagBag

def test_no_import_errors():
    dag_bag = DagBag()
    assert len(dag_bag.import_errors) == 0, "No Import Failures"
    
def test_retries_present():
    dag_bag = DagBag()
    for dag in dag_bag.dags:
        retries = dag_bag.dags[dag].default_args.get('retries', [])
        error_msg = 'Retries not set to 2 for DAG {id}'.format(id=dag)
        assert retries == 2, error_msg
        
def test_function_returns_5():
	assert my_function(input) == 5

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class EvenNumberCheckOperator(BaseOperator):
    @apply_defaults
    def __init__(self, my_operator_param, *args, **kwargs):
        self.operator_param = my_operator_param
        super(EvenNumberCheckOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
		if self.operator_param % 2:
			return True
        else:
			return False

import unittest
import pytest
from datetime import datetime
from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators import EvenNumberCheckOperator

DEFAULT_DATE = datetime(2021, 1, 1)

class EvenNumberCheckOperator(unittest.TestCase):

	def setUp(self):
        super().setUp()
        self.dag = DAG('test_dag', default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE})
			self.even = 10
			self.odd = 11

	def test_even(self):
        """Tests that the EvenNumberCheckOperator returns True for 10."""
        task = EvenNumberCheckOperator(my_operator_param=self.even, task_id='even', dag=self.dag)
        ti = TaskInstance(task=task, execution_date=datetime.now())
        result = task.execute(ti.get_template_context())
        assert result is True

	def test_odd(self):
        """Tests that the EvenNumberCheckOperator returns False for 11."""
        task = EvenNumberCheckOperator(my_operator_param=self.odd, task_id='odd', dag=self.dag)
        ti = TaskInstance(task=task, execution_date=datetime.now())
        result = task.execute(ti.get_template_context())
        assert result is False
        
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.azure.hooks.azure_data_factory import AzureDataFactoryHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

#Get yesterday's date, in the correct format
yesterday_date = '{{ yesterday_ds_nodash }}'

#Define Great Expectations file paths
data_dir = '/usr/local/airflow/include/data/'
data_file_path = '/usr/local/airflow/include/data/'
ge_root_dir = '/usr/local/airflow/include/great_expectations'

def run_adf_pipeline(pipeline_name, date):
    '''Runs an Azure Data Factory pipeline using the AzureDataFactoryHook and passes in a date parameter
    '''
    
    #Create a dictionary with date parameter 
    params = {}
    params["date"] = date

    #Make connection to ADF, and run pipeline with parameter
    hook = AzureDataFactoryHook('azure_data_factory_conn')
    hook.run_pipeline(pipeline_name, parameters=params)

def get_azure_blob_files(blobname, output_filename):
    '''Downloads file from Azure blob storage
    '''
    azure = WasbHook(wasb_conn_id='azure_blob')
    azure.get_file(output_filename, container_name='covid-data', blob_name=blobname)
    

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

with DAG('adf_great_expectations',
        start_date=datetime(2021, 1, 1),
        max_active_runs=1,
        schedule_interval='@daily', 
        default_args=default_args,
        catchup=False
        ) as dag:

        run_pipeline = PythonOperator(
            task_id='run_pipeline',
            python_callable=run_adf_pipeline,
            op_kwargs={'pipeline_name': 'pipeline1', 'date': yesterday_date}
        )

        download_data = PythonOperator(
            task_id='download_data',
            python_callable=get_azure_blob_files,
            op_kwargs={'blobname': 'or/'+ yesterday_date +'.csv', 'output_filename': data_file_path+'or_'+yesterday_date+'.csv'}
        )

        ge_check = GreatExpectationsOperator(
            task_id='ge_checkpoint',
            expectation_suite_name='azure.demo',
            batch_kwargs={
                'path': data_file_path+'or_'+yesterday_date+'.csv',
                'datasource': 'data__dir'
            },
            data_context_root_dir=ge_root_dir
        )

        send_email = EmailOperator(
            task_id='send_email',
            to='noreply@astronomer.io',
            subject='Covid to S3 DAG',
            html_content='<p>The great expectations checks passed successfully. <p>'
        )

        run_pipeline >> download_data >> ge_check >> send_email