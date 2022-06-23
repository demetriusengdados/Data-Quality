from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.sagemaker_transform import SageMakerTransformOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime, timedelta

"""
This DAG shows an example implementation of executing predictions from a machine learning model using AWS SageMaker.
The DAG assumes that a SageMaker model has already been created, and runs the one-time batch inference job
using SageMaker batch transform. This method is useful if you don't have a hosted model endpoint and want
to run ad-hoc predictions when data becomes available.

The DAG uploads a local dataset from the /include directory to S3, then submits a Batch Transform job to SageMaker
to get model inference on that data. The inference results are saved to the S3 Output Path given in the config.
Finally, the inference results are uploaded to a table in Redshift using the S3 to Redshift transfer operator.
"""

# Define variables used in config and Python function
date = '{{ ds_nodash }}'                                                     # Date for transform job name
s3_bucket = 'sagemaker-us-east-2-559345414282'                               # S3 Bucket used with SageMaker 
test_s3_key = 'demo-sagemaker-xgboost-adult-income-prediction/test/test.csv' # Test data S3 key
output_s3_key = 'demo-sagemaker-xgboost-adult-income-prediction/output/'     # Model output data S3 key
sagemaker_model_name = "sagemaker-xgboost-2021-08-03-23-25-30-873"           # SageMaker model name

# Define transform config for the SageMakerTransformOperator
transform_config = {
        "TransformJobName": "test-sagemaker-job-{0}".format(date),
        "TransformInput": { 
            "DataSource": { 
                "S3DataSource": {
                    "S3DataType":"S3Prefix", 
                    "S3Uri": "s3://{0}/{1}".format(s3_bucket, test_s3_key)
                }
            },
            "SplitType": "Line",
            "ContentType": "text/csv",
        },
        "TransformOutput": { 
            "S3OutputPath": "s3://{0}/{1}".format(s3_bucket, output_s3_key)
        },
        "TransformResources": { 
            "InstanceCount": 1,
            "InstanceType": "ml.m5.large"
        },
        "ModelName": sagemaker_model_name
    }

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG('sagemaker_model',
        start_date=datetime(2021, 7, 31),
        max_active_runs=1,
        schedule_interval='@daily',
        default_args=default_args,
        catchup=False
    ) as dag:

    @task
    def upload_data_to_s3(s3_bucket, test_s3_key):
        """
        Uploads validation data to S3 from /include/data 
        """
        s3_hook = S3Hook(aws_conn_id='aws-sagemaker')

        # Take string, upload to S3 using predefined method
        s3_hook.load_file(filename='include/data/test.csv', 
                        key=test_s3_key, 
                        bucket_name=s3_bucket, 
                        replace=True)

    upload_data = upload_data_to_s3(s3_bucket, test_s3_key)

    predict = SageMakerTransformOperator(
        task_id='predict',
        config=transform_config,
        aws_conn_id='aws-sagemaker'
    )

    results_to_redshift = S3ToRedshiftOperator(
            task_id='save_results',
            aws_conn_id='aws-sagemaker',
            s3_bucket=s3_bucket,
            s3_key=output_s3_key,
            schema="PUBLIC",
            table="results",
            copy_options=['csv'],
        )

    upload_data >> predict >> results_to_redshift