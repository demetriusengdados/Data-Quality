from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.sagemaker_training import SageMakerTrainingOperator
from airflow.providers.amazon.aws.operators.sagemaker_model import SageMakerModelOperator
from airflow.providers.amazon.aws.operators.sagemaker_transform import SageMakerTransformOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime, timedelta
import requests
import io
import pandas as pd
import numpy as np

"""
This DAG shows an example implementation of machine learning model orchestration using Airflow
and AWS SageMaker. Using the AWS provider's SageMaker operators, Airflow orchestrates getting data
from an API endpoint and pre-processing it (PythonOperator), training the model (SageMakerTrainingOperator),
creating the model with the training results (SageMakerModelOperator), and testing the model using
a batch transform job (SageMakerTransformOperator).

The example use case shown here is using a built-in SageMaker K-nearest neighbors algorithm to make
predictions on the Iris dataset. To use the DAG, fill in the information directly below with the target
AWS S3 locations, execution role ARN, and model and training job names.
"""

# Define variables used in configs
data_url = "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"   # URL for Iris data API
date = '{{ ds_nodash }}'                                                                # Date for transform job name
s3_bucket = 'sagemaker-us-east-2-559345414282'                                          # S3 Bucket used with SageMaker instance
input_s3_key = 'iris/processed-input-data'                                              # Train and test data S3 path
output_s3_key = 'iris/results'                                                          # S3 path for output data
role = 'your-role-arn'                                                                  # Role ARN to execute SageMaker jobs
model_name = "Iris-KNN"                                                                # Name of model to create
training_job_name = 'train-iris'                                                        # Name of training job

# Define configs for training, model creation, and batch transform jobs
training_config = {
   "AlgorithmSpecification": { 
      "TrainingImage": "404615174143.dkr.ecr.us-east-2.amazonaws.com/knn",
      "TrainingInputMode": "File"
   },
    "HyperParameters": { 
      "predictor_type": "classifier",
      "feature_dim": "4",
      "k": "3",
      "sample_size": "150"
   },
   "InputDataConfig": [ 
      {"ChannelName": "train",
        "DataSource": { 
            "S3DataSource": { 
               "S3DataType": "S3Prefix",
               "S3Uri": "s3://{0}/{1}/train.csv".format(s3_bucket, input_s3_key)
            }
         },
         "ContentType": "text/csv",
         "InputMode": "File"
      }
   ],
   "OutputDataConfig": { 
      "S3OutputPath": "s3://{0}/{1}/results.csv".format(s3_bucket, output_s3_key)
   },
   "ResourceConfig": { 
      "InstanceCount": 1,
      "InstanceType": "ml.m5.large",
      "VolumeSizeInGB": 1
   },
   "RoleArn": role,
   "StoppingCondition": { 
      "MaxRuntimeInSeconds": 6000
   },
   "TrainingJobName": training_job_name
}

model_config = {
   "ExecutionRoleArn": role,
   "ModelName": model_name,
   "PrimaryContainer": { 
      "Mode": "SingleModel",
      "Image": "404615174143.dkr.ecr.us-east-2.amazonaws.com/knn",
      "ModelDataUrl": "s3://{0}/{1}/{2}/output/model.tar.gz".format(s3_bucket, output_s3_key, training_job_name),
   },
}

transform_config = {
    "TransformJobName": "test-knn-{0}".format(date),
    "TransformInput": { 
        "DataSource": { 
            "S3DataSource": {
                "S3DataType":"S3Prefix", 
                "S3Uri": "s3://{0}/{1}/test.csv".format(s3_bucket, input_s3_key)
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
    "ModelName": model_name
    }

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

with DAG('sagemaker_pipeline',
         start_date=datetime(2021, 7, 31),
         max_active_runs=1,
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False
         ) as dag:

    @task
    def data_prep(data_url, s3_bucket, input_s3_key):
        """
        Grabs the Iris dataset from API, splits into train/test splits, and saves CSV's to S3 using S3 Hook
        """
        # Get data from API
        iris_response = requests.get(data_url).content
        columns = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'species']
        iris = pd.read_csv(io.StringIO(iris_response.decode('utf-8')), names=columns)

        # Process data
        iris['species'] = iris['species'].replace({'Iris-virginica': 0, 'Iris-versicolor': 1, 'Iris-setosa': 2})
        iris = iris[['species', 'sepal_length', 'sepal_width', 'petal_length', 'petal_width']]
        
        # Split into test and train data
        iris_train, iris_test = np.split(iris.sample(frac=1, random_state=np.random.RandomState()), [int(0.7 * len(iris))])
        iris_test.drop(['species'], axis=1, inplace=True)

        # Save files to S3
        iris_train.to_csv('iris_train.csv', index=False, header=False)
        iris_test.to_csv('iris_test.csv', index=False, header=False)
        s3_hook = S3Hook(aws_conn_id='aws-sagemaker')
        s3_hook.load_file('iris_train.csv', '{0}/train.csv'.format(input_s3_key), bucket_name=s3_bucket, replace=True)
        s3_hook.load_file('iris_test.csv', '{0}/test.csv'.format(input_s3_key), bucket_name=s3_bucket, replace=True)

    data_prep = data_prep(data_url, s3_bucket, input_s3_key)

    train_model = SageMakerTrainingOperator(
        task_id='train_model',
        config=training_config,
        aws_conn_id='aws-sagemaker',
        wait_for_completion=True
    )

    create_model = SageMakerModelOperator(
        task_id='create_model',
        config=model_config,
        aws_conn_id='aws-sagemaker'
    )

    test_model = SageMakerTransformOperator(
        task_id='test_model',
        config=transform_config,
        aws_conn_id='aws-sagemaker'
    )

    data_prep >> train_model >> create_model >> test_model