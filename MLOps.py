from airflow.decorators import task, dag, task_group
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from datetime import datetime

import logging
import mlflow

import pandas as pd

from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.linear_model import LogisticRegression
import lightgbm as lgb

import include.metrics as metrics
from include.grid_configs import models, params


mlflow.set_tracking_uri('http://host.docker.internal:5000')
try:
   # Creating an experiment 
   mlflow.create_experiment('census_prediction')
except:
   pass
# Setting the environment with the created experiment
mlflow.set_experiment('census_prediction')

mlflow.sklearn.autolog()
mlflow.lightgbm.autolog()

@dag(
   start_date=datetime(2022, 1, 1),
   schedule_interval=None,
   catchup=False
)
def mlflow_multimodel_example():

   @task
   def load_data():
       """Pull Census data from Public BigQuery and save as Pandas dataframe in GCS bucket with XCom"""

       bq = BigQueryHook()
       sql = """
       SELECT * FROM `bigquery-public-data.ml_datasets.census_adult_income`
       """

       return bq.get_pandas_df(sql=sql, dialect='standard')


   @task
   def preprocessing(df: pd.DataFrame):
       """Clean Data and prepare for feature engineering
       
       Returns pandas dataframe via Xcom to GCS bucket.

       Keyword arguments:
       df -- Raw data pulled from BigQuery to be processed. 
       """

       df.dropna(inplace=True)
       df.drop_duplicates(inplace=True)

       # Clean Categorical Variables (strings)
       cols = df.columns
       for col in cols:
           if df.dtypes[col]=='object':
               df[col] =df[col].apply(lambda x: x.rstrip().lstrip())


       # Rename up '?' values as 'Unknown'
       df['workclass'] = df['workclass'].apply(lambda x: 'Unknown' if x == '?' else x)
       df['occupation'] = df['occupation'].apply(lambda x: 'Unknown' if x == '?' else x)
       df['native_country'] = df['native_country'].apply(lambda x: 'Unknown' if x == '?' else x)


       # Drop Extra/Unused Columns
       df.drop(columns=['education_num', 'relationship', 'functional_weight'], inplace=True)

       return df


   @task
   def feature_engineering(df: pd.DataFrame):
       """Feature engineering step
       
       Returns pandas dataframe via XCom to GCS bucket.

       Keyword arguments:
       df -- data from previous step pulled from BigQuery to be processed. 
       """
       # Onehot encoding 
       df = pd.get_dummies(df, prefix='workclass', columns=['workclass'])
       df = pd.get_dummies(df, prefix='education', columns=['education'])
       df = pd.get_dummies(df, prefix='occupation', columns=['occupation'])
       df = pd.get_dummies(df, prefix='race', columns=['race'])
       df = pd.get_dummies(df, prefix='sex', columns=['sex'])
       df = pd.get_dummies(df, prefix='income_bracket', columns=['income_bracket'])
       df = pd.get_dummies(df, prefix='native_country', columns=['native_country'])

       # Bin Ages
       df['age_bins'] = pd.cut(x=df['age'], bins=[16,29,39,49,59,100], labels=[1, 2, 3, 4, 5])

       # Dependent Variable
       df['never_married'] = df['marital_status'].apply(lambda x: 1 if x == 'Never-married' else 0) 

       # Drop redundant column
       df.drop(columns=['income_bracket_<=50K', 'marital_status', 'age'], inplace=True)

       return df

   @task
   def get_models():
       """
       Returns list of models to train from by reading a file in the include/ directory.
       We assume this file has two parameters for each model entry: model, and params
       """
       return [models]

   @task()
   def train(df: pd.DataFrame, model_type=models, model=models[model], grid_params=models[params], **kwargs):
       """Train and validate model using a grid search for the optimal parameter values and a five fold cross validation.

       Returns accuracy score via XCom to GCS bucket.

       Keyword arguments:
       df -- data from previous step pulled from BigQuery to be processed. 
       """
       y = df['never_married']
       X = df.drop(columns=['never_married'])

       X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=55, stratify=y)

       grid_search = GridSearchCV(model, param_grid=grid_params, verbose=1, cv=5, n_jobs=-1)

       with mlflow.start_run(run_name=f'{model_type}_{kwargs["run_id"]}'):

           logging.info('Performing Gridsearch')
           grid_search.fit(X_train, y_train)

           logging.info(f'Best Parameters\n{grid_search.best_params_}')
           best_params = grid_search.best_params_

           if model_type == 'lgbm':

               train_set = lgb.Dataset(X_train, label=y_train)
               test_set = lgb.Dataset(X_test, label=y_test)

               best_params['metric'] = ['auc', 'binary_logloss']

               logging.info('Training model with best parameters')
               clf = lgb.train(
                   train_set=train_set,
                   valid_sets=[train_set, test_set],
                   valid_names=['train', 'validation'],
                   params=best_params,
                   early_stopping_rounds=5
               )

           else:
               logging.info('Training model with best parameters')
               clf = LogisticRegression(penalty=best_params['penalty'], C=best_params['C'], solver=best_params['solver']).fit(X_train, y_train)

           y_pred_class = metrics.test(clf, X_test)

           # Log Classification Report, Confusion Matrix, and ROC Curve
           metrics.log_all_eval_metrics(y_test, y_pred_class)

   df = load_data()
   clean_data = preprocessing(df)
   features = feature_engineering(clean_data)
   train_modes = train.partial(features).expand(get_models())
   
dag = mlflow_multimodel_example()