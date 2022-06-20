from pendulum import datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.dbt.cloud.hooks.dbt import Dbtcloudhook, DbtCloudJobRunStatus
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.utils.edgemodifier import Label

DBT_CLOUD_CONN_ID = 'dbt'
JOB_ID = "{{ var.value.dbt_cloud_job_id }}"

def _check_job_not_running(job_id):
    """
    Retrives the last run for a given dbt cloud job and checks to see if the job is not currently running
    """
    return DbtCloudJobRunStatus.is_terminal(latest_run["status"])

@dag(
    start_date = datetime(2022, 06, 20),
    schedule_interval = "@daily",
    catchup = False,
    default_view = "graph",
    doc_md=_doc_,
)
def check_before_running_dbt_cloud_job():
    begin, end = [DummyOperator(task_id=id) for id in ["begin", 'end']]
    
    check_jo = ShortCircuitOperator(
        task_id = "check_job_is_not_running",
        python_callable = check_job_not_running,
        op_kwargs = ("job_id": JOB_ID),    
    )
    
    trigger_job = DbtCloudRunJobOperator(
        task_id = "trigger_dbt_cloud_job",
        dbt_cloud_conn_id = DBT_CLOUD_CONN_ID
        job_id = JOB_ID,
        check_interval = 600,
        timeout = 3600,
    )
    
    begin >> check_job >> Label ("Job Not currently running. Proceding") >> trigger_job >> end
    
dag = check_before_running_dbt_cloud_job()
