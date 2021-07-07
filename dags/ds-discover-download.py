from datetime import timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from tasks.discover import discover_ids
from tasks.download import download_metadata

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
with DAG(
    'datastore-discovery-download',
    default_args=default_args,
    description='OpenSearch discover + Metadata Download',
    schedule_interval=None,
    start_date=days_ago(0),
    params={
        "target_dir": "/tmp/storage",
        "collection_id": "EO:EUM:DAT:MSG:CLM"
    },
    tags=['data-store'],
) as dag:
    # step 1
    ids_task = discover_ids()

    # step 3
    join = DummyOperator(
        task_id='join',
        trigger_rule='none_failed_or_skipped',
    )

    # step 4
    list_files = BashOperator(
        task_id='list_files',
        bash_command="echo 'contents of directory {{ params.directory }}:' && ls -la {{ params.directory }}",
        params = {'directory' : dag.params["target_dir"]},
        dag=dag
    )
    
    # step 2
    # Generate n parallel tasks
    n=3
    parallel_tasks = []
    for i in range(n):
        dm_task = download_metadata(ids_task, i, n)
        
        ids_task >> dm_task >> join >> list_files
    
    
    