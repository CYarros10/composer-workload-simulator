

# -------------------------------------------------
# Base Taskflow Imports 
# -------------------------------------------------

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import (
    PythonOperator,
    BranchPythonOperator,
)
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# -------------------------------------------------
# Google Cloud Taskflow Imports 
# -------------------------------------------------

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import (
    PythonOperator,
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    DataprocCreateBatchOperator,
)
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
from airflow.providers.apache.beam.operators.beam import BeamRunJavaPipelineOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import (
    GCSToGCSOperator
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryInsertJobOperator
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKECreateClusterOperator,
    GKEDeleteClusterOperator,
    GKEStartPodOperator,
)


# -------------------------------------------------
# Begin DAG
# -------------------------------------------------

with DAG(
    dag_id="experiment_1_dag_3",
    description="This DAG was auto-generated for experimentation purposes.",
    schedule="10 * * * *",
    default_args={
        "retries": 1,
        "execution_timeout": timedelta(minutes=30),
        "sla": timedelta(minutes=25),
        "deferrable": True,
    },
    start_date=datetime.strptime("9/19/2024", "%m/%d/%Y"),
    catchup=False,
    is_paused_upon_creation=False,
    tags=['load_simulation']
) as dag:


    # -------------------------------------------------
    # Default PythonBranchOperator Taskflow 
    # -------------------------------------------------

    def choose_branch(**kwargs):
        execution_date = kwargs['execution_date']
        if execution_date.day % 2 == 0:
            return 'even_day_task_0'
        else:
            return 'odd_day_task_0'

    # Define the BranchPythonOperator
    task_0 = BranchPythonOperator(
        task_id='branch_task_0',
        python_callable=choose_branch,
        provide_context=True,
    )

    # Define tasks for each branch
    even_day_task_0  = EmptyOperator(task_id='even_day_task_0')
    odd_day_task_0  = EmptyOperator(task_id='odd_day_task_0')

    # Define task dependencies
    task_0  >> even_day_task_0
    task_0  >> odd_day_task_0
    
    # -------------------------------------------------
    # Default BashOperator Taskflow 
    # -------------------------------------------------

    task_1 = BashOperator(
        task_id="bash_task_1",
        bash_command="echo 'Hello from BashOperator'",
    )
    
    # -------------------------------------------------
    # Default KubernetesPodOperator Taskflow 
    # -------------------------------------------------

    task_2 = KubernetesPodOperator(
        task_id="kubernetes_task_2",
        name="pod-ex-minimum",
        cmds=["echo"],
        namespace="composer-user-workloads",
        image="gcr.io/gcp-runtimes/ubuntu_20_0_4",
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id="kubernetes_default",
    )
    
    task_0 >> task_1 >> task_2
