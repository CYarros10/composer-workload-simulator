

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
    dag_id="experiment_1_dag_2",
    description="This DAG was auto-generated for experimentation purposes.",
    schedule="30 * * * *",
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
    # Default BashOperator Taskflow 
    # -------------------------------------------------

    task_0 = BashOperator(
        task_id="bash_task_0",
        bash_command="echo 'Hello from BashOperator'",
    )
    
    # -------------------------------------------------
    # Default DataprocBatchOperator Taskflow 
    # -------------------------------------------------    
    
    batch_id = "experiment_1_dag_2-1-batch".replace("_", "-")
    batch_config = {
        "spark_batch": {
            "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
            "main_class": "org.apache.spark.examples.SparkPi",
        }
    }
    task_1 = DataprocCreateBatchOperator(
        task_id="create_batch_1",
        project_id='cy-artifacts',
        region='us-central1',
        batch_id=batch_id,
        batch=batch_config,
    )
    
    
    # -------------------------------------------------
    # Default EmptyOperator Taskflow 
    # -------------------------------------------------

    task_2 = EmptyOperator(
        task_id=f"empty_task_2",
    )
    
    task_0 >> task_1 >> task_2
