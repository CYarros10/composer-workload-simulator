import random
import yaml
from pathlib import Path

from taskflow_collections.base_taskflows import BaseTaskFlows
from taskflow_collections.google_cloud_taskflows import GoogleCloudTaskFlows


def create_dag_string(
    dag_id: str,
    start_date: str,
    schedule: str,
    default_settings: dict,
    taskflow_collections: list,
    taskflows: dict,
    num_tasks: int,
):
    """Generates a string representation of an Airflow DAG with random taskflows."""

    dag_string = ""

    for taskflow_collection in taskflow_collections:
        if taskflow_collection == "base":
            dag_string += BaseTaskFlows.add_imports()
        elif taskflow_collection == "google_cloud":
            dag_string += GoogleCloudTaskFlows.add_imports()

    dag_string += f"""

# -------------------------------------------------
# Begin DAG
# -------------------------------------------------

with DAG(
    dag_id="{dag_id}",
    description="This DAG was auto-generated for experimentation purposes.",
    schedule="{schedule}",
    default_args={{
        "retries": {default_settings['retries']},
        "execution_timeout": timedelta(minutes={default_settings['execution_timeout']}),
        "sla": timedelta(minutes={default_settings['sla']}),
        "deferrable": {default_settings['deferrable']},
    }},
    start_date=datetime.strptime("{start_date}", "%m/%d/%Y"),
    catchup={default_settings['catchup']},
    is_paused_upon_creation={default_settings['is_paused_upon_creation']},
    tags=['load_simulation']
) as dag:

"""

    dag_string += generate_tasks(
        taskflows=taskflows, # merge all taskflows into a single dictionary of taskflows and weights.
        num_tasks=num_tasks,
        dag_id=dag_id,
        project_id=default_settings["project_id"],
        region=default_settings["region"],
    )

    # Set up dependencies
    dependencies = " >> ".join([f"task_{task_id}" for task_id in range(num_tasks)])
    dag_string += f"""
    {dependencies}
"""

    return dag_string


def generate_tasks(
    taskflows: dict, dag_id: str, project_id: str, region: str, num_tasks: int
):
    """Generates task definitions for various taskflows and returns as a string."""

    base = BaseTaskFlows(dag_id=dag_id)
    google_cloud = GoogleCloudTaskFlows(dag_id=dag_id, region=region, project_id=project_id)

    tasks_string = ""

    for task_number in range(num_tasks):

        taskflow_name = random.choices(
            list(taskflows.keys()), weights=list(taskflows.values())
        )[0]

        if taskflow_name == "PythonOperator":
            tasks_string += base.pythonoperator_taskflow(task_id=task_number)

        elif taskflow_name == "KubernetesPodOperator":
            tasks_string += base.kubernetespodoperator_taskflow(task_id=task_number)

        elif taskflow_name == "BashOperator":
            tasks_string += base.bashoperator_taskflow(task_id=task_number)

        elif taskflow_name == "BranchPythonOperator":
            tasks_string += base.pythonbranchoperator_taskflow(task_id=task_number,)

        elif taskflow_name == "EmptyOperator":
            tasks_string += base.emptyoperator_taskflow(task_id=task_number)

        elif taskflow_name == "BigQueryInsertJobOperator":
            tasks_string += google_cloud.bigqueryinsertjoboperator_taskflow(task_id=task_number)

        elif taskflow_name == "DataprocSubmitJobOperator":
            tasks_string += google_cloud.dataprocsubmitjoboperator_taskflow(task_id=task_number)

        elif taskflow_name == "BeamRunJavaPipelineOperator":
            tasks_string += google_cloud.beamrunjavapipelineoperator_taskflow(task_id=task_number)

        elif taskflow_name == "DataprocCreateBatchOperator":
            tasks_string += google_cloud.dataprocbatchoperator_taskflow(task_id=task_number)

        elif taskflow_name == "GCSToGCSOperator":
            tasks_string += google_cloud.gcstogcsoperator_taskflow(task_id=task_number)

        elif taskflow_name == "GCSToBigQueryOperator":
            tasks_string += google_cloud.gcstobigqueryoperator_taskflow(task_id=task_number)

        elif taskflow_name == "GKEStartPodOperator":
            tasks_string += google_cloud.gkestartpodoperator_taskflow(task_id=task_number)

        else:
            raise ValueError(f"Unsupported operator: {taskflow_name}")

    return tasks_string


def load_config_from_file(filepath):
    """
    Load YAML file into dictionary.
    """
    load_config = {}
    try:
        with open(filepath, "r") as f:
            load_config = yaml.safe_load(f)
    except FileNotFoundError:
        print("Error: config.yaml not found.")
    return load_config


def main():
    """
    Reads configuration, generates DAGs, and writes them to files.
    """
    # Load configuration (assuming you have a function to load it)
    load_config = load_config_from_file(
        "configs/sample.yaml"
    )  # Replace with your loading logic

    # merge taskflow collections into single map of taskflows and weights
    taskflows = {}
    taskflow_collections = []
    for key in load_config["taskflows"]:
        taskflow_collections.append(key)
        nested_dict = load_config["taskflows"][key]
        taskflows.update(nested_dict)
    
    print(taskflows)

    # Generate DAGs
    for i in range(load_config["number_of_dags"]):
        experiment_id = load_config["experiment_id"]
        dag_id = f"{experiment_id}_dag_{i}"
        schedule = random.choices(
            list(load_config["schedules"].keys()),
            weights=list(load_config["schedules"].values()),
        )[0]
        start_date = random.choices(
            list(load_config["start_dates"].keys()),
            weights=list(load_config["start_dates"].values()),
        )[0]
        default_settings = load_config["default_settings"].copy()
        default_settings["owner"] = "airflow"


        dag = create_dag_string(
            dag_id=dag_id,
            start_date=start_date,
            schedule=schedule,
            default_settings=default_settings,
            taskflow_collections=taskflow_collections,
            taskflows=taskflows,
            num_tasks=load_config["min_tasks_per_dag"],
        )

        Path(f"dags/{experiment_id}").mkdir(parents=True, exist_ok=True)

        with open(f"dags/{experiment_id}/dag_{i}.py", "w") as file:
            file.write(dag)

    print("DONE.")


if __name__ == "__main__":
    main()
