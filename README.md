# Composer Workload Simulator

This project provides a framework for dynamically generating Apache Airflow DAGs to simulate workloads and test data pipelines on Google Cloud Platform (GCP). 

## Project Structure

```bash
├── README.md
├── configs
│   └── sample.yaml          # Sample configuration file for generating DAGs
├── dags                     # Airflow DAG definitions (auto-generated)
│   ├── experiment_1         # Organized by experiment id
│       └── dag_1.py
│   ... 
├── main.py                  # Main script to generate DAGs based on config
├── taskflow_collections     # Custom TaskFlow collections
│   ├── __init__.py
│   ├── base_taskflows.py    # Base TaskFlow definitions
│   └── google_cloud_taskflows.py # GCP-specific TaskFlow definitions 
```

## Getting Started

* **Configuration:** 
    * Modify the `configs/sample.yaml` file to adjust the desired parameters for DAG generation. See the example below for available options.
* **Running the DAGs:** 
    * Execute `python main.py` to generate the DAG files in the `dags` folder.
    * Monitor the DAGs in the Airflow UI

## Example Configuration (`configs/sample.yaml`)

```yaml
experiment_id: experiment_1
number_of_dags: 10
tasks_per_dag: 3

# Schedules and weights
schedules:
  "@daily": 0.5
  "0 * * * *": 0.1
  "10 * * * *": 0.1
  "20 * * * *": 0.1
  "30 * * * *": 0.1
  "40 * * * *": 0.1

# Start dates and weights
start_dates:
  "9/19/2024": 1

# taskflows and weights
taskflows:
  base:
    PythonOperator: 0.3
    KubernetesPodOperator: 0.3
    BashOperator: 0.3
    EmptyOperator: 0.3
    BranchPythonOperator: 0.3
  google_cloud:
    BigQueryInsertJobOperator: 0.001
    DataprocSubmitJobOperator: 0.001
    BeamRunJavaPipelineOperator: 0.001
    DataprocCreateBatchOperator: 0.001
    GCSToGCSOperator: 0.001
    GCSToBigQueryOperator: 0.001
    GKEStartPodOperator: 0.001

# Default settings for every generated DAG.
default_settings:
  deferrable: true
  retries: 1
  catchup: false
  is_paused_upon_creation: false
  execution_timeout: 30
  sla: 25
  project_id: your-project
  region: your-region
  mode: poke
  poke_interval: 120 
```

## TaskFlow Collections

This project utilizes custom TaskFlow collections to define a variety of tasks:

* **`base_taskflows`:**  Provides reusable TaskFlows for common operations:
    *  `PythonOperator`: Executes a Python callable.
    *  `BashOperator`: Executes a Bash command.
    *  `KubernetesPodOperator`:  Runs a pod on a Kubernetes cluster.
    *  `BranchPythonOperator`:  Conditionally chooses a downstream task.
    *  `EmptyOperator`:  A no-op task, useful for defining dependencies.

* **`google_cloud_taskflows`:** Offers specialized TaskFlows for interacting with GCP services:
    *  `BigQueryInsertJobOperator`: Executes a BigQuery query.
    *  `DataprocSubmitJobOperator`: Submits a Hive job to a Dataproc cluster.
    *  `BeamRunJavaPipelineOperator`: Runs a Java pipeline on Dataflow.
    *  `DataprocCreateBatchOperator`: Creates a batch workload on Dataproc.
    *  `GCSToGCSOperator`: Copies data between GCS buckets.
    *  `GCSToBigQueryOperator`: Loads data from GCS to BigQuery.
    *  `GKEStartPodOperator`: Starts a pod on a GKE cluster.


## Creating Custom TaskFlow Collections

You can extend the functionality of this project by creating your own TaskFlow collections. Here's how:

1. **Create a new Python file:** In the `taskflow_collections` directory, create a new file (e.g., `my_custom_taskflows.py`).

2. **Define a class:**  Create a class that inherits from `BaseTaskFlows` (or create a new base class if needed).

3. **Implement TaskFlow methods:** Add methods to your class that generate the code for your custom TaskFlows. These methods should return strings containing the Airflow operator definitions.

    ```python
    from taskflow_collections.base_taskflows import BaseTaskFlows

    class MyCustomTaskFlows(BaseTaskFlows):
        def __init__(self, dag_id, my_param):
            super().__init__(dag_id)
            self.my_param = my_param

        def my_custom_operator_taskflow(self, task_id: str):
            return f"""
            task_{task_id} = MyCustomOperator(
                task_id="my_custom_task_{task_id}",
                my_param=self.my_param,
            )
            """
    ```

4. **Import and use in `main.py`:**  
    * Import your new class in `main.py`.
    *  Update the `generate_tasks` function in `main.py` to include your custom TaskFlows.
    *  Modify the `configs/sample.yaml` file to add your custom TaskFlow collection and assign weights to the tasks.


## Notes

1. There may be inefficiencies in the dags or inaccuracies in the amount of tasks that actually get generated (usually more than specified). This is a simulation tool and not meant to be generating optimized DAGs. Use this to get an idea for the Composer environment size that can handle your maximum intended workload.
