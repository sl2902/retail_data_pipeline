"""
Produce stream data to allow the producer and consumer
to pubslish and subscribe from pubsublite and write
it to BQ
"""
import subprocess
import os
from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from datetime import datetime, timedelta, timezone
from airflow.operators.bash_operator import (
    BashOperator
)
# from airflow.contrib.operators.spark_submit_operator import(
#     SparkSubmitOperator
# )
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator
)
from airflow.providers.google.cloud.hooks.dataproc import (
    DataprocHook
)
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocSubmitPySparkJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.operators.python_operator import (
    PythonOperator
)
from airflow.operators.python import (
    BranchPythonOperator
)
from airflow.operators.dummy import (
    DummyOperator
)
from airflow.operators.trigger_dagrun import (
    TriggerDagRunOperator
)
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from google.cloud.storage import Client, transfer_manager
from pathlib import Path
import json
from mock_data_scripts.generate_mock_stream_data import run_pipeline
from config_data.gcp_config_parameters import *
from config_data.pubsublite_config import *
import logging
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("airflow.task")

# gcp_info = Variable.get("gcp_info", deserialize_json=True)
# pubsub_info = Variable.get("pubsublite_info", deserialize_json=True)
# cluster_configs = Variable.get("cluster_info", deserialize_json=True)
bucket_folder = os.getenv('mock_data_subfolder')
dim_products = f"{bucket_folder}/dim_products.json"
dim_stores = f"{bucket_folder}/dim_stores.json"
parent_path = Path(__file__).resolve().parent
# jars = [
#     f"gs://{BUCKET_NAME}/jars/{filename}" for filename in os.listdir(f"{parent_path}/jars")
#     if not ("bigquery" in filename)
# ]

jars = [f"gs://{BUCKET_NAME}/jars/pubsublite-spark-sql-streaming-1.0.0-with-dependencies.jar",
        f"gs://{BUCKET_NAME}/jars/google-cloud-pubsublite-1.9.0.jar"
]

pyspark_producer_main_path = f"gs://{BUCKET_NAME}/pyspark_scripts/pubsublite_pyspark_stream_producer.py"
pyspark_consumer_main_path = f"gs://{BUCKET_NAME}/pyspark_scripts/pubsublite_pyspark_stream_consumer.py"
config_files = [f"gs://{BUCKET_NAME}/config_data/"]
# config_files = \
# [
#     f"gs://{BUCKET_NAME}/config_data/gcp_config_parameters.py",
#     f"gs://{BUCKET_NAME}/config_data/pubsublite_config.py",
#     f"gs://{BUCKET_NAME}/config_data/.env",
# ]

def read_cluster_config(**kwargs):
    cfg_path = parent_path = Path(__file__).resolve().parent
    try:
        with open(f"{cfg_path}/config_data/dataproc_cluster_config.json", "r") as f:
            cluster_configs = json.load(f)
    except Exception as e:
        raise Exception(f"Dataproc config file missing")
    kwargs["task_instance"].xcom_push(key="cluster_configs", value=cluster_configs)
    logger.info("value of kwargs", cluster_configs)


def check_dataproc_cluster(**kwargs):
    dataproc_hook = DataprocHook(gcp_conn_id=os.getenv("GOOGLE_CONN_ID"))
    cluster_configs = kwargs['ti'].xcom_pull(task_ids='load_cluster_config', key='cluster_configs')
    try:
        cluster = dataproc_hook.get_cluster(project_id=PROJECT_ID, 
                                            region=REGION, 
                                            cluster_name=cluster_configs["CLUSTER_NAME"])
    except NotFound:
        return 'create_cluster'
    return 'cluster_running' 

base_path = Path(__file__).resolve().parent
config_file_path = f'{base_path}/config_data'
pyspark_file_path = f'{base_path}/pyspark_scripts'
jar_file_path = f'{base_path}/jars'


class UploadLocalFolderToGCS(BaseOperator):
    """
    Custom operator to upload directories to GCS.
    """

    @apply_defaults
    def __init__(self, project_id, bucket_name, src, exclude='', **kwargs):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.src = src
        self.exclude = exclude,
        self.workers = 2

    def execute(self, context):
        """
        Upload files concurrently
        """
        storage_client = storage.Client(project=self.project_id)

        try:
            # Attempt to get the bucket; if NotFound exception is raised, the bucket doesn't exist
            bucket = storage_client.bucket(self.bucket_name)
            for file in os.listdir(self.src):
                destination_filename = self.src.split("/")[-1]
                if not file.startswith(self.exclude):
                    blob = bucket.blob(f"{destination_filename}/{file}")
                    blob.upload_from_filename(os.path.join(self.src, file))
                    self.log.info(f"File {file} uploaded to {destination_filename}.")
        except NotFound:
            self.log.info(f"Bucket '{self.bucket_name}' not found.")
            raise NotFound(f"Bucket '{self.bucket_name}' not found.")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}
dag = DAG(
    'publish_stream_to_bq',
    max_active_runs=1,
    catchup=False,
    default_args=default_args,
    description="Task generates streaming data for producer and consumer",
    schedule_interval="@daily",
    start_date=datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0),
    render_template_as_native_obj=True,
    tags=["dev"]
)


run_date = "{{ dag_run.conf['execution_date'] if dag_run and dag_run.conf and 'execution_date' in dag_run.conf else ds_nodash }}"

# load_config_files = BashOperator(
#     task_id="load_config_files",
#     bash_command = f"gsutil cp -r {config_file_path} gs://{BUCKET_NAME}/",
#     dag=dag
# )
# load_config_files = UploadLocalFolderToGCS(
#     task_id="load_config_files",
#     project_id=PROJECT_ID,
#     bucket_name=BUCKET_NAME,
#     src=config_file_path,
#     exclude="__"
# )

# load_jar_files = BashOperator(
#     task_id="load_jar_files",
#     bash_command = f"gsutil cp -r {jar_file_path} gs://{BUCKET_NAME}/",
#     dag=dag
# )
# load_jar_files = UploadLocalFolderToGCS(
#     task_id="load_jar_files",
#     project_id=PROJECT_ID,
#     bucket_name=BUCKET_NAME,
#     src=jar_file_path
# )

# load_pyspark_files = BashOperator(
#     task_id="load_pyspark_files",
#     bash_command = f"gsutil cp -r {pyspark_file_path} gs://{BUCKET_NAME}/pyspark_scripts/",
#     dag=dag
# )
# load_pyspark_files = UploadLocalFolderToGCS(
#     task_id="load_pyspark_files",
#     project_id=PROJECT_ID,
#     bucket_name=BUCKET_NAME,
#     src=pyspark_file_path,
#     exclude="__"
# )

units = "{{ dag_run.conf['units'] if dag_run and dag_run.conf and 'units' in dag_run.conf else 'minutes' }}"
duration = "{{ dag_run.conf['duration'] if dag_run and dag_run.conf and 'duration' in dag_run.conf else 30 }}"
generate_stream_data = PythonOperator(
    task_id="generate_stream_data",
    python_callable=run_pipeline,
    provide_context=True,
    op_args=[f"{units}", f"{duration}"],
    dag=dag
)

load_cluster_config = PythonOperator(
    task_id="load_cluster_config",
    python_callable=read_cluster_config,
    provide_context=True,
    dag=dag
)

check_cluster_task = BranchPythonOperator(
    task_id='check_dataproc_cluster',
    python_callable=check_dataproc_cluster,
    provide_context=True,
    dag=dag,
)

# Dummy task to execute if the cluster is running
cluster_running = DummyOperator(
    task_id='cluster_running',
    dag=dag,
)


one_success = DummyOperator(
        task_id='one_task_success',
        dag=dag,
        trigger_rule=TriggerRule.ONE_SUCCESS,
)

# init_action_uris = "{{task_instance.xcom_pull(task_ids='load_cluster_config', key='cluster_configs')['INITIALIZATION_ACTIONS']}}"
init_action_uris = [f"gs://{BUCKET_NAME}/{os.getenv('python_data_subfolder')}/pip-install.sh"]
# init_action_uris = [init_action_uris.format(REGION=REGION)]
# num_workers = "{{task_instance.xcom_pull(task_ids='load_cluster_config', key='cluster_configs')['CLUSTER_CONFIG']['worker_config']['num_instances']}}"
# num_masters = "{{task_instance.xcom_pull(task_ids='load_cluster_config', key='cluster_configs')['CLUSTER_CONFIG']['master_config']['num_instances']}}"
master_machine_type = "{{task_instance.xcom_pull(task_ids='load_cluster_config', key='cluster_configs')['CLUSTER_CONFIG']['master_config']['machine_type_uri']}}"
master_disk_type = "{{task_instance.xcom_pull(task_ids='load_cluster_config', key='cluster_configs')['CLUSTER_CONFIG']['master_config']['disk_config']['boot_disk_type']}}"
master_disk_size = "{{task_instance.xcom_pull(task_ids='load_cluster_config', key='cluster_configs')['CLUSTER_CONFIG']['master_config']['disk_config']['boot_disk_size_gb']}}"
work_machine_type = num_workers = "{{task_instance.xcom_pull(task_ids='load_cluster_config', key='cluster_configs')['CLUSTER_CONFIG']['worker_config']['machine_type_uri']}}"
worker_disk_type = "{{task_instance.xcom_pull(task_ids='load_cluster_config', key='cluster_configs')['CLUSTER_CONFIG']['worker_config']['disk_config']['boot_disk_type']}}"
# throws error: cannot convert str to int. Type casting doesn't work. But when xcom jinja is passed directly like shown below, it works
# worker_disk_size = "{{task_instance.xcom_pull(task_ids='load_cluster_config', key='cluster_configs')['CLUSTER_CONFIG']['worker_config']['disk_config']['boot_disk_size_gb']}}"
image_version = "{{task_instance.xcom_pull(task_ids='load_cluster_config', key='cluster_configs')['CLUSTER_CONFIG']['software_config']['image_version']}}"

cluster_configs = ClusterGenerator(
    task_id="create_cluster",
    cluster_name="{{task_instance.xcom_pull(task_ids='load_cluster_config', key='cluster_configs')['CLUSTER_NAME']}}",
    project_id=PROJECT_ID,
    region=REGION,
    zone=f"{REGION}-{zone_id}",
    storage_bucket=BUCKET_NAME,
    metadata="{{task_instance.xcom_pull(task_ids='load_cluster_config', key='cluster_configs')['METADATA']}}",
    init_actions_uris=init_action_uris,
    num_workers="{{task_instance.xcom_pull(task_ids='load_cluster_config', key='cluster_configs')['CLUSTER_CONFIG']['worker_config']['num_instances']}}",
    num_masters="{{task_instance.xcom_pull(task_ids='load_cluster_config', key='cluster_configs')['CLUSTER_CONFIG']['master_config']['num_instances']}}",
    master_machine_type=master_machine_type,
    work_machine_type=work_machine_type,
    master_disk_size="{{task_instance.xcom_pull(task_ids='load_cluster_config', key='cluster_configs')['CLUSTER_CONFIG']['master_config']['disk_config']['boot_disk_size_gb']}}",
    worker_disk_size="{{task_instance.xcom_pull(task_ids='load_cluster_config', key='cluster_configs')['CLUSTER_CONFIG']['worker_config']['disk_config']['boot_disk_size_gb']}}",
    image_version=image_version
).make()

# cluster_configs = "{{task_instance.xcom_pull(task_ids='load_cluster_config', key='cluster_configs')['CLUSTER_CONFIG']}}"
create_cluster = DataprocCreateClusterOperator(
    task_id="create_cluster",
    cluster_name="{{task_instance.xcom_pull(task_ids='load_cluster_config', key='cluster_configs')['CLUSTER_NAME']}}",
    project_id=PROJECT_ID,
    region=REGION,
    cluster_config=cluster_configs,
    dag=dag
)


producer_job = DataprocSubmitPySparkJobOperator(
    task_id="producer_job",
    main=pyspark_producer_main_path,
    pyfiles=config_files,
    cluster_name="{{task_instance.xcom_pull(task_ids='load_cluster_config', key='cluster_configs')['CLUSTER_NAME']}}",
    project_id=PROJECT_ID,
    region=REGION,
    dataproc_jars=jars,
    dag=dag
)

consumer_job = DataprocSubmitPySparkJobOperator(
    task_id="consumer_job",
    main=pyspark_consumer_main_path,
    pyfiles=config_files,
    cluster_name="{{task_instance.xcom_pull(task_ids='load_cluster_config', key='cluster_configs')['CLUSTER_NAME']}}",
    project_id=PROJECT_ID,
    region=REGION,
    dataproc_jars=jars,
    dag=dag
)

all_success = DummyOperator(
        task_id='all_task_success',
        dag=dag,
        trigger_rule=TriggerRule.ALL_SUCCESS,
)

trigger_build_dbt_model = TriggerDagRunOperator(
    task_id="trigger_build_dbt_model",
    trigger_dag_id="build_dbt_model",
    dag=dag
)

delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster",
    cluster_name="{{task_instance.xcom_pull(task_ids='load_cluster_config', key='cluster_configs')['CLUSTER_NAME']}}",
    project_id=PROJECT_ID,
    region=REGION,
    dag=dag
)

generate_stream_data >> load_cluster_config >> check_cluster_task >> [cluster_running, create_cluster]
[cluster_running, create_cluster] >> one_success >> [consumer_job, producer_job]
[consumer_job, producer_job] >> all_success >> trigger_build_dbt_model >> delete_cluster

