"""
Upload config, jars and py_spark files to GCS
"""
import subprocess
import os
from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from datetime import datetime, timedelta, timezone
from airflow.operators.python_operator import (
    PythonOperator
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator
)
from airflow.operators.trigger_dagrun import (
    TriggerDagRunOperator
)
from airflow.operators.dummy_operator import (
    DummyOperator
)
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from google.cloud.storage import Client, transfer_manager
from pathlib import Path
from config_data.gcp_config_parameters import *
from dotenv import load_dotenv

load_dotenv()


base_path = Path(__file__).resolve().parent
env_path = Path(__file__).resolve().parent.parent
env_file_path = f'{env_path}/.env'
dst_env_file_path = f"{os.getenv('config_data_subfolder')}/.env"
config_file_path = f'{base_path}/{os.getenv("config_data_subfolder")}'
pyspark_file_path = f'{base_path}/pyspark_scripts'
jar_file_path = f'{base_path}/jars'
pip_file_path = f'{base_path}/{os.getenv("python_data_subfolder")}/pip-install.sh'
dst_pip_file_path = f"{os.getenv('python_data_subfolder')}/pip-install.sh"


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
    'upload_config_files_to_gcs',
    catchup=False,
    default_args=default_args,
    description="Task uploads config files required by the pipeline",
    schedule_interval="@daily",
    start_date=datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0),
    render_template_as_native_obj=True,
    tags=["dev"]
)


run_date = "{{ dag_run.conf['execution_date'] if dag_run and dag_run.conf and 'execution_date' in dag_run.conf else ds_nodash }}"

start_pipeline = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

trigger_setup_pubsublite_infra = TriggerDagRunOperator(
    task_id="trigger_setup_pubsublite_infra",
    trigger_dag_id="setup_pubsublite_infra",
    dag=dag
)

load_pip_install_file = LocalFilesystemToGCSOperator(
    task_id="load_pip_install_file",
    src=pip_file_path,
    dst=dst_pip_file_path,
    bucket=BUCKET_NAME,
    dag=dag
)

load_env_file = LocalFilesystemToGCSOperator(
    task_id="load_env_file",
    src=env_file_path,
    dst=dst_env_file_path,
    bucket=BUCKET_NAME,
    dag=dag
)

load_config_files = UploadLocalFolderToGCS(
    task_id="load_config_files",
    project_id=PROJECT_ID,
    bucket_name=BUCKET_NAME,
    src=config_file_path,
    exclude="__",
    dag=dag
)

load_jar_files = UploadLocalFolderToGCS(
    task_id="load_jar_files",
    project_id=PROJECT_ID,
    bucket_name=BUCKET_NAME,
    src=jar_file_path,
    exclude="__",
    dag=dag
)

load_pyspark_files = UploadLocalFolderToGCS(
    task_id="load_pyspark_files",
    project_id=PROJECT_ID,
    bucket_name=BUCKET_NAME,
    src=pyspark_file_path,
    exclude="__",
    dag=dag
)

all_success = DummyOperator(
    task_id="all_success",
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS
)

trigger_load_mock_dim_data_to_bq = TriggerDagRunOperator(
    task_id="trigger_load_mock_dim_data_to_bq",
    trigger_dag_id="load_mock_dim_data_bq",
    dag=dag
)

start_pipeline >> trigger_setup_pubsublite_infra >> [load_pip_install_file, load_env_file, load_config_files, load_jar_files, load_pyspark_files] 
[load_pip_install_file, load_env_file, load_config_files, load_jar_files, load_pyspark_files] >> all_success >> trigger_load_mock_dim_data_to_bq