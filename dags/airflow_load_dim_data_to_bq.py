"""
Generate the mock data and store them in GCS
Load the data into BQ
"""
from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash_operator import (
    BashOperator
)
# from airflow.providers.google.cloud.operators.gcs import (
#     GCSCreateBucketOperator
# )
from airflow.providers.google.cloud.hooks.gcs import (
    GCSHook
)
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceSensor
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator
)

from airflow.operators.python import (
    BranchPythonOperator
)
from airflow.operators.dummy import (
    DummyOperator
)
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from mock_data_scripts.generate_mock_dim_data import run_pipeline
from config_data.gcp_config_parameters import *

# gcp_info = Variable.get("gcp_info", deserialize_json=True)
bucket_folder = "generated_data"
dim_products = f"{bucket_folder}/dim_products.json"
dim_stores = f"{bucket_folder}/dim_stores.json"
# gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")

product_schema = [
    {'name': 'product_id', 'type': 'STRING'},
    {'name': 'name', 'type': 'STRING'},
    {'name': 'category', 'type': 'STRING'},
    {'name': 'base_price', 'type': 'INT64'},
    {'name': 'supplier_id', 'type': 'STRING'}
]

store_schema = [
    {'name': 'store_id', 'type': 'STRING'},
    # {'name': 'name', 'type': 'STRING'},
    {'name': 'location', 'type': 'STRING'},
    {'name': 'size', 'type': 'INT64'},
    {'name': 'manager', 'type': 'STRING'}
]

def check_bucket_exists(**kwargs):
    try:
        obj =   gcs_hook.exists(
                        bucket_name=BUCKET_NAME, 
                        object_name=bucket_folder
                )
    except NotFound:
        return 'create_bucket'
    return 'bucket_exists'


class GCSCreateBucketOperator(BaseOperator):
    """
    Custom operator to create a bucket if it doesn't exist.
    """

    @apply_defaults
    def __init__(self, project_id, bucket_name, storage_class, location='US', **kwargs):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.storage_class = storage_class
        self.location = location

    def execute(self, context):
        """
        Check if the specified GCS bucket exists. If not, create the bucket.
        """
        storage_client = storage.Client(project=self.project_id)

        try:
            # Attempt to get the bucket; if NotFound exception is raised, the bucket doesn't exist
            storage_client.get_bucket(self.bucket_name)
            self.log.info(f"Bucket '{self.bucket_name}' already exists.")
        except NotFound:
            self.log.info(f"Bucket '{self.bucket_name}' not found. Creating...")
            bucket = storage_client.bucket(self.bucket_name)
            bucket.storage_class = self.storage_class
            new_bucket = storage_client.create_bucket(bucket, location=self.location)
            self.log.info(f"Bucket '{self.bucket_name}' created in location '{self.location}'.")


class BigQueryCreateDatasetOperator(BaseOperator):
    """
    Custom operator to create a BigQuery dataset if it doesn't exist.
    """

    @apply_defaults
    def __init__(self, project_id, dataset_id, location='US', **kwargs):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.location = location

    def execute(self, context):
        client = bigquery.Client(project=self.project_id)
        dataset_ref = client.dataset(self.dataset_id)

        try:
            client.get_dataset(dataset_ref)
            self.log.info(f"Dataset '{self.dataset_id}' already exists.")
        except Exception as e:
            if 'Not found' in str(e):
                self.log.info(f"Dataset '{self.dataset_id}' not found. Creating...")
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = self.location
                client.create_dataset(dataset)
                self.log.info(f"Dataset '{self.dataset_id}' created.")
            else:
                raise e


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
    "max_active_runs": 1
}
dag = DAG(
    'load_mock_dim_data_bq',
    default_args=default_args,
    description="Task creates bucket and load mock dimensional data into BQ",
    schedule_interval="@daily",
    start_date=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
    tags=["dev"]
)


run_date = "{{ dag_run.conf['execution_date'] if dag_run and dag_run.conf and 'execution_date' in dag_run.conf else ds_nodash }}"

create_bucket = GCSCreateBucketOperator(
    task_id="create_bucket",
    bucket_name=BUCKET_NAME,
    project_id=PROJECT_ID,
    location=LOCATION,
    storage_class="STANDARD",
    dag=dag
)


generate_mock_data = PythonOperator(
    task_id="generate_mock_data",
    python_callable=run_pipeline,
    dag=dag
)

create_dataset = BigQueryCreateDatasetOperator(
    task_id='create_dataset',
    dataset_id=DATASET,
    project_id=PROJECT_ID,
    location=LOCATION,
    # gcp_conn_id='google_cloud_default',
    dag=dag
)

    
load_product_data_bq = GCSToBigQueryOperator(
    task_id='load_product_data_bq',
    bucket=BUCKET_NAME,
    project_id=PROJECT_ID,
    source_objects=dim_products,
    destination_project_dataset_table=f'{PROJECT_ID}.{DATASET}.{PRODUCT_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    schema_fields=product_schema,
    source_format="NEWLINE_DELIMITED_JSON",
    dag=dag,
)

load_store_data_bq = GCSToBigQueryOperator(
    task_id='load_store_data_bq',
    bucket=BUCKET_NAME,
    project_id=PROJECT_ID,
    source_objects=dim_stores,
    destination_project_dataset_table=f'{PROJECT_ID}.{DATASET}.{STORE_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    schema_fields=store_schema,
    source_format="NEWLINE_DELIMITED_JSON",
    dag=dag,
)



# check_bucket_task >> [create_bucket, bucket_exists]
# [create_bucket, bucket_exists] >> one_success 
create_bucket >> generate_mock_data
generate_mock_data >> create_dataset >> [load_product_data_bq, load_store_data_bq]
