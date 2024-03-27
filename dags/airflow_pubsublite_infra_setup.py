"""
Setup pubsublite infra
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
from google.api_core.exceptions import AlreadyExists, NotFound
from google.cloud import bigquery, storage
from google.cloud.pubsublite import (
    AdminClient, 
    Topic, 
    Subscription, 
    Reservation
)
from google.cloud.pubsublite.types import (
    CloudRegion,
    CloudZone,
    ReservationPath,
    SubscriptionPath,
    TopicPath,
)
from airflow.operators.trigger_dagrun import (
    TriggerDagRunOperator
)
from google.protobuf.duration_pb2 import Duration
from config_data.pubsublite_config import *

# pubsub_info = Variable.get("pubsublite_info", deserialize_json=True)

class GCSCreatePubSubLiteReservation(BaseOperator):
    """
    Custom operator to create a pubsublite reservation if it doesn't exist.
    """

    @apply_defaults
    def __init__(self, 
                 project_number, 
                 cloud_region,
                 zone_id,
                 reservation_id,
                 throughput_capacity = 4,
                 **kwargs
        ):
        super().__init__(**kwargs)
        self.project_number = project_number
        self.cloud_region = cloud_region
        self.zone_id = zone_id
        self.reservation_id = reservation_id
        self.throughput_capacity = throughput_capacity

        self.cloud_region = CloudRegion(self.cloud_region)
        self.reservation_path = ReservationPath(self.project_number, self.cloud_region, self.reservation_id)

        self.reservation = Reservation(
            name=str(self.reservation_path),
            throughput_capacity=self.throughput_capacity,
        )
    
    def execute(self, context):
        """
        Check if the specified pubsublite reservation exists. If not, create the reservation.
        """
        client = AdminClient(self.cloud_region)
        try:
            response = client.create_reservation(self.reservation)
            self.log.info(f"{response.name} created successfully.")
        except AlreadyExists:
            self.log.info(f"{self.reservation_path} already exists.")


class GCSCreatePubSubLiteTopic(BaseOperator):
    """
    Custom operator to create a pubsublite topic if it doesn't exist.
    """

    @apply_defaults
    def __init__(self, 
                 project_number, 
                 cloud_region,
                 zone_id,
                 topic_id,
                 reservation_id,
                 regional,
                 num_partitions: int = 1,
                 **kwargs
        ):
        super().__init__(**kwargs)
        self.project_number = project_number
        self.cloud_region = cloud_region
        self.zone_id = zone_id
        self.topic_id = topic_id
        self.reservation_id = reservation_id
        self.regional = regional
        self.num_partitions = num_partitions
        self.publish_mib_per_sec = 16
        self.subscribe_mib_per_sec = 32
        self.per_partition_bytes = 30 * 1024 * 1024 * 1024
        self.cloud_region = CloudRegion(self.cloud_region)
        self.retention_period = 60 * 60
        self.reservation_path = ReservationPath(self.project_number, self.cloud_region, self.reservation_id)

        self.topic_path = None
        if self.regional:
            #  A regional topic.
            self.topic_path = TopicPath(self.project_number, self.cloud_region, self.topic_id)
        else:
            #  A zonal topic
            self.topic_path = TopicPath(
                self.project_number, CloudZone(self.cloud_region, self.zone_id), self.topic_id
            )

        self.topic = Topic(
            name=str(self.topic_path),
            partition_config=Topic.PartitionConfig(
                # A topic must have at least one partition.
                count=self.num_partitions,
                # Set throughput capacity per partition in MiB/s.
                capacity=Topic.PartitionConfig.Capacity(
                    # Set publish throughput capacity per partition to 4 MiB/s. Must be >= 4 and <= 16.
                    publish_mib_per_sec=self.publish_mib_per_sec,
                    # Set subscribe throughput capacity per partition to 4 MiB/s. Must be >= 4 and <= 32.
                    subscribe_mib_per_sec=self.subscribe_mib_per_sec,
                ),
            ),
            retention_config=Topic.RetentionConfig(
                # Set storage per partition to 30 GiB. This must be in the range 30 GiB-10TiB.
                # If the number of byptes stored in any of the topic's partitions grows beyond
                # this value, older messages will be dropped to make room for newer ones,
                # regardless of the value of `period`.
                per_partition_bytes=self.per_partition_bytes,
                # Allow messages to be retained for 1 hour.
                period=Duration(seconds=self.retention_period),
            ),
            reservation_config=Topic.ReservationConfig(
                throughput_reservation=str(self.reservation_path),
            ),
        )

    def execute(self, context):
        """
        Check if the specified pubsublite topic exists. If not, create the topic.
        """
        client = AdminClient(self.cloud_region)

        try:
            # Attempt to get the bucket; if NotFound exception is raised, the bucket doesn't exist
            response = client.create_topic(self.topic)
            if self.regional:
                self.log.info(f"{response.name} (regional topic) created successfully.")
            else:
                self.log.info(f"{response.name} (zonal topic) created successfully.")
            self.log.info(f"Topic '{self.topic_id}' not created. Creating...")
        except AlreadyExists:
            self.log.info(f"Topic '{self.topic_id}' not created. Creating...")

class GCSCreatePubSubLiteSubscription(BaseOperator):
    """
    Custom operator to create a pubsublite subscription if it doesn't exist.
    """

    @apply_defaults
    def __init__(self, 
                 project_number, 
                 cloud_region,
                 zone_id,
                 topic_id,
                 subscription_id,
                 regional,
                 **kwargs
        ):
        super().__init__(**kwargs)
        self.project_number = project_number
        self.cloud_region = cloud_region
        self.zone_id = zone_id
        self.topic_id = topic_id
        self.subscription_id = subscription_id
        self.regional = regional

        if self.regional:
            self.location = CloudRegion(self.cloud_region)
        else:
            self.location = CloudZone(CloudRegion(self.cloud_region), self.zone_id)

        self.topic_path = TopicPath(self.project_number, self.location, self.topic_id)
        self.subscription_path = SubscriptionPath(project_number, self.location, subscription_id)

        self.subscription = Subscription(
            name=str(self.subscription_path),
            topic=str(self.topic_path),
            delivery_config=Subscription.DeliveryConfig(
                # Possible values for delivery_requirement:
                # - `DELIVER_IMMEDIATELY`
                # - `DELIVER_AFTER_STORED`
                # You may choose whether to wait for a published message to be successfully written
                # to storage before the server delivers it to subscribers. `DELIVER_IMMEDIATELY` is
                # suitable for applications that need higher throughput.
                delivery_requirement=Subscription.DeliveryConfig.DeliveryRequirement.DELIVER_AFTER_STORED,
            ),
        )
    
    def execute(self, context):
        """
        Check if the specified pubsublite subscription exists. If not, create the topic.
        """
        client = AdminClient(self.cloud_region)
        try:
            response = client.create_subscription(self.subscription)
            self.log.info(f"{response.name} created successfully.")
        except AlreadyExists:
            print(f"{self.subscription_path} already exists.")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "catchup": False
}
dag = DAG(
    'setup_pubsublite_infra',
    default_args=default_args,
    description="Task creates reservation, topic, subscription and produces data to the topic",
    schedule_interval="@daily",
    start_date=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
    tags=["dev"]
)

create_reservation = GCSCreatePubSubLiteReservation(
    task_id="create_reservation",
    project_number=project_number,
    cloud_region=cloud_region,
    zone_id=zone_id,
    reservation_id=reservation_id,
    dag=dag
)

create_txn_topic = GCSCreatePubSubLiteTopic(
    task_id="create_txn_topic",
    project_number=project_number,
    cloud_region=cloud_region,
    zone_id=zone_id,
    reservation_id=reservation_id,
    topic_id=txn_topic_id,
    regional=regional,
    dag=dag
)

create_inv_topic = GCSCreatePubSubLiteTopic(
    task_id="create_inv_topic",
    project_number=project_number,
    cloud_region=cloud_region,
    zone_id=zone_id,
    reservation_id=reservation_id,
    topic_id=inv_topic_id,
    regional=regional,
    dag=dag
)

create_txn_subscription = GCSCreatePubSubLiteSubscription(
    task_id="create_txn_subscription",
    project_number=project_number,
    cloud_region=cloud_region,
    zone_id=zone_id,
    topic_id=txn_topic_id,
    subscription_id=f"{txn_topic_id}-topic",
    regional=regional,
    dag=dag
)

create_inv_subscription = GCSCreatePubSubLiteSubscription(
    task_id="create_inv_subscription",
    project_number=project_number,
    cloud_region=cloud_region,
    zone_id=zone_id,
    topic_id=inv_topic_id,
    subscription_id=f"{inv_topic_id}-topic",
    regional=regional,
    dag=dag
)

all_success = DummyOperator(
    task_id="all_success",
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS
)

trigger_publish_stream_to_bq = TriggerDagRunOperator(
    task_id="trigger_publish_stream_to_bq",
    trigger_dag_id="publish_stream_to_bq",
    dag=dag
)

create_reservation >> [create_txn_topic, create_inv_topic] 
create_txn_topic >> create_txn_subscription
create_inv_topic >> create_inv_subscription
[create_txn_subscription, create_inv_subscription] >> all_success >> trigger_publish_stream_to_bq