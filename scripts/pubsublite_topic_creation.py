from google.api_core.exceptions import AlreadyExists
from google.cloud.pubsublite import AdminClient, Topic
from google.cloud.pubsublite.types import (
    CloudRegion,
    CloudZone,
    ReservationPath,
    TopicPath,
)
from google.protobuf.duration_pb2 import Duration
import argparse
# from airflow.models import Variable
from config_parameters import *



# parser = argparse.ArgumentParser()

# parser.add_argument("--topic", required=True, help="Enter topic name")
# args = parser.parse_args()
# topic_id = args.topic

# pubsub_info = Variable.get("pubsublite_info", deserialize_json=True)

project_number = project_number #"hive-project-404608"
cloud_region = cloud_region #"us-central1"
zone_id = zone_id #"c"
topic_id = None #args.topic
reservation_id = "reservation"
num_partitions = 1
regional = True

cloud_region = CloudRegion(cloud_region)
reservation_path = ReservationPath(project_number, cloud_region, reservation_id)

topic_path = None
if regional:
    #  A regional topic.
    topic_path = TopicPath(project_number, cloud_region, topic_id)
else:
    #  A zonal topic
    topic_path = TopicPath(
        project_number, CloudZone(cloud_region, zone_id), topic_id
    )

topic = Topic(
    name=str(topic_path),
    partition_config=Topic.PartitionConfig(
        # A topic must have at least one partition.
        count=num_partitions,
        # Set throughput capacity per partition in MiB/s.
        capacity=Topic.PartitionConfig.Capacity(
            # Set publish throughput capacity per partition to 4 MiB/s. Must be >= 4 and <= 16.
            publish_mib_per_sec=16,
            # Set subscribe throughput capacity per partition to 4 MiB/s. Must be >= 4 and <= 32.
            subscribe_mib_per_sec=32,
        ),
    ),
    retention_config=Topic.RetentionConfig(
        # Set storage per partition to 30 GiB. This must be in the range 30 GiB-10TiB.
        # If the number of byptes stored in any of the topic's partitions grows beyond
        # this value, older messages will be dropped to make room for newer ones,
        # regardless of the value of `period`.
        per_partition_bytes=30 * 1024 * 1024 * 1024,
        # Allow messages to be retained for 1 hour.
        period=Duration(seconds=60 * 60),
    ),
    reservation_config=Topic.ReservationConfig(
        throughput_reservation=str(reservation_path),
    ),
)

client = AdminClient(cloud_region)
try:
    response = client.create_topic(topic)
    if regional:
        print(f"{response.name} (regional topic) created successfully.")
    else:
        print(f"{response.name} (zonal topic) created successfully.")
except AlreadyExists:
    print(f"{topic_path} already exists.")