from google.api_core.exceptions import AlreadyExists
from google.cloud.pubsublite import AdminClient, Subscription
from google.cloud.pubsublite.types import (
    CloudRegion,
    CloudZone,
    SubscriptionPath,
    TopicPath,
)
import argparse



parser = argparse.ArgumentParser()

parser.add_argument("--topic", required=True, help="Enter topic name")
args = parser.parse_args()
topic_id = args.topic

project_number = "hive-project-404608"
cloud_region = "us-central1"
zone_id = "c"
regional = True
subscription_id = f"{topic_id}-topic"

if regional:
    location = CloudRegion(cloud_region)
else:
    location = CloudZone(CloudRegion(cloud_region), zone_id)

topic_path = TopicPath(project_number, location, topic_id)
subscription_path = SubscriptionPath(project_number, location, subscription_id)

subscription = Subscription(
    name=str(subscription_path),
    topic=str(topic_path),
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

client = AdminClient(cloud_region)
try:
    response = client.create_subscription(subscription)
    print(f"{response.name} created successfully.")
except AlreadyExists:
    print(f"{subscription_path} already exists.")