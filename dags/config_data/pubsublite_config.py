import os
from dotenv import load_dotenv

load_dotenv()

project_number = os.getenv('project_id')
cloud_region = os.getenv('region')
zone_id = os.getenv('zone_id')
reservation_id = "reservation_2"
throughput_capacity = 4
num_partitions = 2
regional = True
txn_topic_id = "transaction_2"
inv_topic_id = "inventory_2"