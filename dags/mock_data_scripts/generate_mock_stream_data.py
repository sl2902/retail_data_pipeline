
"""
Script created to generate streaming transactions and inventory
records from dimensional data stored on GCS
"""
import numpy as np
from datetime import datetime, timedelta
import pandas as pd
import json
from mock_data_scripts.mock_data_schema import *
from mock_data_scripts.generate_mock_dim_data import list_bucket, create_bucket
from google.cloud import storage
from io import StringIO
# from airflow.models import Variable
from pathlib import Path
from time import time
import argparse
from config_data.gcp_config_parameters import *
from dotenv import load_dotenv
import logging
load_dotenv()
logger = logging.getLogger("airflow.task")

# gcp_info = Variable.get("gcp_info", deserialize_json=True)
bucket = BUCKET_NAME
dim_filepath = os.getenv("mock_data_subfolder")
dim_prod_file_path = f"{dim_filepath}/dim_products.json"
dim_store_file_path = f"{dim_filepath}/dim_stores.json"
stream_filepath = os.getenv('stream_data_subfolder')
stream_prod_file_path = f"{stream_filepath}/transactions/transactions.json"
stream_store_file_path = f"{stream_filepath}/inventories/inventories.json"
parent_path = Path(__file__).resolve().parent
# txn_update_file = f"{parent_path}/config/transaction_stream_update.json"
config_filepath = os.getenv('config_data_subfolder')
txn_update_file = f"{config_filepath}/transaction_stream_update.json"


def read_dim_file(bucket_name, destination_blob_name):
    """Read json blob from bucket."""

    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The contents to upload to the file
    # contents = "these are my contents"

    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    file = blob.download_as_text()
    
    return file  

def write_dim_file(bucket_name, destination_blob_name, content):
    """Write json object to bucket."""

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    if isinstance(content, list):
        contents = "\n".join(json.dumps(line, ensure_ascii=False) for line in content)
    else:
        contents = json.dumps(content, ensure_ascii=False)
    blob.upload_from_file(StringIO(contents), content_type="application/json")

    print(
        f"{destination_blob_name} uploaded to {bucket_name}."
    ) 

def generate_random_sample(df) -> dict:
    """Randomly sample a record from the dimensional DataFrame"""
    return json.loads(df.sample(n=1).to_json(orient="records"))[0] 

def generate_txn_stream(dim_prod: dict, dim_store: dict, tranction_id: int, timestamp: datetime):
    return {
        "transaction_id": f"T{tranction_id}",
        "product_id": dim_prod["product_id"],
        "timestamp": datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S"),
        "quantity": np.random.randint(1, 100),
        "unit_price": dim_prod["base_price"],
        "store_id": dim_store["store_id"]
    }

def generate_inventory_stream(product_id: str, store_id: str, qty_change: int, inventory_id: int, timestamp: datetime):
    return {
        "inventory_id": f"IN{inventory_id}",
        "product_id": product_id,
        "timestamp": datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S"),
        "quantity_change": qty_change * -1,
        "store_id": store_id
    }

def generate_random_timestamp(
        start_date: str,
        units: str,
        _min: int = 5,
        _max: int = 10
):
    # max_minutes = (datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S") - datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")).total_seconds() // 60
    start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
    if units == "seconds":
        start_date += timedelta(seconds=np.random.randint(_min, _max))
    else:
        _min, _max = _min * 60, _max * 60
        start_date += timedelta(seconds=np.random.randint(_min, _max))
    return start_date

def recent_file_handle_gcs(bucket_name, file_path):
    """Open file on Google Cloud Storage for reading and writing."""
    global recent_fh
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    content = blob.download_as_text() if blob.exists() else "{}"
    recent_fh = StringIO(content)


def  get_txn_last_update():
    """Get the last updated transaction_id, inventory_id and start_date of transaction"""
    global recent_fh
    try:
            recent_fh.seek(0)
            data  = json.load(recent_fh)
            recent_fh.seek(0)
    except FileNotFoundError:
        return {"transaction_id": 1000000, "inventory_id": 1000000, "start_date": "2024-01-01 00:00:00"}
    return data

def put_txn_last_update(bucket_name, destination_blob_name, rec: dict):
    """Write the last updated transaction_id, inventory_id and start_date of transaction"""
    global recent_fh
    try:
        json.dump(rec, recent_fh)
        
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        recent_fh.seek(0)
        blob.upload_from_file(recent_fh, content_type='application/json')
        
    except Exception as e:
        print(f"Error saving recent transaction info: {e}")

def close_fh():
    """Close global file handle"""
    global recent_fh
    if recent_fh:
        recent_fh.close()

def run_pipeline(units: str, duration: int):
    logger.info(f'arg units value {units}, duration {duration}')
    dim_prod_df = pd.DataFrame([json.loads(line) 
                   for line in read_dim_file(bucket, dim_prod_file_path).splitlines()])
    dim_store_df = pd.DataFrame([json.loads(line) 
                    for line in read_dim_file(bucket, dim_store_file_path).splitlines()])

    # recent_file_handle_gcs(bucket, txn_update_file)
    start_time = time()
    end_time = time()
    txns_stream, inv_stream = [], []
    # data = get_txn_last_update()
    data = json.loads(read_dim_file(bucket, txn_update_file))
    transaction_id = data["transaction_id"]
    inventory_id = data["inventory_id"]
    last_update = data["last_update"]
    rec = {}
    curr_date = datetime.now().replace(microsecond=0)
    while  end_time - start_time < duration:
        timestamp = generate_random_timestamp(last_update, units)

        # filter future dated transactions
        # current_date() - 1
        if  (units == "minutes" and timestamp > curr_date - timedelta(days=1)) or \
            (units == "seconds" and timestamp > curr_date): 
                end_time = time()
                continue

        dim_prod = generate_random_sample(dim_prod_df)
        dim_store = generate_random_sample(dim_store_df)

        txn_stream = generate_txn_stream(dim_prod, dim_store, transaction_id, timestamp)
        txns_stream.append(txn_stream)

        # print(txn_stream)

        inv_stream.append(generate_inventory_stream(
            txn_stream["product_id"], 
            txn_stream["store_id"], 
            txn_stream["quantity"], 
            inventory_id,  
            timestamp
        ))
        # print(inv_stream)
        transaction_id += 1
        inventory_id += 1
        last_update = datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S")

        # put_txn_last_update(bucket, txn_update_file, rec)
        end_time = time()
    
    rec["transaction_id"] = transaction_id
    rec["inventory_id"] = inventory_id
    rec["last_update"] = last_update
    write_dim_file(bucket, txn_update_file, rec)
    # put_txn_last_update(bucket, txn_update_file, rec)
    # close_fh()
    write_dim_file(bucket, stream_prod_file_path, txns_stream)
    write_dim_file(bucket, stream_store_file_path, inv_stream)

    # close_fh()


def main(units, duration):
    if not list_bucket(bucket):
        create_bucket(bucket)
    run_pipeline(units, duration)

if __name__ == "__main__":
    parser = argparse.ArgumentParser('Generate mock transactions and inventories')
    parser.add_argument('--units',
               help="Enter the time unit for data granularity. The choice is between minutes and seconds",
               default="seconds"
    )
    parser.add_argument('--duration',
               help="Enter duration in seconds. This is the amount of time data will be generated for",
               type=int,
               default=2
    )
    args = parser.parse_args()
    main(args.units, int(args.duration))