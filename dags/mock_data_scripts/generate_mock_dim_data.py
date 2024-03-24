"""
Script created to generate mock data
for dimensional objects - product and
store, and persist them as json files
remotely
"""
import numpy as np
from datetime import datetime, timedelta
import pandas as pd
import json
import os
import sys
from mock_data_scripts.mock_data_schema import *
from google.cloud import storage
from io import StringIO
from config_data.gcp_config_parameters import *
from dotenv import load_dotenv
# from airflow.models import Variable
load_dotenv()

num_prod_examples = int(os.getenv('num_prod_examples', 1000))
num_store_examples = int(os.getenv('num_store_examples', 500))
# num_products = int(os.getenv('num_products', 50))
# num_stores = int(os.getenv('num_stores', 10))
prod_id_count = 0
supplier_id_count = 0
store_id_count = 0

bucket = BUCKET_NAME
dim_filepath = os.getenv('mock_data_subfolder')
dim_prod_file_path = f"{dim_filepath}/dim_products.json"
dim_store_file_path = f"{dim_filepath}/dim_stores.json"

def list_bucket(bucket_name: str=bucket):
    """Lists all buckets."""

    storage_client = storage.Client()
    buckets = storage_client.list_buckets()

    for bucket in buckets:
        if bucket.name == bucket_name:
            print(f"Bucket {bucket_name} exists")
            return True
    return False

def create_bucket(bucket_name: str=bucket):
    """
    Create a new bucket in the US region with the standard storage
    class
    """
    # bucket_name = "your-new-bucket-name"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = "STANDARD"
    new_bucket = storage_client.create_bucket(bucket, location="us")

    print(
        "Created bucket {} in {} with storage class {}".format(
            new_bucket.name, new_bucket.location, new_bucket.storage_class
        )
    )
    return new_bucket

def upload_blob_from_file(bucket_name, destination_blob_name, func, num_examples):
    """Uploads json blob in memory to the bucket."""

    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The contents to upload to the file
    # contents = "these are my contents"

    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    contents = "\n".join(json.dumps(func(), ensure_ascii=False) for _ in range(num_examples))

    blob.upload_from_file(StringIO(contents), content_type="application/json")

    print(
        f"{destination_blob_name} uploaded to {bucket_name}."
    ) 

def generate_products():
    """Generate product record according to the following format
        {"product_id": "P501", "name": "Electronics Gadget", "category": "Electronics",
        "price": 299.99, "supplier_id": "S101"}    
    """
    global prod_id_count, supplier_id_count
    record = {}
    idx = np.random.randint(len(product_category["category"]))
    record["product_id"] = f"P{start_product_id + prod_id_count}"
    record["category"] = product_category["category"][idx]
    record["name"] = f"Item{prod_id_count}" # np.random.choice(product_category["name"][idx])
    b_idx = np.random.randint(len(product_category["base_price"][idx]))
    record["base_price"] = np.random.randint(*product_category["base_price"][idx][b_idx])
    record["supplier_id"] = f"S{start_supplier_id + supplier_id_count}"
    prod_id_count += 1
    supplier_id_count += 1
    return record


def generate_stores():
    """Generate store record according to the following format
        {"store_id": "W001", "location": "New York, NY", "size": 25000, "manager": "John
            Doe" }    
    """
    global store_id_count
    record = {}
    record["store_id"] = f"W{start_store_id + store_id_count}"
    record["location"] = np.random.choice(stores["location"])
    record["size"] = int(np.random.choice(stores["size"]))
    record["manager"] = np.random.choice(stores["manager"])
    store_id_count += 1
    return record


def run_pipeline():
    # generate_products()
    # generate_stores()
    upload_blob_from_file(bucket, dim_prod_file_path, generate_products, num_examples=num_prod_examples)
    print("Uploaded dim products")
    upload_blob_from_file(bucket, dim_store_file_path, generate_stores, num_examples=num_store_examples)
    print("Uploaded dim stores")


def main():
    if not list_bucket(bucket):
        create_bucket(bucket)
    run_pipeline()


if __name__ == "__main__":
    main()