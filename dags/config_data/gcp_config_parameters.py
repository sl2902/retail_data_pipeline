import os
from dotenv import load_dotenv
from pathlib import Path

# project_root = Path(__file__).resolve().parent.parent.parent

# load_dotenv(f"{project_root}/.env")
load_dotenv()

# gcp_info variables
PROJECT_ID = os.getenv('project_id')
REGION = os.getenv('region')
LOCATION = os.getenv('location')
BUCKET_NAME = "mock-retail-data-2"
DATASET = "retail_2"
DBT_DATASET = "dbt_retail"
PRODUCT_TABLE = "product"
STORE_TABLE = "store"
