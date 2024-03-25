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
BUCKET_NAME = os.getenv('bucket_name')
DATASET = os.getenv('dataset')
# DBT_DATASET = os.getenv('DBT_DATASET') # required by streamlit. otherwise we have to push .env file to the repo
PRODUCT_TABLE = "product"
STORE_TABLE = "store"
