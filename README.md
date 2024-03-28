![Python](https://img.shields.io/badge/Python-3.8-4B8BBE.svg?style=flat&logo=python&logoColor=FFD43B&labelColor=306998)
![PySpark](https://img.shields.io/badge/pySpark-3.3-E36B22?style=flat-square&logo=apachespark&logoColor=E36B22&labelColor=3C3A3E)
![CloudStorage](https://img.shields.io/badge/GoogleCloudStorage-3772FF?style=flat&logo=googlecloudstorage&logoColor=white&labelColor=3772FF)
![BigQuery](https://img.shields.io/badge/BigQuery-3772FF?style=flat&logo=googlebigquery&logoColor=white&labelColor=3772FF)
![BigQuery](https://img.shields.io/badge/Pubsublite-3772FF?style=flat&logo=googlepubsub&logoColor=white&labelColor=3772FF)
![Dataproc](https://img.shields.io/badge/Dataproc-3772FF?style=flat&logo=googledataproc&logoColor=white&labelColor=3772FF)
![dbt](https://img.shields.io/badge/dbt-1.7-262A38?style=flat&logo=dbt&logoColor=FF6849&labelColor=262A38)
![Docker](https://img.shields.io/badge/Docker-329DEE?style=flat&logo=docker&logoColor=white&labelColor=329DEE)
![Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)

![License](https://img.shields.io/badge/license-CC--BY--SA--4.0-31393F?style=flat&logo=creativecommons&logoColor=black&labelColor=white)

# US E-commerce retail data pipeline - transaction and inventory analysis
This project was created as part of the finale for the DataTalksClub - Data Engineering Zoomcamp 6 week course. The data used for this project
is completely artifical; they were generated using Python scripts.
There are 4 tables in total: 
1) Two dimensional tables which contains `product` and `store` data
2) Two fact tables which contain `inventories` and `transactions` data.
In order to create a dashboard, some historical data is generated starting Jan 2024 - March 2024. The Airflow job can be run every hour or so to simulate the generation of transactions and inventories every few seconds.
The number of `products` and `stores` is config driven and is set to 2_000 and 1_000 records respectively.

## Problem Description
The goal of the project is to build an end to end streaming data pipeline that will produce data, between 5 and 10 seconds, for a fictitious US E-commerce retail giant. The dimensional data and fact data are store in Google Cloud Storage. Pubsublite is used to produce and consume streaming `transactions` and `inventories`, which is processed using Apache Spark; the final results are stored in Bigquery, and they are transformed using dbt; the analysis is displayed on Streamlit.

## Technology Stack 
The following technologies have been used
- [Google BigQuery](https://cloud.google.com/bigquery?hl=en)
- [Google Cloud Storage (GCS)](https://cloud.google.com/storage?hl=en)
- [Google Dataproc](https://cloud.google.com/dataproc?hl=en)
- [Google Pubsublite](https://cloud.google.com/pubsub/lite/docs)
- [Terraform](https://www.terraform.io/)
- [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/start.html)
- [Apache Spark](https://spark.apache.org/docs/latest/api/python/user_guide)
- [dbt](https://github.com/dbt-labs/dbt-core)
- [Docker](https://docs.docker.com/get-docker/)
- [Make](https://makefiletutorial.com/)
- [Pipenv](https://pipenv.pypa.io/en/latest/)
- [Streamlit](https://streamlit.io/)

## Data Dictionary
Schema for `product`
|Field name    |Type     |Description                               | 
|--------------|---------|------------------------------------------|
|product_id    | STRING  |Unique identifier                         |
|name          | STRING  |Product name                              |
|category      | STRING  |Product classification                    |
|base_price    | FLOAT   |Unit price                                |
|supplier_id   | STRING  |Unique supplier identifer                 |

Schema for `store`
|Field name    |Type     |Description                               | 
|--------------|---------|------------------------------------------|
|store_id      | STRING  |Unique identifier                         |
|location      | STRING  |Store location                            |
|size          | INT     |Store size                                |
|manager       | STRING  |Name of manager                           |

Schema for `transaction`
|Field name    |Type     |Description                               | 
|--------------|---------|------------------------------------------|
|transaction_id| STRING  |Unique identifier                         |
|product_id    | STRING  |Product identifier                        |
|timestamp     | STRING  |Time of transaction                       |
|quantity      | INT     |Number of units                           |
|unit_price    | FLOAT   |Price of product                          |
|store_id      | STRING  |Store identifer                           |

Schema for `inventory`
|Field name     |Type     |Description                               | 
|---------------|---------|------------------------------------------|
|inventory_id   | STRING  |Unique identifier                         |
|product_id     | STRING  |Product identifier                        |
|timestamp      | STRING  |Time of transaction                       |
|quantity_change| INT     |Change in product quantity                |
|store_id       | STRING  |Store identifer                           |


## High level architecture
![High level architecture](assets/high_level_architecture.png)

## End to end data flow chart
![End to end dataflow](assets/end_to_end_dataflow.png)

## Streamlit demo: US retail dashboard
[![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://us-retail-dashboard.streamlit.app/)

The data used for this demo is artifically generated using Python scripts

## Steps to reproduce the project
**1.** Prerequisites:
<details>
<summary>Google Cloud Platform Account</summary>

Note - If you have already done these steps then it is not required.

- Sign up for a free account [here](https://cloud.google.com/free/), and enable billing.
- Create your project
- Create a service account under IAM & Admin
- Grant the following roles - Owner + Storage Admin + Storage Object Admin + Service Usage Admin
- Enable the following apis - 
    `serviceusage` [here](https://console.developers.google.com/apis/api/serviceusage.googleapis.com/)
- Click Add keys, and then crete new key. Download the JSON file and store it in a suitable location locally

</details>

<details>
<summary>Google Cloud SDK - Optional</summary>

Installation instruction [here](https://cloud.google.com/sdk/docs/install-sdk).

</details>

Enable Google authentication - Optional
```shell
export GOOGLE_APPLICATION_CREDENTIALS=<path/to/your/service-account-authkeys>.json
gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS
gcloud auth application-default login
```

<details>
<summary>Install Docker for your OS</summary>

Installation instructions[here](https://docs.docker.com/engine/install/)
</details>

<details>
<summary>Install Terraform</summary>

You can view the [installation instructions for Terraform here](https://developer.hashicorp.com/terraform/downloads?ajs_aid=f70c2019-1bdc-45f4-85aa-cdd585d465b4&product_intent=terraform)

</details>

**2.** Clone the repository:
```shell
git clone https://github.com/sl2902/retail_data_pipeline.git
```

**3.** Change the working directory:
```shell
cd retail_data_pipeline/
```

**4.** Rename the env.template file to `.env`:
```shell
mv env.template .env
```

4.1 Fill in the blanks to the following environment variables in the `.env` file and save them:
```shell
project_id=
bucket_name=
dataset=
SERVICE_ACCOUNT_FILENAME=
HOST_GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/${SERVICE_ACCOUNT_FILENAME}
```

**5.** Build and enable the virtual environment:
```shell
make setup
```

**6.** Start Docker desktop and run docker compose:
```shell
make docker_build
```

**7.** Load environment variables into the project directory:
```shell
source .env
```

**8.** Create GCP Storage bucket and enable required Cloud APIs using Terraform:
```shell
cd terraform
```

8.1 Initialize terraform:
```shell
terraform init
```

8.2 Plan terraform:
```shell
terraform plan
```

8.3 Apply terraform:
```shell
terraform apply
```
Enter "yes" when prompted for a value to the following "Do you want to perform these actions?"
Note - On some occassions, the following api [cloudresourcemanager](https://console.developers.google.com/apis/api/cloudresourcemanager.googleapis.com/overview?project=556382348091). However, on checking the console, the API appears enabled. In such cases ignore the error. Otherwise, manually enable it via the console.

8.4 Return to project root folder:
```shell
cd ../
```

**9.** Launch Airflow UI. username - `airflow`. password - `airflow`:
```shell
http://localhost:8080
```

9.1 Run sanity check to see whether the dags are available; there should be 5 dags, which are paused:
```shell
make dag_list
```
Note - if you haven't provided the `SERVICE_ACCOUNT_FILENAME` and `project_id`, it will fail to add the connection

9.2 Create service account in Airflow connections:
```shell
make add_gcp_service_account_airflow
```

9.3 Unpause all dags:
```shell
make dag_unpause_all_dags
```
Note: Sometimes it takes a while to unpause the DAGs. Run the `make dag_list` command to check whether the `paused` field is set to `False`.
You can also manually unpause the dags from the Airflow UI home page; it is located to the left of the dag name.

9.4 Run the retail data [pipeline](assets/airflow/dag_dependencies.png) dag:
```shell
make dag_run_retail_data_pipeline
```

9.4.1 Check the status of job either on the CLI or via the Airflow UI by clicking on the respective dag:
```shell
make dag_chk_status_run_retail_data_pipeline
```

**10.** The instructions below can be followed if you would like to run the dags one by one:

<details>
<summary>Steps to run dags one by one</summary>

Note - If the environment is in an intermediate state, it is advised to clear up the intermediate objects before
runing the steps below. Otherwise, you may experience unexpected results. To start afresh, follow the steps shown
at the bottom of the page to destroy Terraform resources and then clean the Docker environment. 
When starting afresh, make sure the service usage Cloud API has been enabled. Rebuild the Docker environment and 
apply the Terraform resources.

10.1 Load the configuration files:
```shell
make dag_run_upload_config_files_to_gcs
```

10.1.1 Check the status of job either on the CLI or via the Airflow UI by clicking on the respective dag:
```shell
make dag_chk_status_upload_config_files_to_gcs
```

10.2 Generate and load dimensional data to BQ:
```shell
make dag_run_load_mock_dim_data_bq
```

10.2.1 Check the status of job either on the CLI or via the Airflow UI by clicking on the respective dag:
```shell
make dag_chk_status_load_mock_dim_data_bq
```

10.3 Setup the pub/sub lite infra
```shell
make dag_run_setup_pubsublite_infra
```

10.3.1 Check the status of job either on the CLI or via the Airflow UI by clicking on the respective dag:
```shell
make dag_chk_status_setup_pubsublite_infra
```

10.4 Generate transaction and inventory history for 3 months and stream the data:
```shell
make dag_run_publish_stream_to_bq
```

10.4.1 Check the status of job either on the CLI or via the Airflow UI by clicking on the respective dag:
```shell
make dag_chk_status_publish_stream_to_bq
```

10.5 Transform the data using dbt:
```shell
make dag_run_build_dbt_model
```

10.5.1 Check the status of job either on the CLI or via the Airflow UI by clicking on the respective dag:
```shell
make dag_chk_status_build_dbt_model
```

</details>

**11.** Run the streamlit dashboard:
```shell
make run_streamlit
```
Note - Because you may not have updated the secrets file required by Streamlit to connect to Bigquery, you may
see a message on the dashboard, you can ignore it as the script will fall back on the service account file
defined in the `.env` file. If you want to avoid the warning message, then you will need to create a `.streamlit`
folder in the project directory and a corresponding `secrets.toml` with the following contents drawn from your
gcp service account file
```
[gcp_service_account]

type = "service_account"
project_id = "PROJECT_ID"
private_key_id = "KEY_ID"
private_key = "-----BEGIN PRIVATE KEY-----\nPRIVATE_KEY\n-----END PRIVATE KEY-----"
client_email = "SERVICE_ACCOUNT_EMAIL"
client_id = "CLIENT_ID"
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://accounts.google.com/o/oauth2/token"
auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
client_x509_cert_url = "https://www.googleapis.com/robot/v1/metadata/x509/SERVICE_ACCOUNT_EMAIL"
```

**12.** Simulate generating transactions and inventories in real-time and stream the data for 5 seconds:
```shell
make dag_run_publish_stream_to_bq_sec
```

12.1 Check the status of job either on the CLI or via the Airflow UI by clicking on the respective dag:
```shell
make dag_chk_status_publish_stream_to_bq_sec
```

12.2 Rerun the dbt job:
```shell
make dag_run_build_dbt_model
```

12.3 Check the updated streamlit dashboard
```shell
make run_streamlit
```

12.4 Clean the environment
```shell
make docker_clean
```

**13.** Destroy terraform resources:
```shell
cd terraform && terraform destroy && cd ../
```

13.1 Remove the remaining dependent APIs either via the console or after installing gcloud sdk and initializing the project:
```shell
  gcloud services list --format="value(config.name)" \
| xargs -I{} gcloud services disable --force {}
```

## References
[1] [Pub/Sub Lite Spark Connector](https://github.com/googleapis/java-pubsublite-spark)<br>
[2] [Airflow Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)<br>
[3] [dbt Materialized View](https://docs.getdbt.com/docs/build/materializations)<br>
[4] [Add GCP Credentials via Airfolw CLI](https://stackoverflow.com/questions/50040717/add-gcp-credentials-to-airflow-via-command-line)<br>
[5] [Disable all GCP APIs via CLI](https://stackoverflow.com/questions/72806856/how-to-disable-all-enabled-apis-services)<br>
[6] [Enable service account roles using Terraform](https://stackoverflow.com/questions/55737583/using-terraform-to-create-a-service-account-with-iam-roles)<br>
[7] [Using Airflow decorators instead of Jinja](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/operators.html)<br>


