![Python](https://img.shields.io/badge/Python-3.8-4B8BBE.svg?style=flat&logo=python&logoColor=FFD43B&labelColor=306998)
![PySpark](https://img.shields.io/badge/pySpark-3.3-E36B22?style=flat-square&logo=apachespark&logoColor=E36B22&labelColor=3C3A3E)
![BigQuery](https://img.shields.io/badge/BigQuery-3772FF?style=flat&logo=googlebigquery&logoColor=white&labelColor=3772FF)
![BigQuery](https://img.shields.io/badge/Pubsublite-3772FF?style=flat&logo=googlepubsublite&logoColor=white&labelColor=3772FF)
![dbt](https://img.shields.io/badge/dbt-1.7-262A38?style=flat&logo=dbt&logoColor=FF6849&labelColor=262A38)
![Docker](https://img.shields.io/badge/Docker-329DEE?style=flat&logo=docker&logoColor=white&labelColor=329DEE)
![Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)

![License](https://img.shields.io/badge/license-CC--BY--SA--4.0-31393F?style=flat&logo=creativecommons&logoColor=black&labelColor=white)

# US E-commerce retail data pipeline - transaction and inventory analysis
This project was created as part of the finale for the DataTalksClub - Data Engineering Zoomcamp 6 week course. The data used for this project
is completely artifical; they were generated using Python scripts.
There are 4 tables in total: 
1) Two dimensional tables which contains `product` and `store` data
2) Two fact tables which contain `inventories` and `transactions` data
In order to create a dashboard, some historical data is generated starting Jan 2024 - March 2024. The Airflow job can be run every hour or so to simulate the generation of transactions and inventories every few seconds.

## Problem Description
The goal of the project is to build an end to end streaming data pipeline that will produce data, aggregated to one second interval, of a fictitious US E-commerce retail giant. The dimensional data and fact data are store in Google Cloud Storage. Pubsublite is used to produce and consume streaming transactions and inventories, which is processed using Apache Spark; the final results are stored in Bigquery and these are transformed using dbt; the analysis is displayed on Streamlit.

## Technology Stack 
The following technologies have been used
- [Google Cloud Storage (GCS)](https://cloud.google.com/storage?hl=en)
- [Google BigQuery](https://cloud.google.com/bigquery?hl=en)
- [Google Pubsublite](https://cloud.google.com/pubsub/lite/docs)
- Terraform<br>
- [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/start.html)
- [Apache Spark](https://spark.apache.org/docs/latest/api/python/user_guide)
- [dbt](https://github.com/dbt-labs/dbt-core)
- [Docker](https://docs.docker.com/get-docker/)
- [Pipenv](https://pipenv.pypa.io/en/latest/)
- [Streamlit](https://streamlit.io/)

## High level architecture
![High level architecture](assets/high_level_architecture.png)

## End to end data flow chart
![End to end dataflow](assets/end_to_end_dataflow.png)

## Streamlit demo: US retail dashboard
[![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://us-retail.streamlit.app/)


The data used for this demo is artifically generated using Python scripts