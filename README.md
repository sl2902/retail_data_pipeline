# US E-commerce retail data pipeline - transaction and inventory analysis
This project was created as part of the finale for the DataTalksClub - Data Engineering Zoomcamp 6 week course. The data used for this project
is completely artifical; they were generated using Python scripts.
There are 4 tables in total: 
1) Two dimensional tables which contains `product` and `store` data
2) Two fact tables which contain `inventories` and `transactions` data
In order to create a dashboard, some historical data is generated starting Jan 2024 - March 2024. The Airflow job can be run every hour or so to simulate the generation of transactions and inventories every few seconds.

## Problem Description
The goal of the project is to build an end to end streaming data pipeline that will produce data, aggregated to one second interval, of a fictitious US E-commerce retail giant. The dimensional data and fact data are store in Google Cloud Storage

## High level architecture
![High level architecture](assets/high_level_architecture.png)

## End to end data flow chart
![End to end dataflow](assets/end_to_end_dataflow.png)

## Streamlit demo: US retail dashboard
[![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://us-retail.streamlit.app/)


The data used for this demo is artifically generated using Python scripts
