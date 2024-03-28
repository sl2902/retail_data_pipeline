"""
Build dbt models
"""
from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from datetime import datetime, timedelta, timezone
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash_operator import (
    BashOperator
)
import os

DBT_CWD = os.environ.get("DBT_CWD", "opt/airflow/dags/dbt")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
    "max_active_runs": 1
}
dag = DAG(
    'build_dbt_model',
    default_args=default_args,
    description="Create dbt models in BQ",
    schedule_interval="@daily",
    start_date=datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0),
    tags=["dev"]
)

run_date = "{{ dag_run.conf['execution_date'] if dag_run and dag_run.conf and 'execution_date' in dag_run.conf else ds_nodash }}"
run_dbt_deps = BashOperator(
    task_id="run_dbt_deps",
    bash_command="dbt deps",
    cwd=DBT_CWD,
    dag=dag
)

build_stg_and_fact_tbls = BashOperator(
    task_id="build_stg_and_fact_tbls",
    bash_command="dbt build --full-refresh",
    cwd=DBT_CWD,
    dag=dag
)

run_dbt_deps >> build_stg_and_fact_tbls