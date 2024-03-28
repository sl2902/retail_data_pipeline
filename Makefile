SHELL:=/bin/bash
include .env

setup: ## Setup the development environment
	@pipenv install --dev; pipenv run pre-commit install; pipenv shell; pipenv sync

docker_build: ## Docker build and run the Airflow pipeline
	@docker-compose up --build -d

docker_logs: ## Check data pipeline logs
	@docker-compose logs -f

docker_restart: ## Restart Airflow pipeline environment
	@docker-compose restart

docker_stop: ## Stop Airflow pipeline environment
	@docker-compose down

dag_list: ## List DAGs
	@docker-compose run airflow-cli airflow dags list

dag_clear_all_jobs: ## Clear all dags
	@docker-compose run airflow-cli airflow tasks clear -y upload_config_files_to_gcs
	@docker-compose run airflow-cli airflow tasks clear -y load_mock_dim_data_bq
	@docker-compose run airflow-cli airflow tasks clear -y setup_pubsublite_infra
	@docker-compose run airflow-cli airflow tasks clear -y build_dbt_model
	@docker-compose run airflow-cli airflow tasks clear -y publish_stream_to_bq

add_gcp_service_account_airflow: ## Add GCP service account to Airflow connection
	@docker compose run airflow-cli airflow connections add $(GOOGLE_CONN_ID) \
    	--conn-type=google_cloud_platform \
		--conn-description="Google service account credentials" \
    	--conn-extra='{"extra__google_cloud_platform__key_path": "$(GOOGLE_APPLICATION_CREDENTIALS)", "extra__google_cloud_platform__project": "$(project_id)", "extra__google_cloud_platform__scope": "https://www.googleapis.com/auth/cloud-platform"}'

unpause_all_dags: ## Unpause the dags
	@docker compose run airflow-cli airflow dags unpause upload_config_files_to_gcs
	@docker compose run airflow-cli airflow dags unpause load_mock_dim_data_bq
	@docker compose run airflow-cli airflow dags unpause setup_pubsublite_infra
	@docker compose run airflow-cli airflow dags unpause publish_stream_to_bq
	@docker compose run airflow-cli airflow dags unpause build_dbt_model

dag_run_retail_data_pipeline: ## Run the end to end retail data pipeline
	@docker compose run airflow-cli airflow dags trigger upload_config_files_to_gcs
	@docker compose run airflow-cli airflow dags list-runs -d upload_config_files_to_gcs --state running

dag_chk_status_run_retail_data_pipeline: ## Check status of dag - End to end retail data pipeline
	@docker compose run airflow-cli airflow dags list-runs -d upload_config_files_to_gcs | head -3
	@docker compose run airflow-cli airflow dags list-runs -d load_mock_dim_data_bq | head -3
	@docker compose run airflow-cli airflow dags list-runs -d publish_stream_to_bq | head -3
	@docker compose run airflow-cli airflow dags list-runs -d setup_pubsublite_infra | head -3
	@docker compose run airflow-cli airflow dags list-runs -d build_dbt_model | head -3

dag_run_upload_config_files_to_gcs: ## Upload config files to GCS
	@docker compose run airflow-cli airflow dags unpause upload_config_files_to_gcs
	@docker compose run airflow-cli airflow dags trigger upload_config_files_to_gcs
	@docker compose run airflow-cli airflow dags list-runs -d upload_config_files_to_gcs --state running

dag_chk_status_upload_config_files_to_gcs: ## Check status of dag - Upload config files to GCS
	@docker compose run airflow-cli airflow dags list-runs -d upload_config_files_to_gcs | head -3

dag_run_load_mock_dim_data_bq: ## Load mock dimensional data to BQ
	@docker compose run airflow-cli airflow dags unpause load_mock_dim_data_bq
	@docker compose run airflow-cli airflow dags trigger load_mock_dim_data_bq
	@docker compose run airflow-cli airflow dags list-runs -d load_mock_dim_data_bq --state running

dag_chk_status_load_mock_dim_data_bq: ## Check status of dag - Load mock dimensional data to BQ
	@docker compose run airflow-cli airflow dags list-runs -d load_mock_dim_data_bq | head -3

dag_run_setup_pubsublite_infra: ## Setup Pubsublite infra
	@docker compose run airflow-cli airflow dags unpause setup_pubsublite_infra
	@docker compose run airflow-cli airflow dags trigger setup_pubsublite_infra
	@docker compose run airflow-cli airflow dags list-runs -d setup_pubsublite_infra --state running

dag_chk_status_setup_pubsublite_infra: ## Check status of dag - Setup Pubsublite infra
	@docker compose run airflow-cli airflow dags list-runs -d setup_pubsublite_infra | head -3

dag_run_publish_stream_to_bq: ## Start producer and consumer and publish stream to BQ; the first run is to produce historical data
	@docker compose run airflow-cli airflow dags unpause publish_stream_to_bq
	@docker compose run airflow-cli airflow dags trigger publish_stream_to_bq --conf '{"units": "minutes", "duration": 30}'
	@docker compose run airflow-cli airflow dags list-runs -d publish_stream_to_bq --state running

dag_chk_status_publish_stream_to_bq: ## Check status of dag - Start producer and consumer and publish stream to BQ
	@docker compose run airflow-cli airflow dags list-runs -d publish_stream_to_bq | head -3

dag_run_build_dbt_model: ## Run dbt model
	@docker compose run airflow-cli airflow dags unpause build_dbt_model
	@docker compose run airflow-cli airflow dags trigger build_dbt_model
	@docker compose run airflow-cli airflow dags list-runs -d build_dbt_model --state running

dag_chk_status_build_dbt_model: ## Check status of dag - Run dbt model
	@docker compose run airflow-cli airflow dags list-runs -d build_dbt_model | head -3

dag_run_publish_stream_to_bq_sec: ## Start producer and consumer and publish stream to BQ; this job runs for 5 seconds
	@docker compose run airflow-cli airflow dags unpause publish_stream_to_bq
	@docker compose run airflow-cli airflow dags trigger publish_stream_to_bq --conf '{"units": "seconds", "duration": 5}'
	@docker compose run airflow-cli airflow dags list-runs -d publish_stream_to_bq --state running

dag_chk_status_publish_stream_to_bq_sec: ## Check status of dag - Start producer and consumer and publish stream to BQ
	@docker compose run airflow-cli airflow dags list-runs -d publish_stream_to_bq | head -3

run_streamlit: ## Start Streamlit dashboard
	@streamlit run streamlit/dashboard.py

docker_clean: ## Clean Docker environment
	# @docker container stop $(docker container ls -q)
	# @docker container prune
	@docker rmi -f image $(docker image ls -q)
	@docker-compose down -v
	@docker compose down --volumes --rmi all

help: ## Help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(filter-out .env, $(MAKEFILE_LIST)) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-45s\033[0m %s\n", $$1, $$2}'