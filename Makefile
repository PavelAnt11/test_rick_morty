# Makefile
setup:  create-dir install
create-dir:
	mkdir -p  ./logs ./plugins ./config

#env-prepare:
	#echo -e "AIRFLOW_UID=$(id -u)" > .env

install:
	docker compose up airflow-init

start: start-airflow start-postgres
start-airflow:
	docker compose -f docker-compose-airflow.yaml up -d
start-postgres:
	docker compose -f docker-compose-postgres.yaml up -d
