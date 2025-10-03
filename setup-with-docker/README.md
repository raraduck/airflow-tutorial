# Running Airflow in Docker [link][site1]
[site1]: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

## 0. Prerequisites
The default amount of memory available for Docker on macOS is often not enough to get Airflow up and running. If enough memory is not allocated, it might lead to the webserver continuously restarting. You should allocate at least 4GB memory for the Docker Engine (ideally 8GB).

You can check if you have enough memory by running this command:
```bash
docker run --rm "debian:bookworm-slim" bash -c 'numfmt --to iec $(echo $(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE))))'
```


## 1. Fetching yaml
To deploy Airflow on Docker Compose, you should fetch docker-compose.yaml.
```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.1.0/docker-compose.yaml'
```

This file contains several service definitions:

- airflow-scheduler - The scheduler monitors all tasks and Dags, then triggers the task instances once their dependencies are complete.
- airflow-dag-processor - The Dag processor parses Dag files.
- airflow-api-server - The api server is available at http://localhost:8080.
- airflow-worker - The worker that executes the tasks given by the scheduler.
- airflow-triggerer - The triggerer runs an event loop for deferrable tasks.
- airflow-init - The initialization service.
- postgres - The database.
- redis - The redis - broker that forwards messages from scheduler to worker.

Optionally, you can enable flower by adding --profile flower option, e.g. docker compose --profile flower up, or by explicitly specifying it on the command line e.g. docker compose up flower.

flower - The flower app for monitoring the environment. It is available at http://localhost:5555.

All these services allow you to run Airflow with CeleryExecutor. For more information, see Architecture Overview.

## 2. Initializing Environment
### 2.1. Setting the right Airflow user
```bash
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
### 2.2. Initialize the database
On all operating systems, you need to run database migrations and create the first user account. To do this, run.
```bash
docker compose up airflow-init
```
The account created has the login airflow and the password airflow.

## 3. Running Airflow
Now you can start all services in background:
```bash
docker compose up -d
```

- Airflow 2.8+ / 3.x: from airflow.operators.empty import EmptyOperator
- Airflow 2.x ~ 3.x: from airflow.operators.python import PythonOperator
- schedule_interval → ❌ (Airflow 3.x에서 제거됨), schedule → ✅ 사용해야 함

## 4. Tips for Airflow Connection (CLI version)
```bash
airflow connections add 'my_s3' \
    --conn-type 's3' \
    --conn-login 'AWS_ACCESS_KEY_ID' \
    --conn-password 'AWS_SECRET_ACCESS_KEY' \
    --conn-extra '{"region_name": "ap-northeast-2"}'

airflow connections add 'my_databricks' \
    --conn-type 'databricks' \
    --conn-host 'https://<your-databricks-instance>' \
    --conn-login 'token' \
    --conn-password '<DATABRICKS_PERSONAL_ACCESS_TOKEN>'
```

## 5. Tips for CLI Control
```bash
#1. cli 명령 1회성
docker exec -it airflow-airflow-scheduler-1 airflow dags list

#2. cli mode 접속
docker exec -it airflow-airflow-scheduler-1 /bin/bash

#3. dag 목록 보기
airflow dags list

#4. dag import error 보기
airflow dags list-import-errors

#5. DAG 실행 트리거하기
airflow dags trigger sample_dag

#6. 실행 내역 확인
airflow dags list-runs -d sample_dag

#7. Task 목록 보기
airflow tasks list sample_dag
```