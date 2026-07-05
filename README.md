Setup for Airflow + Kafka (KRaft) + Spark (master+worker) docker-compose

Summary
- Airflow (2.8.1) built from `airflow/Dockerfile` with Spark binaries included
- Kafka running in KRaft mode (bitnami/kafka) on service name `kafka:9092`
- Spark master `spark-master:7077` and worker `spark-worker`
- PostgreSQL used as Airflow metadata DB
- All services on Docker network `fraud-net` so service name resolution works

Quick start
1. Build and start services:

```bash
docker compose up --build -d
```

2. Initialize the Airflow DB and create an admin user (run once):

```bash
docker compose exec airflow airflow db upgrade
docker compose exec airflow airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
```

3. Create the Airflow connection to the Spark master (inside the airflow container):

```bash
docker compose exec airflow airflow connections add 'spark_default' --conn-type 'spark' --conn-host 'spark://spark-master:7077'
```

4. Create Kafka topic and produce some messages to `transactions_pca` for the existing fraud streaming application:

```bash
docker compose exec kafka kafka-topics.sh --create --topic transactions_pca --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
docker compose exec kafka kafka-console-producer.sh --broker-list kafka:9092 --topic transactions_pca
> {"transaction_id": "1", "event_time": "2026-01-01T00:00:00Z", "amount": 100.0, "features": {"V1": 0.1, "V2": 0.2}, "label": 0, "ingestion_time": "2026-01-01T00:00:00Z"}
```

Notes & rationale
- Use Postgres instead of SQLite for the Airflow metadata DB to allow LocalExecutor and support concurrent tasks.
- Install Spark binaries in the Airflow image so `spark-submit` is available locally; the operator will submit to the remote Spark master (`spark://spark-master:7077`).
- Add Redis as a managed service so your legacy hold/release patterns can use service-based Redis host resolution.
- Use the `org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.2` package via `--packages` to enable Kafka reads in Spark 3.5.x.

Volumes & folder layout (recommended)
- `airflow/dags` -> `/opt/airflow/dags` (Airflow DAGs)
- `airflow/plugins` -> `/opt/airflow/plugins` (Airflow plugins)
- `airflow/logs` -> `/opt/airflow/logs` (Airflow logs)
- `streaming` -> `/opt/project/streaming` (Spark streaming application source)
- `model` -> `/opt/project/model` (trained model artifacts)
- `data` -> `/opt/project/data` (shared raw and enriched data)
- `checkpoints` -> `/opt/project/checkpoints` (Spark checkpoint locations)
- `common` -> `/opt/project/common` (shared utilities and Redis helper)

Networking
- All services are attached to the `fraud-net` bridge network by Compose; use service names `kafka`, `spark-master`, `spark-worker`, `postgres`, `redis`, and `airflow` when configuring clients.

Spark Kafka dependency
- Use the Maven coordinate: `org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.2` (pass via `--packages`).

Windows + Docker Desktop tips
- If Docker Desktop is running with WSL2 backend, prefer mounting paths using WSL paths (e.g. place repo inside your WSL home) — Windows file sharing can be slow.
- If you see networking errors between containers, ensure Compose uses the same network and that Docker Desktop's DNS settings are default.
- Long path and volume permission issues: enable shared drives or use named volumes to avoid Windows permission conflicts.
- If `kafka-console-producer.sh` fails to connect, check `KAFKA_CFG_ADVERTISED_LISTENERS` and map published ports.

Project directory (final)
```
.
├── airflow
│   ├── Dockerfile
│   ├── dags
│   │   ├── fraud_retrain_dag.py
│   │   ├── spark_monitor_dag.py
│   │   └── ...
│   └── plugins
├── batch
│   └── train_fraud_model.py
├── common
│   └── redis_client.py
├── docker-compose.yml
├── requirements.txt
├── spark
│   └── Dockerfile
├── streaming
│   └── spark_streaming_fraud.py
├── model
├── data
└── checkpoints
```
