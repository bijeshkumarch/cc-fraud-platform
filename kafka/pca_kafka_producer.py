# producer/pca_kafka_producer.py
import csv
import json
import time
from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer
import platform


BASE_DIR = "/mnt/c/Users/Bijesh Kumar/documents/fraud-platform"

os_name = platform.system()
if os_name == "Windows":
    BASE_DIR = "C:/Users/Bijesh Kumar/Documents/fraud-platform"


BASE_TIME = datetime(2013, 9, 3, tzinfo=timezone.utc)

def delivery_report(err, msg):
    """Called once for each message produced to indicate successful or failed delivery"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

conf = {
    'bootstrap.servers': 'localhost:9092',
}

producer = Producer(conf)


with open(rf"{BASE_DIR}/data/source/creditcard.csv") as f:
    reader = csv.DictReader(f)
    for i, row in enumerate(reader):
        event_time = BASE_TIME + timedelta(seconds=float(row["Time"]))

        event = {
            "transaction_id": f"txn_{i}",
            "event_time": event_time.isoformat(),
            "amount": float(row["Amount"]),
            "features": {f"V{k}": float(row[f"V{k}"]) for k in range(1, 29)},
            "label": int(row["Class"]),
            "ingestion_time": datetime.now(timezone.utc).isoformat()
        }

        producer.produce(
            topic="transactions_pca",
            key=None,
            value=json.dumps(event).encode('utf-8'),
            callback=delivery_report
        )

        # simulate ~50 TPS
        time.sleep(0.02)
        
        # for testing only 3 records
        if i == 2:
            break

producer.flush()

