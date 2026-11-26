import csv
import json
import os
import random
import time
from datetime import datetime

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = ["localhost:19092", "localhost:19093", "localhost:19094"]
TOPIC = "ev-charging-sessions"
CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "charger_sessions.csv")
START_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def load_sessions():
    with open(CSV_PATH, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    rows.sort(key=lambda r: datetime.strptime(r["startTime"], START_TIME_FORMAT))
    return rows


def to_message(row):
    return {
        "stationName": row["stationName"],
        "portNumber": int(row["portNumber"]),
        "portType": row["portType"],
        "plugType": row["plugType"],
        "startTime": row["startTime"],
        "endTime": row["endTime"],
        "totalDuration": row["totalDuration"],
        "chargingDuration": row["chargingDuration"],
        "energyKwh": float(row["energyKwh"]),
        "endedBy": row["endedBy"],
    }


def main():
    sessions = load_sessions()
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        enable_idempotence=True,
        acks="all",
        retries=10,
        max_in_flight_requests_per_connection=5,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    futures = []
    try:
        for row in sessions:
            message = to_message(row)
            futures.append(producer.send(TOPIC, key=message["stationName"], value=message))
            time.sleep(random.uniform(0.05, 0.2))
    finally:
        producer.flush()
        producer.close()

    sent = sum(1 for future in futures if future.succeeded())
    print(f"전송 완료 : {sent}건 -> topic={TOPIC}")


if __name__ == "__main__":
    main()
