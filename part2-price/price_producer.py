import csv
import json
import os
from collections import Counter

from kafka import KafkaProducer
from kafka.partitioner import DefaultPartitioner

BOOTSTRAP_SERVERS = ["localhost:19092", "localhost:19093", "localhost:19094"]
TOPIC = "product-prices"
CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "india_grocery_prices.csv")

REGION_PARTITIONS = {
    "Delhi": 0, "Jaipur": 0, "Lucknow": 0, "Kanpur": 0,
    "Mumbai": 1, "Ahmedabad": 1, "Bhopal": 1,
    "Hyderabad": 2, "Chennai": 2, "Patna": 2,
}


class RegionPartitioner:
    def __init__(self):
        self.default_partitioner = DefaultPartitioner()

    def __call__(self, key, all_partitions, available):
        market = key.decode("utf-8") if key is not None else None
        partition = REGION_PARTITIONS.get(market)
        if partition is not None and partition in all_partitions:
            return partition
        return self.default_partitioner(key, all_partitions, available)


def to_message(row):
    return {
        "item_name": row["item_name"],
        "market": row["market"],
        "year": int(row["year"]),
        "month": int(row["month"]),
        "price": row["price"],
        "currency": row["currency"],
        "unit": row["unit"],
    }


def main():
    with open(CSV_PATH, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        enable_idempotence=True,
        acks="all",
        partitioner=RegionPartitioner(),
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    futures = []
    try:
        for row in rows:
            message = to_message(row)
            futures.append(producer.send(TOPIC, key=message["market"], value=message))
    finally:
        producer.flush()
        producer.close()

    partition_counts = Counter(future.get(timeout=10).partition for future in futures)
    print(f"전송 완료 : {sum(partition_counts.values())}건 -> topic={TOPIC}")
    for partition in sorted(partition_counts):
        print(f"  partition {partition}: {partition_counts[partition]}건")


if __name__ == "__main__":
    main()
