import json
import sys
import time
from collections import Counter

from kafka import ConsumerRebalanceListener, KafkaConsumer

BOOTSTRAP_SERVERS = ["localhost:19092", "localhost:19093", "localhost:19094"]
TOPIC = "ev-charging-sessions"
GROUP_ID = "ev-consumer-lcm"
IDLE_LIMIT_SEC = 60
PROCESS_DELAY_SEC = 0.05


class RebalanceListener(ConsumerRebalanceListener):
    def __init__(self, consumer):
        self.consumer = consumer

    def on_partitions_revoked(self, revoked):
        if revoked:
            self.consumer.commit()
            print(f"[반납 파티션] {sorted(tp.partition for tp in revoked)}")

    def on_partitions_assigned(self, assigned):
        print(f"[할당 파티션] {sorted(tp.partition for tp in assigned)}")


def main():
    instance_no = sys.argv[1] if len(sys.argv) > 1 else "1"

    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    consumer.subscribe([TOPIC], listener=RebalanceListener(consumer))
    print(f"group_id={GROUP_ID} 소비 시작 (consumer #{instance_no})")

    counts = Counter()
    processed_partitions = set()
    last_message_time = time.time()

    try:
        while time.time() - last_message_time < IDLE_LIMIT_SEC:
            records = consumer.poll(timeout_ms=1000)
            if not records:
                continue
            for tp, messages in records.items():
                for message in messages:
                    counts[message.value["endedBy"]] += 1
                    processed_partitions.add(tp.partition)
                    time.sleep(PROCESS_DELAY_SEC)
            consumer.commit()
            last_message_time = time.time()
    finally:
        consumer.close()

    print(f"===== consumer #{instance_no} =====")
    print(f"처리 파티션: {sorted(processed_partitions)}")
    for ended_by, count in sorted(counts.items()):
        print(f"  {ended_by:<22}: {count}")
    print(f"  {'합계':<20}: {sum(counts.values())}")


if __name__ == "__main__":
    main()
