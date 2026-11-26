import sys

from kafka import KafkaAdminClient, KafkaConsumer

BOOTSTRAP_SERVERS = ["localhost:19092", "localhost:19093", "localhost:19094"]


def main():
    if len(sys.argv) < 2:
        print("사용법: python kafka_lag_monitor.py <group_id>")
        sys.exit(1)
    group_id = sys.argv[1]

    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP_SERVERS)
    try:
        committed = admin.list_consumer_group_offsets(group_id)
        partitions = sorted(committed.keys(), key=lambda tp: (tp.topic, tp.partition))
        end_offsets = consumer.end_offsets(partitions)

        print(f"group = {group_id}")
        print(f"{'topic':<25}{'partition':>10}{'current-offset':>16}{'log-end-offset':>16}{'lag':>8}")
        total_lag = 0
        for tp in partitions:
            current = committed[tp].offset
            log_end = end_offsets[tp]
            lag = log_end - current
            total_lag += lag
            print(f"{tp.topic:<25}{tp.partition:>10}{current:>16}{log_end:>16}{lag:>8}")
        print(f"{'TOTAL LAG':<67}{total_lag:>8}")
    finally:
        consumer.close()
        admin.close()


if __name__ == "__main__":
    main()
