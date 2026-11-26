https://github.com/leecm1010-debug/de8-week9

# 데엔8기 9주차 — Kafka(AWS EC2) 실시간 스트리밍 · S3 Bronze/Silver/Gold 배치 파이프라인

- 식별자(이니셜): `lcm`
- part1-ev: 전기차 충전소 운영 모니터링 (Q1~Q5)
- part2-price: 식료품 가격 배치 인텔리전스 (Q6~Q10)
- 실행 환경: EC2 t3.large 1대 (Ubuntu 22.04) · docker-compose (Kafka 3 broker, kafka-ui, Spark master/worker, Airflow)
- AWS 접근: EC2 IAM Role `de9-ec2-lcm` (액세스 키 미사용)
