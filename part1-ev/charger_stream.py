import json
import os
import time
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

BUCKET = os.environ["S3_BUCKET"]
REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
BOOTSTRAP_SERVERS = "kafka1:9092,kafka2:9092,kafka3:9092"
TOPIC = "ev-charging-sessions"

BRONZE_PATH = f"s3a://{BUCKET}/bronze/ev-charging-sessions/"
SILVER_PATH = f"s3a://{BUCKET}/silver/ev-charging-sessions/"
GOLD_PATH = f"s3a://{BUCKET}/gold/ev_charging_agg/"
CHECKPOINT_PATH = f"s3a://{BUCKET}/checkpoints/ev-charging-sessions"

IDLE_LIMIT_SEC = 5400
START_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss"

SESSION_SCHEMA = StructType([
    StructField("stationName", StringType()),
    StructField("portNumber", IntegerType()),
    StructField("portType", StringType()),
    StructField("plugType", StringType()),
    StructField("startTime", StringType()),
    StructField("endTime", StringType()),
    StructField("totalDuration", StringType()),
    StructField("chargingDuration", StringType()),
    StructField("energyKwh", DoubleType()),
    StructField("endedBy", StringType()),
])


def create_spark():
    return (
        SparkSession.builder
        .appName("charger_stream_lcm")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{REGION}.amazonaws.com")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def to_seconds(column_name):
    parts = F.split(F.col(column_name), ":")
    return (
        parts.getItem(0).cast("int") * 3600
        + parts.getItem(1).cast("int") * 60
        + parts.getItem(2).cast("int")
    )


def add_location_group(df):
    return df.withColumn("locationGroup", F.regexp_replace(F.col("stationName"), r" #\d+$", ""))


def add_dt(df):
    return df.withColumn("dt", F.date_format(F.to_timestamp(F.col("startTime"), START_TIME_FORMAT), "yyyy-MM-dd"))


def to_silver(df):
    return (
        add_location_group(df)
        .withColumn("totalDurationSec", to_seconds("totalDuration"))
        .withColumn("chargingDurationSec", to_seconds("chargingDuration"))
        .withColumn("idleSec", F.col("totalDurationSec") - F.col("chargingDurationSec"))
    )


def send_slack(text):
    if not SLACK_WEBHOOK_URL:
        print(f"[Slack 미설정] {text}")
        return
    body = json.dumps({"text": text}).encode("utf-8")
    request = urllib.request.Request(
        SLACK_WEBHOOK_URL, data=body, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        response.read()
    time.sleep(1)


def write_gold(spark):
    gold_df = (
        spark.read.parquet(SILVER_PATH)
        .filter(F.col("idleSec") > IDLE_LIMIT_SEC)
        .groupBy("locationGroup")
        .agg(F.count("*").alias("anomaly_count"))
        .orderBy("locationGroup")
    )
    gold_df.coalesce(1).write.mode("overwrite").parquet(GOLD_PATH)
    total = gold_df.agg(F.sum("anomaly_count")).first()[0]
    print(f"[gold] 설치구역 {gold_df.count()}개, 이상 누적 {total}건 -> {GOLD_PATH}")


def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    batch_df.persist()

    bronze_df = add_dt(batch_df)
    bronze_df.write.mode("append").partitionBy("dt").parquet(BRONZE_PATH)

    silver_df = add_dt(to_silver(batch_df))
    silver_df.write.mode("append").partitionBy("dt").parquet(SILVER_PATH)

    alerts = silver_df.filter(F.col("idleSec") > IDLE_LIMIT_SEC).orderBy("startTime").collect()
    for row in alerts:
        send_slack(
            f"[포트 점유 이상] {row['stationName']} (설치구역: {row['locationGroup']} "
            f"/ 시작 {row['startTime']} / idleSec={row['idleSec']})"
        )

    print(f"[batch {batch_id}] bronze/silver {batch_df.count()}건 적재, 포트 점유 이상 {len(alerts)}건 알림")
    write_gold(batch_df.sparkSession)
    batch_df.unpersist()


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 60)
        .load()
        .select(F.from_json(F.col("value").cast("string"), SESSION_SCHEMA).alias("data"))
        .select("data.*")
    )

    (
        events.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/bronze_silver")
        .trigger(processingTime="10 seconds")
        .start()
    )

    (
        add_location_group(events)
        .groupBy("locationGroup")
        .count()
        .orderBy("locationGroup")
        .writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .option("numRows", 50)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/agg")
        .trigger(processingTime="10 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
