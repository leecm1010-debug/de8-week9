import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

BUCKET = os.environ["S3_BUCKET"]
REGION = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2"))
BOOTSTRAP_SERVERS = "kafka1:9092,kafka2:9092,kafka3:9092"
TOPIC = "product-prices"

BRONZE_PATH = f"s3a://{BUCKET}/bronze/product-prices/"
SILVER_PATH = f"s3a://{BUCKET}/silver/product-prices/"
GOLD_PATH = f"s3a://{BUCKET}/gold/grocery_price_change/"

PRICE_SCHEMA = StructType([
    StructField("item_name", StringType()),
    StructField("market", StringType()),
    StructField("year", IntegerType()),
    StructField("month", IntegerType()),
    StructField("price", StringType()),
    StructField("currency", StringType()),
    StructField("unit", StringType()),
])


def create_spark():
    return (
        SparkSession.builder
        .appName("price_batch_lcm")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{REGION}.amazonaws.com")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_kafka(spark):
    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )


def to_bronze(raw_df):
    return raw_df.select(
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        "topic",
        "partition",
        "offset",
        "timestamp",
    )


def to_silver(bronze_df):
    return (
        bronze_df
        .select(F.from_json(F.col("value"), PRICE_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("price", F.col("price").cast("double"))
        .filter(F.col("price").isNotNull() & (F.col("price") > 0))
    )


def monthly_stats(silver_df):
    return (
        silver_df
        .groupBy("item_name", "month")
        .agg(
            F.round(F.avg("price"), 2).alias("avg_price"),
            F.max("price").alias("max_price"),
            F.min("price").alias("min_price"),
        )
        .orderBy("item_name", "month")
    )


def price_change(silver_df):
    item_window = Window.partitionBy("market", "item_name").orderBy("year", "month")
    market_window = Window.partitionBy("market").orderBy(F.abs(F.col("change_rate")).desc())

    return (
        silver_df
        .withColumn("price_prev", F.lag("price").over(item_window))
        .filter((F.col("year") == 2016) & (F.col("month") == 12))
        .select(
            "market",
            "item_name",
            F.col("price_prev").alias("price_nov"),
            F.col("price").alias("price_dec"),
        )
        .withColumn(
            "change_rate",
            F.round((F.col("price_dec") - F.col("price_nov")) / F.col("price_nov") * 100, 2),
        )
        .withColumn("change_rank", F.rank().over(market_window))
    )


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    bronze_df = to_bronze(read_kafka(spark)).cache()
    bronze_df.write.mode("overwrite").parquet(BRONZE_PATH)
    print(f"Bronze 적재 : {bronze_df.count()}건 -> {BRONZE_PATH}")

    silver_df = to_silver(bronze_df).cache()
    silver_df.write.mode("overwrite").parquet(SILVER_PATH)
    print(f"Silver 적재 : {silver_df.count()}건 -> {SILVER_PATH}")

    print("=== Q7 품목별 월별 평균/최고/최저가 ===")
    monthly_stats(silver_df).show(100, truncate=False)

    gold_df = price_change(silver_df).cache()
    gold_df.coalesce(1).write.mode("overwrite").parquet(GOLD_PATH)
    print(f"Gold 적재 : {gold_df.count()}건 -> {GOLD_PATH}")

    print("--- Q8 도시별 변동률 1위 ---")
    (
        gold_df
        .filter(F.col("change_rank") == 1)
        .orderBy("market")
        .show(100, truncate=False)
    )

    spark.stop()


if __name__ == "__main__":
    main()
