import os
from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

BUCKET = os.environ.get("S3_BUCKET", "")
REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")
DATABASE = "de9_lcm"
TABLE = "grocery_price_change"
ATHENA_OUTPUT = f"s3://{BUCKET}/athena-results/"

CREATE_TABLE_SQL = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.{TABLE} (
    market string,
    item_name string,
    price_nov double,
    price_dec double,
    change_rate double,
    change_rank int
)
STORED AS PARQUET
LOCATION 's3://{BUCKET}/gold/{TABLE}/'
"""

TOP1_SQL = f"""
SELECT market, item_name, price_nov, price_dec, change_rate, change_rank
FROM {DATABASE}.{TABLE}
WHERE change_rank = 1
ORDER BY market
"""

with DAG(
    dag_id="price_pipeline_lcm",
    start_date=datetime(2026, 11, 1),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["de9"],
) as dag:
    spark_batch = SparkSubmitOperator(
        task_id="spark_batch",
        conn_id="spark_default",
        application="/opt/de9/part2-price/price_batch.py",
        name="price_batch_lcm",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4",
        conf={
            "spark.driver.host": "airflow",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.jars.ivy": "/tmp/.ivy2",
        },
        driver_memory="768m",
        executor_memory="1g",
        total_executor_cores=2,
    )

    register_table = AthenaOperator(
        task_id="register_table",
        query=CREATE_TABLE_SQL,
        database=DATABASE,
        output_location=ATHENA_OUTPUT,
        workgroup="primary",
        region_name=REGION,
    )

    query_top1 = AthenaOperator(
        task_id="query_top1",
        query=TOP1_SQL,
        database=DATABASE,
        output_location=ATHENA_OUTPUT,
        workgroup="primary",
        region_name=REGION,
    )

    spark_batch >> register_table >> query_top1
