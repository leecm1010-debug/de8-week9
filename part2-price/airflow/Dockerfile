FROM apache/airflow:2.10.5-python3.8

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
COPY --from=apache/spark:3.5.3 /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="/opt/java/openjdk/bin:${PATH}"

USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow==2.10.5" \
    "apache-airflow-providers-apache-spark==4.11.0" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.8.txt" \
    && pip install --no-cache-dir --no-deps "pyspark==3.5.3"
