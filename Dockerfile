# Pin to Spark 3.4.x — matches spark-sql-kafka connector version in spark_utils.py.
# Upgrading this tag requires also updating _KAFKA_SPARK_PKG in src/spark_utils.py.
FROM jupyter/pyspark-notebook:spark-3.4.1

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    git \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt

USER ${NB_UID}

RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /home/jovyan/work
