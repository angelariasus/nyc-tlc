"""
PySpark session factory and I/O helpers for the TLC Data Lake.
Includes the MongoDB Spark Connector configuration.
"""
from __future__ import annotations

import os
import glob
from functools import reduce

from pyspark.sql import DataFrame, SparkSession

from core.audit.logger import setup_logger
from core.config.settings import settings

logger = setup_logger("tlc.spark_utils")

# Mongo Spark Connector package (version matches PySpark 3.x in the container)
_MONGO_SPARK_PKG = "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"

# Kafka Spark SQL connector — must match the Spark version inside the container.
# Spark 3.4.x → use 3.4.1; Spark 3.5.x → use 3.5.0
_KAFKA_SPARK_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"


def get_spark(
    app_name: str | None = None,
    driver_memory: str | None = None,
    executor_memory: str | None = None,
) -> SparkSession:
    """
    Create or retrieve the active SparkSession.

    The session is pre-configured with:
    - MongoDB Spark Connector (read/write to all Medallion layers)
    - Snappy Parquet compression
    - Adaptive Query Execution (AQE) enabled
    - Dynamic partition overwrite mode

    Parameters
    ----------
    app_name:
        Overrides ``settings.SPARK_APP_NAME`` when provided.
    driver_memory / executor_memory:
        Override environment defaults when provided.

    Returns
    -------
    SparkSession
    """
    app_name       = app_name       or settings.SPARK_APP_NAME
    driver_memory  = driver_memory  or settings.SPARK_DRIVER_MEMORY
    executor_memory = executor_memory or settings.SPARK_EXECUTOR_MEMORY

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(settings.SPARK_MASTER)
        .config("spark.driver.memory",  driver_memory)
        .config("spark.executor.memory", executor_memory)
        .config("spark.sql.shuffle.partitions",              "8")
        .config("spark.sql.parquet.compression.codec",       "snappy")
        .config("spark.sql.adaptive.enabled",                "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode",  "dynamic")
        .config("spark.sql.parquet.enableVectorizedReader",  "false")
        .config("spark.sql.parquet.mergeSchema",             "false")
        .config("spark.ui.showConsoleProgress",              "false")
        # MongoDB Spark Connector
        .config("spark.jars.packages", _MONGO_SPARK_PKG)
        .config("spark.mongodb.read.connection.uri",  settings.mongo_uri())
        .config("spark.mongodb.write.connection.uri", settings.mongo_uri())
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info(
        f"[SPARK] Session ready | version={spark.version} "
        f"master={spark.sparkContext.master}"
    )
    return spark


def get_spark_streaming(
    app_name: str | None = None,
    driver_memory: str | None = None,
    executor_memory: str | None = None,
    kafka_bootstrap: str = "kafka:9092",
) -> SparkSession:
    """
    Create or retrieve a SparkSession with **both** the MongoDB Spark Connector
    and the Spark-Kafka connector loaded.

    Use this session for notebooks that use ``readStream`` from Kafka and
    write to MongoDB via ``foreachBatch``.

    Parameters
    ----------
    app_name:
        Overrides ``settings.SPARK_APP_NAME`` when provided.
    driver_memory / executor_memory:
        Override environment defaults when provided.
    kafka_bootstrap:
        Kafka bootstrap servers string (default: ``"kafka:9092"`` for Docker).

    Returns
    -------
    SparkSession
        Session pre-configured for Kafka streaming + MongoDB writes.

    Notes
    -----
    * **Never** call ``.count()`` on a streaming DataFrame outside of
      ``foreachBatch``.  All audit counts must happen inside the batch callback
      where the DataFrame is a static, countable micro-batch.
    * Set ``checkpointLocation`` on every ``writeStream`` to guarantee
      exactly-once semantics and safe restarts.
    """
    app_name        = app_name        or (settings.SPARK_APP_NAME + "_Streaming")
    driver_memory   = driver_memory   or settings.SPARK_DRIVER_MEMORY
    executor_memory = executor_memory or settings.SPARK_EXECUTOR_MEMORY

    # Combine both JARs into a single comma-separated packages string
    packages = f"{_MONGO_SPARK_PKG},{_KAFKA_SPARK_PKG}"

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(settings.SPARK_MASTER)
        .config("spark.driver.memory",   driver_memory)
        .config("spark.executor.memory", executor_memory)
        .config("spark.sql.shuffle.partitions",              "4")
        .config("spark.sql.parquet.compression.codec",       "snappy")
        .config("spark.sql.adaptive.enabled",                "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.enableVectorizedReader",  "false")
        .config("spark.sql.parquet.mergeSchema",             "false")
        .config("spark.ui.showConsoleProgress",              "false")
        # Combined packages: MongoDB + Kafka
        .config("spark.jars.packages", packages)
        # MongoDB Spark Connector
        .config("spark.mongodb.read.connection.uri",  settings.mongo_uri())
        .config("spark.mongodb.write.connection.uri", settings.mongo_uri())
        # Kafka consumer defaults
        .config("spark.streaming.kafka.consumer.cache.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info(
        f"[SPARK] Streaming session ready | version={spark.version} "
        f"kafka_bootstrap={kafka_bootstrap}"
    )
    return spark


# ── MongoDB I/O helpers ───────────────────────────────────────────────────────

def read_mongo(
    spark: SparkSession,
    database: str,
    collection: str,
) -> DataFrame:
    """
    Read a full MongoDB collection into a Spark DataFrame.

    Parameters
    ----------
    database:
        Name of the MongoDB database (e.g. ``settings.MONGO_DB_BRONZE``).
    collection:
        Collection name (e.g. ``"yellow_raw"``).
    """
    uri = settings.mongo_spark_uri(database, collection)
    df = (
        spark.read
        .format("mongodb")
        .option("connection.uri", uri)
        .load()
    )
    logger.info(f"[SPARK] Read from MongoDB {database}.{collection}")
    return df


def write_mongo(
    df: DataFrame,
    database: str,
    collection: str,
    mode: str = "append",
) -> int:
    """
    Write a Spark DataFrame to a MongoDB collection.

    Parameters
    ----------
    df:
        DataFrame to persist.
    database / collection:
        Target MongoDB namespace.
    mode:
        ``"append"`` (default) or ``"overwrite"``.

    Returns
    -------
    int
        Number of rows written.
    """
    n = df.count()
    uri = settings.mongo_spark_uri(database, collection)
    (
        df.write
        .format("mongodb")
        .option("connection.uri", uri)
        .mode(mode)
        .save()
    )
    logger.info(
        f"[SPARK] Wrote {n:,} rows → MongoDB {database}.{collection} (mode={mode})"
    )
    return n


# ── Parquet I/O helpers ───────────────────────────────────────────────────────

def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    """Read one or more Parquet file(s) from the local path with robust type promotion."""
    # Si la ruta es un directorio, recolectamos los archivos individualmente.
    # Esto permite usar unionByName, que inyecta CASTs logicos (INT->BIGINT) automaticamente 
    # y evita que el motor Parquet a bajo nivel explote por discrepancias de tipos.
    if os.path.isdir(path):
        files = glob.glob(os.path.join(path, "*.parquet"))
        if not files:
            df = spark.read.parquet(path)
        else:
            dfs = [spark.read.parquet(f) for f in files]
            df = reduce(lambda d1, d2: d1.unionByName(d2, allowMissingColumns=True), dfs)
    else:
        df = spark.read.parquet(path)
        
    logger.info(f"[SPARK] Read Parquet ← {path} (Robust Mode)")
    return df


def write_parquet(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
    coalesce_to: int | None = None,
) -> int:
    """
    Write a Spark DataFrame to Parquet.

    Uses ``coalesce()`` (not ``repartition()``) to avoid unnecessary shuffles
    when reducing the number of output files.
    """
    n = df.count()
    if coalesce_to:
        df = df.coalesce(coalesce_to)

    writer = df.write.mode(mode).format("parquet")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)

    logger.info(f"[SPARK] Wrote {n:,} rows → Parquet {path}")
    return n
