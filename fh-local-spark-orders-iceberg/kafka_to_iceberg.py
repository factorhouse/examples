import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.types import DecimalType
from pyspark import SparkContext


def from_avro(col, config):
    """
    Avro deserialization

    Source: https://github.com/AbsaOSS/ABRiS/blob/master/documentation/python-documentation.md

    :param col (PySpark column / str): column name "key" or "value"
    :param config (za.co.absa.abris.config.FromAvroConfig): abris config, generated from abris_config helper function
    :return: PySpark Column
    """
    jvm_gateway = SparkContext._active_spark_context._gateway.jvm
    abris_avro = jvm_gateway.za.co.absa.abris.avro
    return Column(abris_avro.functions.from_avro(_to_java_column(col), config))


def from_avro_abris_config(config_map, topic, is_key):
    """
    Create from avro abris config with a schema url

    Source: https://github.com/AbsaOSS/ABRiS/blob/master/documentation/python-documentation.md

    :param config_map (dict[str, str]): configuration map to pass to deserializer, ex: {'schema.registry.url': 'http://localhost:8081'}
    :param topic (str): kafka topic
    :param is_key (bool): boolean
    :return: za.co.absa.abris.config.FromAvroConfig
    """
    jvm_gateway = SparkContext._active_spark_context._gateway.jvm
    scala_map = jvm_gateway.PythonUtils.toScalaMap(config_map)
    return (
        jvm_gateway.za.co.absa.abris.config.AbrisConfig.fromConfluentAvro()
        .downloadReaderSchemaByLatestVersion()
        .andTopicNameStrategy(topic, is_key)
        .usingSchemaRegistry(scala_map)
    )


if __name__ == "__main__":
    BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka-1:19092")
    TOPIC_NAME = os.getenv("TOPIC_NAME", "orders")
    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema:8081")
    SCHEMA_REGISTRY_AUTH_USER_INFO = "admin:admin"

    ICEBERG_CATALOG_NAME = "demo"
    ICEBERG_DB_NAME = "db"
    ICEBERG_TABLE_NAME = "orders"
    ICEBERG_TABLE_FQN = f"{ICEBERG_CATALOG_NAME}.{ICEBERG_DB_NAME}.{ICEBERG_TABLE_NAME}"
    CHECKPOINT_LOCATION_BASE = "/tmp/spark-events"

    spark = SparkSession.builder.appName("KafkaToIceberg").getOrCreate()
    sc = spark.sparkContext
    log4jLogger = sc._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)

    LOGGER.info("Spark Session created for KafkaToIceberg.")
    LOGGER.info(
        f"Using Iceberg catalog: {ICEBERG_CATALOG_NAME} (from spark-defaults.conf)"
    )

    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS f{ICEBERG_CATALOG_NAME}.{ICEBERG_DB_NAME}"
    )
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE_FQN} (
            order_id STRING,
            item STRING,
            price DECIMAL(10, 2),
            supplier STRING,
            bid_time TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (DAY(bid_time))
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.target-file-size-bytes' = '134217728',
            'write.parquet.compression-codec' = 'snappy',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max' = '3',
            'write.delete.mode' = 'copy-on-write',
            'write.update.mode' = 'copy-on-write'
        )
    """)

    # Configuration map for Abris to connect to Schema Registry
    config_map = {
        "schema.registry.url": SCHEMA_REGISTRY_URL,
        "basic.auth.credentials.source": "USER_INFO",
        "basic.auth.user.info": SCHEMA_REGISTRY_AUTH_USER_INFO,
    }
    from_avro_abris_settings = from_avro_abris_config(
        config_map=config_map,
        topic=TOPIC_NAME,
        is_key=False,
    )

    # Read from Kafka as a stream
    kafka_raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )
    # Deserialize
    deserialized_df = kafka_raw_df.withColumn(
        "parsed_value", from_avro("value", from_avro_abris_settings)
    )
    final_df = deserialized_df.select(
        col("parsed_value.order_id").alias("order_id"),
        col("parsed_value.item").alias("item"),
        col("parsed_value.price").cast(DecimalType(10, 2)).alias("price"),
        col("parsed_value.supplier").alias("supplier"),
        col("parsed_value.bid_time").alias("bid_time"),
    )

    LOGGER.info(f"Starting stream to Iceberg table: {ICEBERG_TABLE_FQN}")

    query = (
        final_df.writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .option("fanout-enabled", "true")
        .option(
            "checkpointLocation",
            f"{CHECKPOINT_LOCATION_BASE}/{ICEBERG_DB_NAME}_{ICEBERG_TABLE_NAME}",
        )
        .toTable(ICEBERG_TABLE_FQN)
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        LOGGER.info("Streaming query interrupted by user. Stopping...")
    except Exception as e:
        raise RuntimeError("Streaming query terminated with an error") from e
    finally:
        LOGGER.info("Stopping Spark session.")
        spark.stop()
