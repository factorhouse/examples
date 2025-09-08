from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, hour, sum, count
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.types import DecimalType
from pyspark import SparkContext

if __name__ == "__main__":
    ICEBERG_CATALOG_NAME = "demo_ib"
    ICEBERG_DB_NAME = "default"
    ICEBERG_SOURCE_TABLE_NAME = "orders"
    ICEBERG_TARGET_TABLE_NAME = "supplier_stats"

    ICEBERG_SOURCE_TABLE_FQN = (
        f"{ICEBERG_CATALOG_NAME}.{ICEBERG_DB_NAME}.{ICEBERG_SOURCE_TABLE_NAME}"
    )
    ICEBERG_TARGET_TABLE_FQN = (
        f"{ICEBERG_CATALOG_NAME}.{ICEBERG_DB_NAME}.{ICEBERG_TARGET_TABLE_NAME}"
    )

    spark = SparkSession.builder.appName("SupplierStats").getOrCreate()
    sc = spark.sparkContext
    log4jLogger = sc._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)

    LOGGER.info("Spark Session created for SupplierStats batch job.")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_TARGET_TABLE_FQN} (
            supplier STRING,
            bid_date DATE,
            bid_hour INT,
            total_price DECIMAL(10, 2),
            total_count BIGINT
        )
        USING iceberg
        PARTITIONED BY (bid_date, bid_hour)
        TBLPROPERTIES ('format-version' = '2')
    """)

    LOGGER.info("Beginning batch transformation and overwrite...")

    spark.sql(f"""
        INSERT OVERWRITE TABLE {ICEBERG_TARGET_TABLE_FQN}
        SELECT
            supplier,
            CAST(bid_time AS DATE) AS bid_date,
            HOUR(bid_time) AS bid_hour,
            SUM(price) AS total_price,
            COUNT(*) AS total_count
        FROM {ICEBERG_SOURCE_TABLE_FQN}
        WHERE CAST(bid_time AS DATE) = CURRENT_DATE()
        GROUP BY
            supplier,
            bid_date,
            bid_hour
    """)

    LOGGER.info(
        f"Successfully overwrote table {ICEBERG_TARGET_TABLE_FQN} with new statistics."
    )

    spark.stop()
