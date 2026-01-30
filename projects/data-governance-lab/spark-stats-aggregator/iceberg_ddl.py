from pyspark.sql import SparkSession

if __name__ == "__main__":
    ICEBERG_CATALOG = "demo_ib"
    ICEBERG_INPUT_DB = "dev"
    ICEBERG_OUTPUT_DB = "dev"

    # Target Tables
    TARGET_TABLE_USER = f"{ICEBERG_CATALOG}.{ICEBERG_OUTPUT_DB}.user_daily_stats"
    TARGET_TABLE_CATEGORY = (
        f"{ICEBERG_CATALOG}.{ICEBERG_OUTPUT_DB}.category_daily_stats"
    )

    spark = SparkSession.builder.master("local[*]").getOrCreate()

    sc = spark.sparkContext
    log4jLogger = sc._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)

    LOGGER.info("Creating Target Icebertg Tables")
    LOGGER.info(f"\t{TARGET_TABLE_USER}")
    LOGGER.info(f"\t{TARGET_TABLE_CATEGORY}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE_USER} (
            user_first_name STRING NOT NULL,
            user_last_name STRING NOT NULL,
            user_email STRING NOT NULL,
            report_date DATE NOT NULL,
            total_revenue DECIMAL(15, 2) NOT NULL,
            total_orders BIGINT NOT NULL
        )
        USING iceberg
        PARTITIONED BY (report_date)
        TBLPROPERTIES ('format-version' = '2')
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE_CATEGORY} (
            category STRING NOT NULL,
            report_date DATE NOT NULL,
            total_revenue DECIMAL(15, 2) NOT NULL,
            total_orders BIGINT NOT NULL
        )
        USING iceberg
        PARTITIONED BY (report_date)
        TBLPROPERTIES ('format-version' = '2')
    """)

    spark.stop()
