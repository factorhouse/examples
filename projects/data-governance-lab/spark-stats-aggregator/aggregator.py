from pyspark.sql import SparkSession

if __name__ == "__main__":
    # -------------------------------------------------------------------------
    # Configuration
    # -------------------------------------------------------------------------
    ICEBERG_CATALOG = "demo_ib"
    ICEBERG_DB = "default"

    # Source Table
    SOURCE_TABLE = f"{ICEBERG_CATALOG}.{ICEBERG_DB}.orders_enriched"

    # Target Tables
    TARGET_TABLE_USER = f"{ICEBERG_CATALOG}.{ICEBERG_DB}.user_daily_stats"
    TARGET_TABLE_CATEGORY = f"{ICEBERG_CATALOG}.{ICEBERG_DB}.category_daily_stats"

    # -------------------------------------------------------------------------
    # Spark Session Setup
    # -------------------------------------------------------------------------
    spark = (
        SparkSession.builder.appName("StatsAggregator")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .getOrCreate()
    )

    sc = spark.sparkContext
    log4jLogger = sc._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)

    LOGGER.info(f"Starting Stats Aggregation from source: {SOURCE_TABLE}")

    # -------------------------------------------------------------------------
    # 1. Total Revenue by User and Date
    # -------------------------------------------------------------------------
    LOGGER.info(f"Processing Target: {TARGET_TABLE_USER}")

    # Ensure Target Table Exists
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE_USER} (
            user_first_name STRING,
            user_last_name STRING,
            user_email STRING,
            report_date DATE,
            total_revenue DECIMAL(15, 2),
            total_orders BIGINT
        )
        USING iceberg
        PARTITIONED BY (report_date)
        TBLPROPERTIES ('format-version' = '2')
    """)

    # Execute Overwrite
    # Note: We calculate revenue as quantity * unit_price
    spark.sql(f"""
        INSERT OVERWRITE TABLE {TARGET_TABLE_USER}
        SELECT 
            user_first_name,
            user_last_name,
            user_email,
            CAST(created_at AS DATE) AS report_date,
            CAST(SUM(quantity * unit_price) AS DECIMAL(15, 2)) AS total_revenue,
            COUNT(*) AS total_orders
        FROM {SOURCE_TABLE}
        GROUP BY 
            user_first_name, 
            user_last_name,
            user_email,
            CAST(created_at AS DATE)
    """)

    LOGGER.info(f"Successfully overwrote {TARGET_TABLE_USER}")

    # -------------------------------------------------------------------------
    # 2. Total Revenue by Category and Date
    # -------------------------------------------------------------------------
    LOGGER.info(f"Processing Target: {TARGET_TABLE_CATEGORY}")

    # Ensure Target Table Exists
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE_CATEGORY} (
            category STRING,
            report_date DATE,
            total_revenue DECIMAL(15, 2),
            total_orders BIGINT
        )
        USING iceberg
        PARTITIONED BY (report_date)
        TBLPROPERTIES ('format-version' = '2')
    """)

    # Execute Overwrite
    spark.sql(f"""
        INSERT OVERWRITE TABLE {TARGET_TABLE_CATEGORY}
        SELECT 
            category,
            CAST(created_at AS DATE) AS report_date,
            CAST(SUM(quantity * unit_price) AS DECIMAL(15, 2)) AS total_revenue,
            COUNT(*) AS total_orders
        FROM {SOURCE_TABLE}
        GROUP BY 
            category, 
            CAST(created_at AS DATE)
    """)

    LOGGER.info(f"Successfully overwrote {TARGET_TABLE_CATEGORY}")

    # -------------------------------------------------------------------------
    # Cleanup
    # -------------------------------------------------------------------------
    spark.stop()
