import os
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # -------------------------------------------------------------------------
    # Configuration
    # -------------------------------------------------------------------------
    # OpenMetadata
    LINEAGE_TRANSPORT_HOST = "http://omt-server:8585/api"
    PIPELINE_SERVICE_NAME = "fh_local_spark"
    PIPELINE_NAME = "StatsAggregator"
    DATABASE_SERVICE_NAME = "fh_local_iceberg"
    # Iceberg
    ICEBERG_CATALOG = "demo_ib"
    ICEBERG_INPUT_DB = "dev"
    ICEBERG_OUTPUT_DB = "dev"

    # Source Table
    SOURCE_TABLE = f"{ICEBERG_CATALOG}.{ICEBERG_INPUT_DB}.orders_enriched"
    # Target Tables
    TARGET_TABLE_USER = f"{ICEBERG_CATALOG}.{ICEBERG_OUTPUT_DB}.user_daily_stats"
    TARGET_TABLE_CATEGORY = (
        f"{ICEBERG_CATALOG}.{ICEBERG_OUTPUT_DB}.category_daily_stats"
    )

    # -------------------------------------------------------------------------
    # Spark Session Setup
    # -------------------------------------------------------------------------
    spark = (
        SparkSession.builder.master("local[*]")
        .config(
            "spark.extraListeners",
            "io.openlineage.spark.agent.OpenLineageSparkListener",
        )
        .config("spark.openmetadata.transport.type", "openmetadata")
        .config("spark.openmetadata.transport.hostPort", LINEAGE_TRANSPORT_HOST)
        .config("spark.openmetadata.transport.jwtToken", os.environ["JWT_TOKEN"])
        .config(
            "spark.openmetadata.transport.pipelineServiceName", PIPELINE_SERVICE_NAME
        )
        .config("spark.openmetadata.transport.pipelineName", PIPELINE_NAME)
        .config(
            "spark.openmetadata.transport.pipelineDescription",
            "Process daily statistics from demo_ib.orders_enriched.",
        )
        .config(
            "spark.openmetadata.transport.databaseServiceNames", DATABASE_SERVICE_NAME
        )
        .config("spark.openmetadata.transport.debug", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .getOrCreate()
    )

    sc = spark.sparkContext
    log4jLogger = sc._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)

    LOGGER.info(f"Starting Stats Aggregation from source: {SOURCE_TABLE}")

    # -------------------------------------------------------------------------
    # Total Revenue by User and Date
    # -------------------------------------------------------------------------
    LOGGER.info(f"Processing Target: {TARGET_TABLE_USER}")

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
    # Total Revenue by Category and Date
    # -------------------------------------------------------------------------
    LOGGER.info(f"Processing Target: {TARGET_TABLE_CATEGORY}")

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
