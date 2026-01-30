package io.factorhouse.demo.infrastructure.iceberg

import io.factorhouse.demo.config.AppConfig
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Schema
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.flink.CatalogLoader
import org.apache.iceberg.hive.HiveCatalog
import org.apache.iceberg.types.Types
import org.slf4j.LoggerFactory

object IcebergUtils {
    private val logger = LoggerFactory.getLogger(IcebergUtils::class.java)

    val SCHEMA =
        Schema(
            Types.NestedField.required(1, "order_id", Types.StringType.get()),
            Types.NestedField.required(2, "user_id", Types.StringType.get()),
            Types.NestedField.required(3, "product_name", Types.StringType.get()),
            Types.NestedField.required(4, "category", Types.StringType.get()),
            Types.NestedField.required(5, "quantity", Types.IntegerType.get()),
            Types.NestedField.required(6, "unit_price", Types.DoubleType.get()),
            Types.NestedField.required(7, "created_at", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(8, "user_first_name", Types.StringType.get()),
            Types.NestedField.optional(9, "user_last_name", Types.StringType.get()),
            Types.NestedField.optional(10, "user_email", Types.StringType.get()),
            Types.NestedField.optional(11, "user_postal_code", Types.StringType.get()),
        )

    fun createHadoopConfig(config: AppConfig): Configuration {
        val hadoopConf = Configuration()
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoopConf.set("fs.s3a.endpoint", config.s3Endpoint)
        hadoopConf.set("fs.s3a.access.key", "admin")
        hadoopConf.set("fs.s3a.secret.key", "password")
        hadoopConf.set("fs.s3a.path.style.access", "true")
        return hadoopConf
    }

    fun createCatalogLoader(config: AppConfig): CatalogLoader {
        val props =
            mapOf(
                "uri" to config.hmsEndpoint,
                "warehouse" to config.icebergWarehouse,
                "io-impl" to "org.apache.iceberg.hadoop.HadoopFileIO",
            )
        return CatalogLoader.hive(config.icebergCatalogName, createHadoopConfig(config), props)
    }

    fun ensureTableExists(config: AppConfig) {
        val props =
            mapOf(
                "uri" to config.hmsEndpoint,
                "warehouse" to config.icebergWarehouse,
                "io-impl" to "org.apache.iceberg.hadoop.HadoopFileIO",
            )

        val catalog = HiveCatalog()
        catalog.conf = createHadoopConfig(config)
        catalog.initialize(config.icebergCatalogName, props)

        val namespace = Namespace.of(config.icebergDatabase)
        if (!catalog.namespaceExists(namespace)) {
            catalog.createNamespace(namespace)
            logger.info("Created Iceberg Namespace (Database): ${config.icebergDatabase}")
        }

        val tableId = TableIdentifier.of(config.icebergDatabase, config.icebergTableName)
        if (!catalog.tableExists(tableId)) {
            val partitionSpec = PartitionSpec.builderFor(SCHEMA).day("created_at").build()
            catalog.createTable(
                tableId,
                SCHEMA,
                partitionSpec,
                mapOf("format-version" to "2", "write.format.default" to "parquet"),
            )
            println("Created Iceberg table: $tableId")
        }
    }
}
