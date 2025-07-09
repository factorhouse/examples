package io.factorhouse.demo

import mu.KotlinLogging
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.Expressions.col
import org.apache.flink.table.api.FormatDescriptor
import org.apache.flink.table.api.Schema
import org.apache.flink.table.api.TableDescriptor
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.types.Types
import java.util.HashMap
import org.apache.iceberg.Schema as IcebergSchema
import org.apache.iceberg.hive.HiveCatalog as IcebergHiveCatalog

object TableApp {
    private val bootstrapAddress = System.getenv("BOOTSTRAP") ?: "kafka-1:19092"
    private val inputTopicName = System.getenv("INPUT_TOPIC") ?: "orders"
    private val registryUrl = System.getenv("REGISTRY_URL") ?: "http://schema:8081"
    private val icebergCatalogName = System.getenv("ICEBERG_CATALOG_NAME") ?: "demo_ib"
    private val hmsEndpoint = System.getenv("HMS_ENDPOINT") ?: "thrift://hive-metastore:9083"
    private val logger = KotlinLogging.logger {}

    fun run() {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.parallelism = 3
        env.enableCheckpointing(60000L) // 60 seconds
        env.checkpointConfig.minPauseBetweenCheckpoints = 30000L // 30 seconds minimum pause
        env.checkpointConfig.setCheckpointTimeout(120000L) // 2 minutes timeout
        env.checkpointConfig.maxConcurrentCheckpoints = 1

        val tEnv = StreamTableEnvironment.create(env)

        // 1. Create and Register Iceberg catalog and sink table
        val createCatalogSql =
            """
            CREATE CATALOG $icebergCatalogName WITH (
                'type' = 'iceberg',
                'catalog-type' = 'hive',
                'uri' = '$hmsEndpoint'
            )
            """.trimIndent()
        try {
            tEnv.executeSql(createCatalogSql)
            logger.info { "Iceberg catalog '$icebergCatalogName' created or already exists." }
        } catch (e: Exception) {
            logger.error(e) { "Failed to create Iceberg catalog '$icebergCatalogName'. This might be okay if it already exists." }
        }
        tEnv.useCatalog(icebergCatalogName)

        val hiveConf =
            Configuration().apply {
                set("hive.metastore.uris", "thrift://hive-metastore:9083")
            }
        val icebergCatalog = IcebergHiveCatalog()
        icebergCatalog.setConf(hiveConf)
        icebergCatalog.initialize(icebergCatalogName, HashMap())

        val icebergSchema =
            IcebergSchema(
                listOf(
                    Types.NestedField.required(1, "order_id", Types.StringType.get()),
                    Types.NestedField.required(2, "item", Types.StringType.get()),
                    Types.NestedField.required(3, "price", Types.DecimalType.of(10, 2)),
                    Types.NestedField.required(4, "supplier", Types.StringType.get()),
                    Types.NestedField.required(5, "bid_time", Types.TimestampType.withoutZone()),
                ),
            )

        val partitionSpec =
            PartitionSpec
                .builderFor(icebergSchema)
                .day("bid_time")
                .build()

        val tableIdentifier = TableIdentifier.of(Namespace.of("default"), "orders")
        if (!icebergCatalog.tableExists(tableIdentifier)) {
            logger.info { "Creating Iceberg table: $tableIdentifier" }
            icebergCatalog.createTable(
                tableIdentifier,
                icebergSchema,
                partitionSpec,
                mapOf(
                    "format-version" to "2",
                    "write.format.default" to "parquet",
                    "write.target-file-size-bytes" to "134217728",
                    "write.parquet.compression-codec" to "snappy",
                    "write.metadata.delete-after-commit.enabled" to "true",
                    "write.metadata.previous-versions-max" to "3",
                    "write.delete.mode" to "copy-on-write",
                    "write.update.mode" to "copy-on-write",
                ),
            )
        }

        // 2. Define Kafka source table and create as a temporary table
        val kafkaSourceBuilder =
            TableDescriptor
                .forConnector("kafka")
                .schema(
                    Schema
                        .newBuilder()
                        .column("order_id", DataTypes.STRING())
                        .column("item", DataTypes.STRING())
                        .column("price", DataTypes.STRING())
                        .column("supplier", DataTypes.STRING())
                        .column("bid_time", DataTypes.TIMESTAMP(3))
                        .watermark("bid_time", "bid_time - INTERVAL '5' SECONDS")
                        .build(),
                ).format(
                    FormatDescriptor
                        .forFormat("avro-confluent")
                        .option("schema-registry.url", registryUrl)
                        .option("basic-auth.credentials-source", "USER_INFO")
                        .option("basic-auth.user-info", "admin:admin")
                        .option("schema-registry.subject", "$inputTopicName-value")
                        .build(),
                ).option("topic", inputTopicName)
                .option("properties.bootstrap.servers", bootstrapAddress)
                .option("scan.startup.mode", "earliest-offset")

        tEnv.createTemporaryTable("orders_source", kafkaSourceBuilder.build())
        logger.info { "Kafka source table orders_source created programmatically." }

        // 3. Write to iceberg table
        val sinkTable =
            tEnv.from("orders_source").select(
                col("order_id"),
                col("item"),
                col("price").cast(DataTypes.DECIMAL(10, 2)).`as`("price"),
                col("supplier"),
                col("bid_time"),
            )
        logger.info { "Preparing to insert into Iceberg table: `$icebergCatalogName.default.orders`" }
        try {
            val tableResult = sinkTable.executeInsert("$icebergCatalogName.default.orders", false)
            logger.info { "Flink job submitted. Job ID: ${tableResult.jobClient.get().jobID}" }
        } catch (e: Exception) {
            logger.error(e) { "Failed to execute insert into Iceberg table '$icebergCatalogName.default.orders'." }
            return
        }
    }
}
