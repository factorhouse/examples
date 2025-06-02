package io.factorhouse.demo

import mu.KotlinLogging
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.Expressions.col
import org.apache.flink.table.api.FormatDescriptor
import org.apache.flink.table.api.Schema
import org.apache.flink.table.api.TableDescriptor
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

object TableApp {
    private val bootstrapAddress = System.getenv("BOOTSTRAP") ?: "kafka-1:19092"
    private val inputTopicName = System.getenv("INPUT_TOPIC") ?: "orders"
    private val registryUrl = System.getenv("REGISTRY_URL") ?: "http://schema:8081"
    private val icebergCatalogName = System.getenv("ICEBERG_CATALOG_NAME") ?: "demo"
    private val icebergRestUri = System.getenv("ICEBERG_REST_URL") ?: "http://rest:8181"
    private val s3Endpoint = System.getenv("S3_ENDPOINT") ?: "http://minio:9000"
    private val logger = KotlinLogging.logger {}

    fun run() {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.parallelism = 3
        env.enableCheckpointing(60000L) // 60 seconds
        env.checkpointConfig.minPauseBetweenCheckpoints = 30000L // 30 seconds minimum pause
        env.checkpointConfig.setCheckpointTimeout(120000L) // 2 minutes timeout
        env.checkpointConfig.maxConcurrentCheckpoints = 1

        val tEnv = StreamTableEnvironment.create(env)

        // 1. Create and Register Iceberg catalog
        val createCatalogSql =
            """
            CREATE CATALOG $icebergCatalogName WITH (
                'type' = 'iceberg',
                'catalog-type' = 'rest',
                'uri' = '$icebergRestUri',
                'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
                's3.endpoint' = '$s3Endpoint',
                's3.path-style-access' = 'true',
                's3.access-key-id' = 'admin',
                's3.secret-access-key' = 'password'
            )
            """.trimIndent()
        try {
            tEnv.executeSql(createCatalogSql)
            logger.info { "Iceberg catalog '$icebergCatalogName' created or already exists." }
        } catch (e: Exception) {
            logger.error(e) { "Failed to create Iceberg catalog '$icebergCatalogName'. This might be okay if it already exists." }
        }
        tEnv.useCatalog(icebergCatalogName)

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
                ).option("topic", inputTopicName)
                .option("properties.bootstrap.servers", bootstrapAddress)
                .format(
                    FormatDescriptor
                        .forFormat("avro-confluent")
                        .option("schema-registry.url", registryUrl)
                        .option("basic-auth.credentials-source", "USER_INFO")
                        .option("basic-auth.user-info", "admin:admin")
                        .option("schema-registry.subject", "$inputTopicName-value")
                        .build(),
                ).option("scan.startup.mode", "earliest-offset")

        tEnv.createTemporaryTable("orders", kafkaSourceBuilder.build())
        logger.info { "Kafka source table orders created programmatically." }

        // 3. Write to iceberg table
        val sinkTable =
            tEnv.from("orders").select(
                col("order_id"),
                col("item"),
                col("price").cast(DataTypes.DECIMAL(10, 2)).`as`("price"),
                col("supplier"),
                col("bid_time"),
            )
        logger.info { "Preparing to insert into Iceberg table: `demo.db.orders`" }
        try {
            val tableResult = sinkTable.executeInsert("demo.db.orders", false)
            logger.info { "Flink job submitted. Job ID: ${tableResult.jobClient.get().jobID}" }
        } catch (e: Exception) {
            logger.error(e) { "Failed to execute insert into Iceberg table 'demo.db.orders'." }
            return
        }
    }
}
