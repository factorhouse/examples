package io.factorhouse.demo

import io.factorhouse.demo.flink.SupplierStatsAggregator
import io.factorhouse.demo.flink.SupplierStatsFunction
import io.factorhouse.demo.kafka.createLegacyStatsSink
import io.factorhouse.demo.kafka.createOrdersSource
import io.factorhouse.demo.kafka.createTopicIfNotExists
import io.factorhouse.demo.kafka.getLatestSchema
import io.openlineage.flink.OpenLineageFlinkJobListener
import mu.KotlinLogging
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import java.time.Duration

object DataStreamApp {
    private val toSkipPrint = System.getenv("TO_SKIP_PRINT")?.toBoolean() ?: true
    private val bootstrapAddress = System.getenv("BOOTSTRAP") ?: "kafka-1:19092"
    private val inputTopicName = System.getenv("INPUT_TOPIC") ?: "orders"
    private val outputTopicName = System.getenv("OUTPUT_TOPIC") ?: "supplier-stats"
    private val registryUrl = System.getenv("REGISTRY_URL") ?: "http://schema:8081"
    private val openLineageNamespace = System.getenv("OPENLINEAGE_NAMESPACE") ?: "fh-local"
    private val openLineageJobName = System.getenv("OPENLINEAGE_JOBNAME") ?: "supplier-stats"
    private val windowSizeSeconds = System.getenv("WINDOW_SIZE_SECONDS")?.toLong() ?: 5L
    private val allowedLatenessSeconds = System.getenv("ALLOWED_LATENESS_SECONDS")?.toLong() ?: 5L
    private val maxOutOfOrdernessSeconds = System.getenv("MAX_OUT_OF_ORDERNESS_SECONDS")?.toLong() ?: 5L
    private val sourceIdlenessSeconds = System.getenv("SOURCE_IDLENESS_SECONDS")?.toLong() ?: 60L
    private val registryConfig =
        mapOf(
            "basic.auth.credentials.source" to "USER_INFO",
            "basic.auth.user.info" to "admin:admin",
        )
    private const val NUM_PARTITIONS = 3
    private const val REPLICATION_FACTOR: Short = 3
    private val logger = KotlinLogging.logger {}

    fun run() {
        logger.info { "Starting Flink job." }
        logger.info { "Job Parameters:" }
        logger.info { "  - Input Topic: $inputTopicName" }
        logger.info { "  - Output Topic: $outputTopicName" }
        logger.info { "  - Bootstrap Servers: $bootstrapAddress" }
        logger.info { "  - Schema Registry URL: $registryUrl" }
        logger.info { "  - OpenLineage Namespace: $openLineageNamespace" }
        logger.info { "  - OpenLineage Job name: $openLineageJobName" }

        // Create output topic if not existing
        logger.info { "Ensuring output topic '$outputTopicName' exists..." }
        createTopicIfNotExists(outputTopicName, bootstrapAddress, NUM_PARTITIONS, REPLICATION_FACTOR)

        val jobName = openLineageJobName
        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.parallelism = 3

        logger.info { "Registering OpenLineageFlinkJobListener for job '$jobName' in namespace '$openLineageNamespace'." }
        val listener =
            OpenLineageFlinkJobListener
                .builder()
                .executionEnvironment(
                    env,
                ).jobNamespace(openLineageNamespace)
                .jobName(jobName)
                .build()
        env.registerJobListener(listener)

        logger.info { "Creating Kafka source from topic '$inputTopicName'." }
        val inputAvroSchema = getLatestSchema("$inputTopicName-value", registryUrl, registryConfig)
        val ordersGenericRecordSource =
            createOrdersSource(
                inputTopicName,
                "$inputTopicName-ds",
                bootstrapAddress,
                registryUrl,
                registryConfig,
                inputAvroSchema,
            )

        logger.info { "Creating legacy Kafka sink to topic '$outputTopicName'." }
        val statsSink =
            createLegacyStatsSink(
                topic = outputTopicName,
                bootstrapAddress = bootstrapAddress,
                registryUrl = registryUrl,
                registryConfig = registryConfig,
                outputSubject = "$outputTopicName-value",
            )

        val sourceStream =
            env
                .fromSource(
                    ordersGenericRecordSource,
                    WatermarkStrategy.noWatermarks(), // Watermark will be assigned after filter
                    "KafkaGenericRecordSource",
                ).filter { record -> record.get("bid_time") is Long }
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy
                        .forBoundedOutOfOrderness<GenericRecord>(Duration.ofSeconds(maxOutOfOrdernessSeconds))
                        .withTimestampAssigner { record, _ -> record.get("bid_time") as Long }
                        .withIdleness(Duration.ofSeconds(sourceIdlenessSeconds)),
                )

        val statsStream =
            sourceStream
                .keyBy { record -> record.get("supplier").toString() }
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(windowSizeSeconds)))
                .allowedLateness(Duration.ofSeconds(allowedLatenessSeconds))
                .aggregate(SupplierStatsAggregator(), SupplierStatsFunction())

        if (!toSkipPrint) {
            statsStream
                .print()
                .name("SupplierStatsPrint")
        }

        statsStream.addSink(statsSink).name("SupplierStatsSink")
        logger.info { "Submitting Flink job '$jobName' to the cluster in attached mode..." }
        env.executeAsync(jobName).jobExecutionResult.get()
        logger.info { "Flink job '$jobName' finished successfully." }
    }
}
