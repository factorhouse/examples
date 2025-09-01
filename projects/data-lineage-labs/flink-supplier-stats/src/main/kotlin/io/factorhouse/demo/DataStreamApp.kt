package io.factorhouse.demo

import io.factorhouse.demo.avro.SupplierStats
import io.factorhouse.demo.flink.SupplierStatsAggregator
import io.factorhouse.demo.flink.SupplierStatsFunction
import io.factorhouse.demo.kafka.createLegacyStatsSink
import io.factorhouse.demo.kafka.createOrdersSource
import io.factorhouse.demo.kafka.createStatsSink
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
        // Create output topic if not existing
        createTopicIfNotExists(outputTopicName, bootstrapAddress, NUM_PARTITIONS, REPLICATION_FACTOR)

        val jobName = "$outputTopicName-job"
        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.parallelism = 3

        val listener =
            OpenLineageFlinkJobListener
                .builder()
                .executionEnvironment(
                    env,
                ).jobNamespace(openLineageNamespace)
                .jobName(jobName)
                .build()
        env.registerJobListener(listener)

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
//        val statsSink =
//            createStatsSink(
//                topic = outputTopicName,
//                bootstrapAddress = bootstrapAddress,
//                registryUrl = registryUrl,
//                registryConfig = registryConfig,
//                outputSubject = "$outputTopicName-value",
//            )
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

        // statsStream.sinkTo(statsSink).name("SupplierStatsSink")
        statsStream.addSink(statsSink).name("SupplierStatsSink")
        env.execute(jobName)
    }
}
