package io.factorhouse.demo.infrastructure.kafka

import io.factorhouse.demo.config.AppConfig
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import java.time.Duration

typealias AvroMapper<T> = (GenericRecord) -> T?

class KafkaSourceFactory(
    private val env: StreamExecutionEnvironment,
    private val config: AppConfig,
) {
    private val logger = LoggerFactory.getLogger(KafkaSourceFactory::class.java)

    fun <T> createStream(
        topic: String,
        mapperType: Class<T>,
        mapper: AvroMapper<T>,
        startingOffsets: OffsetsInitializer,
    ): DataStream<T> {
        logger.info("Initializing Kafka Source for topic: '{}'", topic)
        logger.info("Using Schema Registry at: '{}' with user info strategy.", config.registryUrl)

        val schema = Connector.getLatestSchema(topic, config.registryUrl, config.registryCredentials)

        val source =
            Connector.createSource(
                topic = topic,
                group = "$topic-flink-group",
                bootstrap = config.bootstrapAddress,
                regUrl = config.registryUrl,
                regConf = config.registryCredentials,
                schema = schema,
                startingOffsets = startingOffsets,
            )

        // Define Watermark Strategy for Event Time processing
        val wmStrategy =
            WatermarkStrategy
                .forBoundedOutOfOrderness<GenericRecord>(Duration.ofSeconds(5))
                .withTimestampAssigner { envelope, _ ->
                    // Attempt to extract 'ts_ms' from Debezium envelope
                    val sourceMeta = envelope.get("source") as? GenericRecord
                    val ts =
                        (sourceMeta?.get("ts_ms") as? Long)
                            ?: (envelope.get("ts_ms") as? Long)
                            ?: System.currentTimeMillis()
                    ts
                }.withIdleness(Duration.ofSeconds(60))

        logger.debug("Source built for topic '{}'. Attaching to environment.", topic)

        return env
            .fromSource(source, wmStrategy, "Source-$topic")
            .flatMap { envelope: GenericRecord, out: Collector<T> ->
                val obj = mapper(envelope)
                if (obj != null) out.collect(obj)
            }.returns(TypeInformation.of(mapperType))
            .name("kafka-source-$topic")
    }
}
