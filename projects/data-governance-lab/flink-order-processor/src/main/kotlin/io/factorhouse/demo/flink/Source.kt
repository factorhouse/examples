package io.factorhouse.demo.flink

import io.factorhouse.demo.kafka.Connector
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import java.time.Duration

typealias AvroMapper<T> = (GenericRecord) -> T?

object Source {
    private val logger = LoggerFactory.getLogger(Source::class.java)

    fun setWatermarkStrategy(
        outOfOrdernessSec: Long = 5,
        idlenessSec: Long = 60,
    ): WatermarkStrategy<GenericRecord> =
        WatermarkStrategy
            .forBoundedOutOfOrderness<GenericRecord>(Duration.ofSeconds(outOfOrdernessSec))
            .withTimestampAssigner { envelope, _ ->
                val source = envelope.get("source") as? GenericRecord
                val dbTime = source?.get("ts_ms") as? Long
                dbTime ?: (envelope.get("ts_ms") as? Long) ?: System.currentTimeMillis()
            }.withIdleness(Duration.ofSeconds(idlenessSec))

    inline fun <reified T> createDebeziumStream(
        env: StreamExecutionEnvironment,
        topicName: String,
        bootstrapAddress: String,
        registryUrl: String,
        registryConfig: Map<String, String>,
        crossinline mapper: AvroMapper<T>, // The function that converts GenericRecord -> T?
    ): DataStream<T> {
        val schema = Connector.getLatestSchema("$topicName-value", registryUrl, registryConfig)
        val genericRecordSource =
            Connector.createSource(
                topicName = topicName,
                groupId = "$topicName-flink-group",
                bootstrapAddress = bootstrapAddress,
                registryUrl = registryUrl,
                registryConfig = registryConfig,
                schema = schema,
            )
        return env
            .fromSource(
                genericRecordSource,
                setWatermarkStrategy(),
                "Connector-$topicName",
            ).name("$topicName-generic-source")
            .flatMap { envelope: GenericRecord, out: Collector<T> ->
                // Apply the mapper function passed as argument
                val domainObject = mapper(envelope)

                if (domainObject != null) {
                    out.collect(domainObject)
                }
            }.returns(TypeInformation.of(T::class.java))
            .name("$topicName-specific-source")
    }
}
