package io.factorhouse.demo.infrastructure.kafka

import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule

object KafkaSinkFactory {
    class JsonSerializer<T> : SerializationSchema<T> {
        // Static mapper to avoid "NotSerializableException" for the ObjectMapper itself
        companion object {
            private val mapper: ObjectMapper =
                ObjectMapper()
                    .registerModule(JavaTimeModule())
                    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        }

        override fun serialize(element: T): ByteArray = mapper.writeValueAsBytes(element)
    }

    fun <T> createSink(
        topic: String,
        bootstrap: String,
    ): KafkaSink<T> =
        KafkaSink
            .builder<T>()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(
                KafkaRecordSerializationSchema
                    .builder<T>()
                    .setTopic(topic)
                    .setValueSerializationSchema(JsonSerializer<T>()) // Now accepts SerializationSchema
                    .build(),
            ).setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build()
}
