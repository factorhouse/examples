package io.factorhouse.demo.kafka

import io.factorhouse.demo.avro.SupplierStats
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import java.nio.charset.StandardCharsets
import java.util.Properties

fun createOrdersSource(
    topic: String,
    groupId: String,
    bootstrapAddress: String,
    registryUrl: String,
    registryConfig: Map<String, String>,
    schema: Schema,
): KafkaSource<GenericRecord> =
    KafkaSource
        .builder<GenericRecord>()
        .setBootstrapServers(bootstrapAddress)
        .setTopics(topic)
        .setGroupId(groupId)
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(
            ConfluentRegistryAvroDeserializationSchema.forGeneric(
                schema,
                registryUrl,
                registryConfig,
            ),
        ).setProperties(
            Properties().apply {
                put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500")
            },
        ).build()

fun createStatsSink(
    topic: String,
    bootstrapAddress: String,
    registryUrl: String,
    registryConfig: Map<String, String>,
    outputSubject: String,
): KafkaSink<SupplierStats> =
    KafkaSink
        .builder<SupplierStats>()
        .setBootstrapServers(bootstrapAddress)
        .setKafkaProducerConfig(
            Properties().apply {
                setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
                setProperty(ProducerConfig.LINGER_MS_CONFIG, "100")
                setProperty(ProducerConfig.BATCH_SIZE_CONFIG, (64 * 1024).toString())
                setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
            },
        ).setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .setRecordSerializer(
            KafkaRecordSerializationSchema
                .builder<SupplierStats>()
                .setTopic(topic)
                .setKeySerializationSchema { value: SupplierStats ->
                    value.supplier.toByteArray(StandardCharsets.UTF_8)
                }.setValueSerializationSchema(
                    ConfluentRegistryAvroSerializationSchema.forSpecific<SupplierStats>(
                        SupplierStats::class.java,
                        outputSubject,
                        registryUrl,
                        registryConfig,
                    ),
                ).build(),
        ).build()
