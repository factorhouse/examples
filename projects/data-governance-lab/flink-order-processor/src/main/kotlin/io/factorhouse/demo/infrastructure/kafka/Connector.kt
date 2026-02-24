package io.factorhouse.demo.infrastructure.kafka

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.slf4j.LoggerFactory
import java.util.Properties

/**
 * Low-level utility to interact with Schema Registry and build the Kafka Source.
 */
object Connector {
    private val logger = LoggerFactory.getLogger(Connector::class.java)

    fun getLatestSchema(
        topic: String,
        url: String,
        configs: Map<String, String>,
    ): Schema {
        logger.info("Fetching latest schema for subject '$topic-value' from $url")
        val client = CachedSchemaRegistryClient(url, 100, configs)
        val meta = client.getLatestSchemaMetadata("$topic-value")
        return Schema.Parser().parse(meta.schema)
    }

    fun createSource(
        topic: String,
        group: String,
        bootstrap: String,
        regUrl: String,
        regConf: Map<String, String>,
        schema: Schema,
        startingOffsets: OffsetsInitializer,
    ): KafkaSource<GenericRecord> =
        KafkaSource
            .builder<GenericRecord>()
            .setBootstrapServers(bootstrap)
            .setTopics(topic)
            .setGroupId(group)
            .setStartingOffsets(startingOffsets)
            .setProperty("commit.offsets.on.checkpoint", "true")
            .setValueOnlyDeserializer(
                ConfluentRegistryAvroDeserializationSchema.forGeneric(schema, regUrl, regConf),
            ).setProperties(
                Properties().apply {
                    put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500")
                },
            ).build()
}
