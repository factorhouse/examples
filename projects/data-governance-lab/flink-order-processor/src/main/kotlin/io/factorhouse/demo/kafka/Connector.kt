package io.factorhouse.demo.kafka

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.slf4j.LoggerFactory
import java.util.Properties

object Connector {
    private val logger = LoggerFactory.getLogger(Connector::class.java)

    fun createSource(
        topicName: String,
        groupId: String,
        bootstrapAddress: String,
        registryUrl: String,
        registryConfig: Map<String, String>,
        schema: Schema,
    ): KafkaSource<GenericRecord> =
        KafkaSource
            .builder<GenericRecord>()
            .setBootstrapServers(bootstrapAddress)
            .setTopics(topicName)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setProperty("commit.offsets.on.checkpoint", "true")
            .setValueOnlyDeserializer(
                ConfluentRegistryAvroDeserializationSchema.forGeneric(schema, registryUrl, registryConfig),
            ).setProperties(
                Properties().apply {
                    put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500")
                },
            ).build()

    fun getLatestSchema(
        schemaSubject: String,
        registryUrl: String,
        registryConfig: Map<String, String>,
    ): Schema {
        val schemaRegistryClient =
            CachedSchemaRegistryClient(
                registryUrl,
                100,
                registryConfig,
            )
        logger.info("Fetching latest schema for subject '$schemaSubject' from $registryUrl")
        try {
            val latestSchemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(schemaSubject)
            logger.info("Successfully fetched schema ID ${latestSchemaMetadata.id} version ${latestSchemaMetadata.version}")
            return Schema.Parser().parse(latestSchemaMetadata.schema)
        } catch (e: Exception) {
            throw RuntimeException("Failed to retrieve schema for subject '$schemaSubject' from registry $registryUrl", e)
        }
    }
}
