package io.factorhouse.demo.kafka

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import mu.KotlinLogging
import org.apache.avro.Schema
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.errors.TopicExistsException
import java.util.Properties
import java.util.concurrent.ExecutionException

private val logger = KotlinLogging.logger { }

fun createTopicIfNotExists(
    topicName: String,
    bootstrapAddress: String,
    numPartitions: Int,
    replicationFactor: Short,
) {
    val props =
        Properties().apply {
            put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress)
            put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000")
            put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000")
            put(AdminClientConfig.RETRIES_CONFIG, "1")
        }

    AdminClient.create(props).use { client ->
        val newTopic = NewTopic(topicName, numPartitions, replicationFactor)
        val result = client.createTopics(listOf(newTopic))

        try {
            logger.info { "Attempting to create topic '$topicName'..." }
            result.all().get()
            logger.info { "Topic '$topicName' created successfully!" }
        } catch (e: ExecutionException) {
            if (e.cause is TopicExistsException) {
                logger.warn { "Topic '$topicName' was created concurrently or already existed. Continuing..." }
            } else {
                throw RuntimeException("Unrecoverable error while creating a topic '$topicName'.", e)
            }
        }
    }
}

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
    logger.info { "Fetching latest schema for subject '$schemaSubject' from $registryUrl" }
    try {
        val latestSchemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(schemaSubject)
        logger.info {
            "Successfully fetched schema ID ${latestSchemaMetadata.id} version ${latestSchemaMetadata.version} for subject '$schemaSubject'"
        }
        return Schema.Parser().parse(latestSchemaMetadata.schema)
    } catch (e: Exception) {
        logger.error(e) { "Failed to retrieve schema for subject '$schemaSubject' from registry $registryUrl" }
        throw RuntimeException("Failed to retrieve schema for subject '$schemaSubject'", e)
    }
}
