package io.factorhouse.demo.infrastructure.kafka

import io.factorhouse.demo.config.AppConfig
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.slf4j.LoggerFactory
import java.util.Properties

object KafkaUtils {
    private val logger = LoggerFactory.getLogger(KafkaUtils::class.java)

    fun ensureTopicExists(
        config: AppConfig,
        partitions: Int = 1,
        replication: Short = 1,
    ) {
        val props = Properties()
        props[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = config.bootstrapAddress

        try {
            AdminClient.create(props).use { client ->
                val topics = client.listTopics().names().get()
                if (topics.contains(config.missingOrderTopic)) {
                    logger.info("Kafka topic '${config.missingOrderTopic}' already exists.")
                    return
                }

                logger.info("Creating Kafka topic '${config.missingOrderTopic}' with $partitions partitions...")
                val newTopic = NewTopic(config.missingOrderTopic, partitions, replication)
                client.createTopics(listOf(newTopic)).all().get()
                logger.info("Successfully created topic '${config.missingOrderTopic}'.")
            }
        } catch (e: Exception) {
            logger.error("Failed to ensure Kafka topic exists: ${e.message}", e)
            throw RuntimeException(e)
        }
    }
}
