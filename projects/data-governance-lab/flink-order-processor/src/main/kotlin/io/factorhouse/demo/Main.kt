package io.factorhouse.demo

import io.factorhouse.demo.config.AppConfig
import io.factorhouse.demo.infrastructure.clickhouse.ClickHouseUtils
import io.factorhouse.demo.infrastructure.iceberg.IcebergUtils
import io.factorhouse.demo.infrastructure.kafka.KafkaUtils
import io.factorhouse.demo.topology.OrderEnrichmentJob
import org.apache.flink.configuration.ExternalizedCheckpointRetention
import org.apache.flink.core.execution.CheckpointingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.LoggerFactory
import kotlin.system.exitProcess

fun main() {
    val logger = LoggerFactory.getLogger("Main")
    val config = AppConfig()

    logger.info("Starting OrderEnrichment Application...")

    try {
        // Infrastructure Checks
        ClickHouseUtils.ensureTableExists(config)
        IcebergUtils.ensureTableExists(config)
        KafkaUtils.ensureTopicExists(config)

        // Setup Environment
        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        configureEnvironment(env, config)

        // Build Topology
        OrderEnrichmentJob(config).createTopology(env)

        logger.info("Executing Flink Job: ${config.jobName}")
        env.execute(config.jobName)
    } catch (e: Exception) {
        logger.error("Critical error in Flink job execution", e)
        exitProcess(1)
    }
}

fun configureEnvironment(
    env: StreamExecutionEnvironment,
    config: AppConfig,
) {
    env.enableCheckpointing(config.checkpointInterval, CheckpointingMode.EXACTLY_ONCE)
    env.checkpointConfig.setCheckpointTimeout(config.checkPointTimeout)
    env.checkpointConfig.setExternalizedCheckpointRetention(
        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION,
    )
    env.checkpointConfig.setMinPauseBetweenCheckpoints(config.minPauseBetweenCheckpoints)
    env.checkpointConfig.setMaxConcurrentCheckpoints(config.maxConcurrentCheckpoints)
    env.checkpointConfig.setTolerableCheckpointFailureNumber(config.tolerableCheckpointFailureNumber)
}
