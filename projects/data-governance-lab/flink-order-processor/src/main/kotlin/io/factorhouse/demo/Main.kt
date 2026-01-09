package io.factorhouse.demo

import io.factorhouse.demo.flink.OrderEnrichment
import io.factorhouse.demo.flink.Source
import io.factorhouse.demo.model.Order
import io.factorhouse.demo.model.User
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.configuration.ExternalizedCheckpointRetention
import org.apache.flink.core.execution.CheckpointingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles

fun main(args: Array<String>) {
    val bootstrapAddress = System.getenv("BOOTSTRAP") ?: "kafka-1:19092"
    val registryUrl = System.getenv("REGISTRY_URL") ?: "http://schema:8081"
    val registryConfig =
        mapOf(
            "basic.auth.credentials.source" to "USER_INFO",
            "basic.auth.user.info" to "admin:admin",
        )
    val logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

    try {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.enableCheckpointing(6000, CheckpointingMode.EXACTLY_ONCE)
        env.checkpointConfig.setCheckpointTimeout(6000)
        env.checkpointConfig.setExternalizedCheckpointRetention(
            ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION,
        )
        env.checkpointConfig.setCheckpointTimeout(6000)
        env.checkpointConfig.setMinPauseBetweenCheckpoints(500)
        env.checkpointConfig.setMaxConcurrentCheckpoints(1)
        env.checkpointConfig.setTolerableCheckpointFailureNumber(3)

        val userStream =
            Source.createDebeziumStream<User>(
                env = env,
                topicName = "ecomm.demo.users",
                bootstrapAddress = bootstrapAddress,
                registryUrl = registryUrl,
                registryConfig = registryConfig,
                mapper = User::from,
            )

        val orderStream =
            Source.createDebeziumStream<Order>(
                env = env,
                topicName = "ecomm.demo.orders",
                bootstrapAddress = bootstrapAddress,
                registryUrl = registryUrl,
                registryConfig = registryConfig,
                mapper = Order::from,
            )

        val userStateDescriptor: MapStateDescriptor<String, User> =
            MapStateDescriptor(
                "user-broadcast-state",
                String::class.java,
                User::class.java,
            )
        val broadcastUserStream = userStream.broadcast(userStateDescriptor)
        val enrichedStream =
            orderStream
                .keyBy { it.orderId }
                .connect(broadcastUserStream)
                .process(OrderEnrichment(userStateDescriptor))

        enrichedStream.print()

        env.execute("OrderProcessor")
    } catch (e: Exception) {
        logger.error("Critical error in Flink job execution", e)
    }
}
