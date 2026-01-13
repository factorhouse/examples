package io.factorhouse.demo.topology

import io.factorhouse.demo.config.AppConfig
import io.factorhouse.demo.domain.mapper.DebeziumMapper
import io.factorhouse.demo.domain.mapper.IcebergMapper
import io.factorhouse.demo.domain.model.Order
import io.factorhouse.demo.domain.model.User
import io.factorhouse.demo.infrastructure.clickhouse.ClickHouseSinkFactory
import io.factorhouse.demo.infrastructure.iceberg.IcebergSinkFactory
import io.factorhouse.demo.infrastructure.kafka.KafkaSinkFactory
import io.factorhouse.demo.infrastructure.kafka.KafkaSourceFactory
import io.factorhouse.demo.topology.processing.MISSING_ORDERS_TAG
import io.factorhouse.demo.topology.processing.UserOrderJoinFunction
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.kafka.clients.consumer.OffsetResetStrategy

class OrderEnrichmentJob(
    private val config: AppConfig,
) {
    fun createTopology(env: StreamExecutionEnvironment) {
        val sourceFactory = KafkaSourceFactory(env, config)

        // 1. Create Source Streams
        val userStream =
            sourceFactory.createStream(
                config.userTopic,
                User::class.java,
                DebeziumMapper::toUser,
                getOffsetsInitializer("ALWAYS_EARLIEST"),
            )

        val orderStream =
            sourceFactory.createStream(
                config.orderTopic,
                Order::class.java,
                DebeziumMapper::toOrder,
                getOffsetsInitializer("LATEST"),
            )

        // 2. Enrich Order Stream
        val enrichedStream =
            orderStream
                .keyBy { it.userId }
                .connect(userStream.keyBy { it.userId })
                .process(UserOrderJoinFunction())
                .name("EnrichmentProcess")

        // 3. Sink: ClickHouse
        enrichedStream
            .sinkTo(ClickHouseSinkFactory.createSink(config))
            .name("ClickHouseSink")

        // 4. Sink: Iceberg
        // Convert to RowData first
        val rowDataStream =
            enrichedStream
                .map { IcebergMapper.toRowData(it) }
                .returns(IcebergMapper.TYPE_INFO)
                .name("MapToIcebergRowData")

        IcebergSinkFactory.createSink(rowDataStream, config)

        // 5. Handle Side Output (Missing Orders)
        val missingOrdersStream = enrichedStream.getSideOutput(MISSING_ORDERS_TAG)
        val missingOrdersSink =
            KafkaSinkFactory.createSink<Order>(
                topic = config.missingOrderTopic,
                bootstrap = config.bootstrapAddress,
            )
        missingOrdersStream
            .sinkTo(missingOrdersSink)
            .name("MissingOrdersDLQ")
    }

    private fun getOffsetsInitializer(strategy: String): OffsetsInitializer =
        when (strategy.uppercase()) {
            "LATEST" -> OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST)

            "EARLIEST" -> OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)

            // Useful if you want to force re-reading from start despite checkpoints
            "ALWAYS_EARLIEST" -> OffsetsInitializer.earliest()

            "ALWAYS_LATEST" -> OffsetsInitializer.latest()

            else -> OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)
        }
}
