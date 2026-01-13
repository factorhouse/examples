package io.factorhouse.demo.infrastructure.clickhouse

import com.clickhouse.data.ClickHouseFormat
import io.factorhouse.demo.config.AppConfig
import io.factorhouse.demo.domain.mapper.ClickHouseMapper
import io.factorhouse.demo.domain.model.EnrichedOrder
import org.apache.flink.connector.base.sink.writer.ElementConverter
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor
import org.apache.flink.connector.clickhouse.data.ClickHousePayload
import org.apache.flink.connector.clickhouse.sink.ClickHouseAsyncSink
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig

object ClickHouseSinkFactory {
    fun createSink(config: AppConfig): ClickHouseAsyncSink<EnrichedOrder> {
        val clientConfig =
            ClickHouseClientConfig(
                config.chEndpoint,
                config.chDatabase,
                "",
                "default",
                config.chTable,
            )

        val elementConverter: ElementConverter<EnrichedOrder, ClickHousePayload> =
            ClickHouseConvertor(EnrichedOrder::class.java, ClickHouseMapper())

        val sink =
            ClickHouseAsyncSink<EnrichedOrder>(
                elementConverter,
                1000,
                100,
                2000,
                10 * 1024 * 1024,
                1000,
                10 * 1024 * 1024,
                clientConfig,
            )
        sink.setClickHouseFormat(ClickHouseFormat.TabSeparated)
        return sink
    }
}
