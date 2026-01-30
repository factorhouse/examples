package io.factorhouse.demo.infrastructure.iceberg

import io.factorhouse.demo.config.AppConfig
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.data.RowData
import org.apache.iceberg.DistributionMode
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.flink.TableLoader
import org.apache.iceberg.flink.sink.FlinkSink

object IcebergSinkFactory {
    fun createSink(
        stream: DataStream<RowData>,
        config: AppConfig,
    ) {
        val loader = IcebergUtils.createCatalogLoader(config)
        val tableId = TableIdentifier.of(config.icebergDatabase, config.icebergTableName)

        FlinkSink
            .forRowData(stream)
            .tableLoader(TableLoader.fromCatalog(loader, tableId))
            .distributionMode(DistributionMode.HASH)
            .set("write.format.default", "parquet")
            .set("write.target-file-size-bytes", "134217728")
            .append()
    }
}
