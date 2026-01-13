package io.factorhouse.demo.domain.mapper

import io.factorhouse.demo.domain.model.EnrichedOrder
import io.factorhouse.demo.infrastructure.iceberg.IcebergUtils
import org.apache.flink.table.data.GenericRowData
import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.StringData
import org.apache.flink.table.data.TimestampData
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.iceberg.flink.FlinkSchemaUtil

object IcebergMapper {
    // We pre-compute TypeInfo to avoid overhead per record
    val TYPE_INFO = InternalTypeInfo.of(FlinkSchemaUtil.convert(IcebergUtils.SCHEMA))

    fun toRowData(order: EnrichedOrder): RowData {
        val row = GenericRowData(11)
        row.setField(0, StringData.fromString(order.orderId))
        row.setField(1, StringData.fromString(order.userId))
        row.setField(2, StringData.fromString(order.productName))
        row.setField(3, StringData.fromString(order.category))
        row.setField(4, order.quantity)
        row.setField(5, order.unitPrice)
        row.setField(6, TimestampData.fromInstant(order.createdAt))
        if (order.userFirstName != null) row.setField(7, StringData.fromString(order.userFirstName))
        if (order.userLastName != null) row.setField(8, StringData.fromString(order.userLastName))
        if (order.userEmail != null) row.setField(9, StringData.fromString(order.userEmail))
        if (order.userPostalCode != null) row.setField(10, StringData.fromString(order.userPostalCode))
        return row
    }
}
