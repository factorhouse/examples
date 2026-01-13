package io.factorhouse.demo.domain.mapper

import io.factorhouse.demo.domain.model.EnrichedOrder
import org.apache.flink.connector.clickhouse.convertor.POJOConvertor
import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.sql.Timestamp

class ClickHouseMapper : POJOConvertor<EnrichedOrder>() {
    private val tab = '\t'.code
    private val newLine = '\n'.code
    private val nullMarker = "\\N".toByteArray(StandardCharsets.UTF_8)

    override fun instrument(
        out: OutputStream,
        order: EnrichedOrder,
    ) {
        writeField(out, order.orderId)
        out.write(tab)
        writeField(out, order.userId)
        out.write(tab)
        writeField(out, order.productName)
        out.write(tab)
        writeField(out, order.category)
        out.write(tab)
        writeField(out, order.quantity)
        out.write(tab)
        writeField(out, order.unitPrice)
        out.write(tab)
        // Use SQL Timestamp format
        writeField(out, Timestamp.from(order.createdAt).toString())
        out.write(tab)
        writeField(out, order.userFirstName)
        out.write(tab)
        writeField(out, order.userLastName)
        out.write(tab)
        writeField(out, order.userEmail)
        out.write(tab)
        writeField(out, order.userPostalCode)
        out.write(newLine)
    }

    private fun writeField(
        out: OutputStream,
        value: Any?,
    ) {
        if (value == null) {
            out.write(nullMarker)
        } else {
            val str =
                value
                    .toString()
                    .replace("\\", "\\\\")
                    .replace("\t", "\\t")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r")
            out.write(str.toByteArray(StandardCharsets.UTF_8))
        }
    }
}
