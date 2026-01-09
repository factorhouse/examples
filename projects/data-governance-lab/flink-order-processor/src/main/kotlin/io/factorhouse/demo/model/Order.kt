package io.factorhouse.demo.model

import org.apache.avro.generic.GenericRecord
import java.io.Serializable
import java.time.Instant

data class Order(
    val orderId: String,
    val userId: String,
    val productName: String,
    val category: String,
    val quantity: Int,
    val unitPrice: Double,
    val createdAt: Instant,
) {
    companion object : Serializable {
        private const val serialVersionUID = 20240101L

        fun from(envelope: GenericRecord): Order? {
            val payload = envelope.getDebeziumPayload() ?: return null

            val micros = payload.get("created_at") as? Long ?: 0L
            val createdAtInstant =
                Instant.ofEpochSecond(
                    micros / 1_000_000,
                    (micros % 1_000_000) * 1_000,
                )

            return Order(
                orderId = payload.get("order_id").toString(),
                userId = payload.get("user_id").toString(),
                productName = payload.get("product_name").toString(),
                category = payload.get("category").toString(),
                quantity = (payload.get("quantity") as? Number?)?.toInt() ?: 0,
                unitPrice = (payload.get("unit_price") as? Number?)?.toDouble() ?: 0.0,
                createdAt = createdAtInstant,
            )
        }
    }
}
