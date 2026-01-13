package io.factorhouse.demo.domain.mapper

import io.factorhouse.demo.domain.model.Order
import io.factorhouse.demo.domain.model.User
import org.apache.avro.generic.GenericRecord
import java.io.Serializable
import java.time.Instant

object DebeziumMapper : Serializable {
    private const val serialVersionUID = 1L

    private fun readResolve(): Any = DebeziumMapper

    private fun getPayload(envelope: GenericRecord): GenericRecord? {
        val after = envelope.get("after") as? GenericRecord
        val before = envelope.get("before") as? GenericRecord
        return after ?: before
    }

    private fun parseTimestamp(payload: GenericRecord): Instant {
        val micros = payload.get("created_at") as? Long ?: 0L
        return Instant.ofEpochSecond(
            micros / 1_000_000,
            (micros % 1_000_000) * 1_000,
        )
    }

    fun toUser(envelope: GenericRecord): User? {
        val payload = getPayload(envelope) ?: return null
        return User(
            userId = payload.get("user_id").toString(),
            firstName = payload.get("first_name")?.toString() ?: "",
            lastName = payload.get("last_name")?.toString() ?: "",
            email = payload.get("email")?.toString() ?: "",
            postalCode = payload.get("postal_code")?.toString() ?: "",
            createdAt = parseTimestamp(payload),
        )
    }

    fun toOrder(envelope: GenericRecord): Order? {
        val payload = getPayload(envelope) ?: return null
        return Order(
            orderId = payload.get("order_id").toString(),
            userId = payload.get("user_id").toString(),
            productName = payload.get("product_name").toString(),
            category = payload.get("category").toString(),
            quantity = (payload.get("quantity") as? Number?)?.toInt() ?: 0,
            unitPrice = (payload.get("unit_price") as? Number?)?.toDouble() ?: 0.0,
            createdAt = parseTimestamp(payload),
        )
    }
}
