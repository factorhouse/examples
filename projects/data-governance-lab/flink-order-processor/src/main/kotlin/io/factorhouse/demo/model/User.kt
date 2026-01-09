package io.factorhouse.demo.model

import org.apache.avro.generic.GenericRecord
import java.io.Serializable
import java.time.Instant

data class User(
    val userId: String,
    val firstName: String,
    val lastName: String,
    val email: String,
    val postalCode: String,
    val createdAt: Instant,
) {
    companion object : Serializable {
        private const val serialVersionUID = 20240101L

        fun from(envelope: GenericRecord): User? {
            val payload = envelope.getDebeziumPayload() ?: return null

            val micros = payload.get("created_at") as? Long ?: 0L
            val createdAtInstant =
                Instant.ofEpochSecond(
                    micros / 1_000_000,
                    (micros % 1_000_000) * 1_000,
                )

            return User(
                userId = payload.get("user_id").toString(),
                firstName = payload.get("first_name")?.toString() ?: "",
                lastName = payload.get("last_name")?.toString() ?: "",
                email = payload.get("email")?.toString() ?: "",
                postalCode = payload.get("postal_code")?.toString() ?: "",
                createdAt = createdAtInstant,
            )
        }
    }
}
