package io.factorhouse.demo.domain.model

import java.time.Instant

data class EnrichedOrder(
    val orderId: String,
    val productName: String,
    val category: String,
    val quantity: Int,
    val unitPrice: Double,
    val createdAt: Instant,
    val userId: String,
    val userFirstName: String?,
    val userLastName: String?,
    val userEmail: String?,
    val userPostalCode: String?,
)
