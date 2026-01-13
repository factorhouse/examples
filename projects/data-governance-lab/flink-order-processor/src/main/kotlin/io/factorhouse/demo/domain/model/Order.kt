package io.factorhouse.demo.domain.model

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
) : Serializable
