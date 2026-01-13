package io.factorhouse.demo.domain.model

import java.io.Serializable
import java.time.Instant

data class User(
    val userId: String,
    val firstName: String,
    val lastName: String,
    val email: String,
    val postalCode: String,
    val createdAt: Instant,
) : Serializable
