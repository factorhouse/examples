package io.factorhouse.demo.config

import java.io.Serializable

data class AppConfig(
    // Kafka
    val bootstrapAddress: String = System.getenv("BOOTSTRAP") ?: "kafka-1:19092",
    val registryUrl: String = System.getenv("REGISTRY_URL") ?: "http://schema:8081",
    val registryCredentials: Map<String, String> =
        mapOf(
            "basic.auth.credentials.source" to "USER_INFO",
            "basic.auth.user.info" to "admin:admin",
        ),
    val userTopic: String = "ecomm.demo.users",
    val orderTopic: String = "ecomm.demo.orders",
    val missingOrderTopic: String = "missing-orders",
    // Flink / State
    val checkpointInterval: Long = 10_000,
    val checkPointTimeout: Long = 6_000,
    val minPauseBetweenCheckpoints: Long = 500,
    val maxConcurrentCheckpoints: Int = 1,
    val tolerableCheckpointFailureNumber: Int = 3,
    // Iceberg / S3
    val s3Endpoint: String = System.getenv("S3_ENDPOINT") ?: "http://minio:9000",
    val hmsEndpoint: String = System.getenv("HMS_ENDPOINT") ?: "thrift://hive-metastore:9083",
    val icebergCatalogName: String = "demo_ib",
    val icebergWarehouse: String = "s3a://warehouse/",
    val icebergTableName: String = "orders_enriched",
    // ClickHouse
    val chEndpoint: String = System.getenv("CH_ENDPOINT") ?: "http://ch-server:8123",
    val chDatabase: String = "default",
    val chTable: String = "orders_enriched",
) : Serializable
