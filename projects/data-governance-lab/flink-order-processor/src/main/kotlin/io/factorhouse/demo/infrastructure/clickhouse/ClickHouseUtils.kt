package io.factorhouse.demo.infrastructure.clickhouse

import io.factorhouse.demo.config.AppConfig
import org.slf4j.LoggerFactory
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

object ClickHouseUtils {
    private val logger = LoggerFactory.getLogger(ClickHouseUtils::class.java)

    fun ensureTableExists(config: AppConfig) {
        val client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()

        val createDbSql = "CREATE DATABASE IF NOT EXISTS ${config.chDatabase}"
        executeSql(client, config, createDbSql)

        val createTableSql =
            """
            CREATE TABLE IF NOT EXISTS ${config.chDatabase}.${config.chTable} (
                order_id String,
                user_id String,
                product_name String,
                category String,
                quantity Int32,
                unit_price Float64,
                created_at DateTime64(3),
                user_first_name Nullable(String),
                user_last_name Nullable(String),
                user_email Nullable(String),
                user_postal_code Nullable(String)
            ) ENGINE = MergeTree()
            ORDER BY (created_at, category);
            """.trimIndent()

        executeSql(client, config, createTableSql)
    }

    private fun executeSql(
        client: HttpClient,
        config: AppConfig,
        sql: String,
    ) {
        val request =
            HttpRequest
                .newBuilder()
                .uri(URI.create(config.chEndpoint))
                .header("X-ClickHouse-User", config.chUser)
                .POST(HttpRequest.BodyPublishers.ofString(sql))
                .build()

        try {
            val response = client.send(request, HttpResponse.BodyHandlers.ofString())
            if (response.statusCode() == 200) {
                logger.info("Executed SQL successfully on ClickHouse.")
            } else {
                throw RuntimeException("Failed to execute SQL: ${response.body()}")
            }
        } catch (e: Exception) {
            logger.error("ClickHouse connection error: ${e.message}")
            throw e
        }
    }
}
