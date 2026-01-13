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
        val sql =
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

        val client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()
        val request =
            HttpRequest
                .newBuilder()
                .uri(URI.create(config.chEndpoint))
                .header("X-ClickHouse-User", "default")
                .POST(HttpRequest.BodyPublishers.ofString(sql))
                .build()

        try {
            val response = client.send(request, HttpResponse.BodyHandlers.ofString())
            if (response.statusCode() == 200) {
                logger.info("ClickHouse table '${config.chTable}' verification successful.")
            } else {
                throw RuntimeException("Failed to check ClickHouse table: ${response.body()}")
            }
        } catch (e: Exception) {
            logger.error("Could not connect to ClickHouse at startup: ${e.message}")
            throw e
        }
    }
}
