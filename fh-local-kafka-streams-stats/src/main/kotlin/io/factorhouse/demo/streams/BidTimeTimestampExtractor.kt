package io.factorhouse.demo.streams

import mu.KotlinLogging
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

class BidTimeTimestampExtractor : TimestampExtractor {
    private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
    private val logger = KotlinLogging.logger { }

    override fun extract(
        record: ConsumerRecord<Any, Any>,
        partitionTime: Long,
    ): Long =
        try {
            val value = record.value() as? GenericRecord
            val bidTime = value?.get("bid_time")?.toString()
            when {
                bidTime.isNullOrBlank() -> {
                    logger.warn { "Missing or blank 'bid_time'. Falling back to partitionTime: $partitionTime" }
                    partitionTime
                }
                else -> {
                    val parsedTimestamp =
                        LocalDateTime
                            .parse(bidTime, formatter)
                            .atZone(ZoneId.systemDefault())
                            .toInstant()
                            .toEpochMilli()
                    logger.debug { "Extracted timestamp $parsedTimestamp from bid_time '$bidTime'" }
                    parsedTimestamp
                }
            }
        } catch (e: Exception) {
            when (e.cause) {
                is DateTimeParseException -> {
                    logger.error(e) { "Failed to parse 'bid_time'. Falling back to partitionTime: $partitionTime" }
                    partitionTime
                }
                else -> {
                    logger.error(e) { "Unexpected error extracting timestamp. Falling back to partitionTime: $partitionTime" }
                    partitionTime
                }
            }
        }
}
