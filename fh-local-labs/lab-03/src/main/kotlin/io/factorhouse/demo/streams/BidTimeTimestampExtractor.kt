package io.factorhouse.demo.streams

import mu.KotlinLogging
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor

class BidTimeTimestampExtractor : TimestampExtractor {
    private val logger = KotlinLogging.logger { }

    override fun extract(
        record: ConsumerRecord<Any, Any>,
        partitionTime: Long,
    ): Long {
        val value = record.value() as? GenericRecord
        val bidTime = value?.get("bid_time") as? Long
        return when {
            bidTime != null -> {
                logger.debug { "Extracted timestamp $bidTime from bid_time field." }
                bidTime
            }
            else -> {
                logger.warn { "Missing or invalid 'bid_time'. Falling back to partitionTime: $partitionTime" }
                partitionTime
            }
        }
    }
}
