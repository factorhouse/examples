package io.factorhouse.demo.flink

import mu.KotlinLogging
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.functions.AggregateFunction

data class SupplierStatsAccumulator(
    var totalPrice: Double = 0.0,
    var count: Long = 0L,
)

class SupplierStatsAggregator : AggregateFunction<GenericRecord, SupplierStatsAccumulator, SupplierStatsAccumulator> {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    override fun createAccumulator(): SupplierStatsAccumulator = SupplierStatsAccumulator()

    override fun add(
        value: GenericRecord,
        accumulator: SupplierStatsAccumulator,
    ): SupplierStatsAccumulator {
        val priceField = value["price"]
        var priceToAdd = 0.0

        if (priceField != null) {
            try {
                priceToAdd = priceField.toString().toDouble()
            } catch (_: NumberFormatException) {
                logger.warn { "Could not parse price '$priceField' to Double for record. Skipping this price value." }
            }
        } else {
            logger.debug { "Price field is null for a record. Treating as 0.0 for aggregation." }
        }

        return SupplierStatsAccumulator(
            accumulator.totalPrice + priceToAdd,
            accumulator.count + 1,
        )
    }

    override fun getResult(accumulator: SupplierStatsAccumulator): SupplierStatsAccumulator = accumulator

    override fun merge(
        a: SupplierStatsAccumulator,
        b: SupplierStatsAccumulator,
    ): SupplierStatsAccumulator =
        SupplierStatsAccumulator(
            totalPrice = a.totalPrice + b.totalPrice,
            count = a.count + b.count,
        )
}
