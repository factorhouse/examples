package io.factorhouse.demo.model

import org.apache.avro.generic.GenericRecord

fun GenericRecord.getDebeziumPayload(): GenericRecord? {
    val after = this.get("after") as? GenericRecord
    val before = this.get("before") as? GenericRecord
    return after ?: before
}
