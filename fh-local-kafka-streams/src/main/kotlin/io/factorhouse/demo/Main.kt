package io.factorhouse.demo

import mu.KotlinLogging
import kotlin.system.exitProcess

private val logger = KotlinLogging.logger {}

fun main() {
    try {
        StreamsApp.run()
    } catch (e: Exception) {
        logger.error(e) { "Fatal error in the streams app. Shutting down." }
        exitProcess(1)
    }
}
