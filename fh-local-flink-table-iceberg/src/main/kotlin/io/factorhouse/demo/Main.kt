package io.factorhouse.demo

import mu.KotlinLogging
import kotlin.system.exitProcess

private val logger = KotlinLogging.logger { }

fun main(args: Array<String>) {
    try {
        TableApp.run()
    } catch (e: Exception) {
        logger.error(e) { "Fatal error in ${args.getOrNull(0) ?: "app"}. Shutting down." }
        exitProcess(1)
    }
}
