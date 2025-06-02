package io.factorhouse.demo

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import io.factorhouse.avro.SupplierStats
import io.factorhouse.demo.kafka.createTopicIfNotExists
import io.factorhouse.demo.streams.BidTimeTimestampExtractor
import io.factorhouse.kpow.StreamsRegistry
import io.factorhouse.kpow.key.ClusterIdKeyStrategy
import mu.KotlinLogging
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.kstream.Windowed
import java.time.Duration
import java.util.Properties

object StreamsApp {
    private val bootstrapAddress = System.getenv("BOOTSTRAP") ?: "localhost:9092"
    private val inputTopicName = System.getenv("TOPIC") ?: "orders"
    private val registryUrl = System.getenv("REGISTRY_URL") ?: "http://localhost:8081"
    private val registryConfig =
        mapOf(
            "schema.registry.url" to registryUrl,
            "basic.auth.credentials.source" to "USER_INFO",
            "basic.auth.user.info" to "admin:admin",
        )
    private val windowSize = Duration.ofSeconds(5)
    private val gradePeriod = Duration.ofSeconds(5)
    private const val NUM_PARTITIONS = 3
    private const val REPLICATION_FACTOR: Short = 3
    private val logger = KotlinLogging.logger {}

    fun run() {
        // Create output topics if not existing
        val outputTopicName = "supplier-stats-ks"
        createTopicIfNotExists(outputTopicName, bootstrapAddress, NUM_PARTITIONS, REPLICATION_FACTOR)

        val props =
            Properties().apply {
                put(StreamsConfig.APPLICATION_ID_CONFIG, "$outputTopicName-app-id")
                put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress)
                put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()::class.java.name)
                put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest")
                put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, BidTimeTimestampExtractor::class.java.name)
                put("schema.registry.url", registryUrl)
                put("basic.auth.credentials.source", "USER_INFO")
                put("basic.auth.user.info", "admin:admin")
            }

        val keySerde = Serdes.String()
        val valueSerde =
            GenericAvroSerde().apply {
                configure(registryConfig, false)
            }
        val supplierStatsSerde =
            SpecificAvroSerde<SupplierStats>().apply {
                configure(registryConfig, false)
            }

        val builder = StreamsBuilder()
        val source: KStream<String, GenericRecord> = builder.stream(inputTopicName, Consumed.with(keySerde, valueSerde))
        val aggregated: KTable<Windowed<String>, SupplierStats> =
            source
                .map { _, value ->
                    val supplier = value["supplier"]?.toString() ?: "UNKNOWN"
                    val price = value["price"]?.toString()?.toDoubleOrNull() ?: 0.0
                    KeyValue(supplier, price)
                }.groupByKey(Grouped.with(keySerde, Serdes.Double()))
                .windowedBy(TimeWindows.ofSizeAndGrace(windowSize, gradePeriod))
                .aggregate(
                    {
                        SupplierStats
                            .newBuilder()
                            .setWindowStart("")
                            .setWindowEnd("")
                            .setSupplier("")
                            .setTotalPrice(0.0)
                            .setCount(0L)
                            .build()
                    },
                    { key, value, aggregate ->
                        SupplierStats
                            .newBuilder(aggregate)
                            .setSupplier(key)
                            .setTotalPrice(aggregate.totalPrice + value)
                            .setCount(
                                aggregate.count + 1,
                            ).build()
                    },
                    Materialized.with(keySerde, supplierStatsSerde),
                )
        aggregated
            .toStream()
            .map { key, value ->
                val windowStart = key.window().startTime().toString()
                val windowEnd = key.window().endTime().toString()
                val updatedValue =
                    SupplierStats
                        .newBuilder(value)
                        .setWindowStart(windowStart)
                        .setWindowEnd(windowEnd)
                        .build()
                KeyValue(key.key(), updatedValue)
            }.peek { _, value ->
                logger.info { "Supplier stats: $value" }
            }.to(outputTopicName, Produced.with(keySerde, supplierStatsSerde))

        val topology = builder.build()
        val streams = KafkaStreams(topology, props)

        try {
            val streamRegistry = StreamsRegistry(props)
            val keyStrategy = ClusterIdKeyStrategy(props)
            streamRegistry.register(streams, topology, keyStrategy)
            logger.info { "Successfully registered with Kpow StreamsRegistry." }
        } catch (e: Exception) {
            logger.error(e) { "Failed to initialize or register with Kpow StreamsRegistry. Kpow monitoring might be unavailable." }
        }

        try {
            streams.start()
            logger.info { "Kafka Streams started successfully." }

            Runtime.getRuntime().addShutdownHook(
                Thread {
                    logger.info { "Shutting down Kafka Streams..." }
                    streams.close()
                },
            )
        } catch (e: Exception) {
            streams.close(Duration.ofSeconds(5))
            throw RuntimeException("Error while running Kafka Streams", e)
        }
    }
}
