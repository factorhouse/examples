package io.factorhouse.demo.openlineage

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import io.openlineage.client.OpenLineage
import io.openlineage.client.transports.HttpTransport
import mu.KotlinLogging
import org.apache.avro.Schema
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.TableIdentifier
import java.net.URI
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.UUID
import org.apache.avro.Schema as AvroSchema

class Integration(
    private val jobName: String,
    private val jobNamespace: String,
    private val registryUrl: String,
    private val openLineageUrl: String,
    private val kafkaNamespace: String,
) {
    private val logger = KotlinLogging.logger {}
    private val ol = OpenLineage(URI.create("https://github.com/OpenLineage/OpenLineage"))
    private val transport: HttpTransport = HttpTransport.builder().uri(URI.create(openLineageUrl)).build()
    val runId: UUID = UUID.randomUUID()

    companion object {
        private const val DEFAULT_CACHE_CAPACITY = 1000
    }

    fun emitRichEvent(
        topicName: String,
        schemaType: String = "value",
        icebergCatalog: Catalog,
        tableIdentifier: TableIdentifier,
        datasetNamespace: String,
        datasetName: String,
        eventType: OpenLineage.RunEvent.EventType,
    ) {
        logger.info { "Emitting rich OpenLineage $eventType event for run ID: $runId" }
        val jobTypeFacet = ol.newJobTypeJobFacet("STREAMING", "FLINK", "CUSTOM_FLINK_JOB")
        val event =
            ol
                .newRunEventBuilder()
                .eventTime(ZonedDateTime.now(ZoneOffset.UTC))
                .eventType(eventType)
                .run(ol.newRunBuilder().runId(runId).build())
                .job(
                    ol
                        .newJobBuilder()
                        .namespace(jobNamespace)
                        .name(jobName)
                        .facets(ol.newJobFacetsBuilder().jobType(jobTypeFacet).build())
                        .build(),
                ).inputs(listOf(buildKafkaInputDataset(topicName, schemaType)))
                .outputs(listOf(buildIcebergOutputDataset(icebergCatalog, tableIdentifier, datasetNamespace, datasetName)))
                .build()
        transport.emit(event)
    }

    fun emitMinimalEvent(eventType: OpenLineage.RunEvent.EventType) {
        logger.info { "Emitting minimal OpenLineage $eventType event for run ID: $runId" }
        val jobTypeFacet = ol.newJobTypeJobFacet("STREAMING", "FLINK", "CUSTOM_FLINK_JOB")
        val event =
            ol
                .newRunEventBuilder()
                .eventTime(ZonedDateTime.now(ZoneOffset.UTC))
                .eventType(eventType)
                .run(ol.newRunBuilder().runId(runId).build())
                .job(
                    ol
                        .newJobBuilder()
                        .namespace(jobNamespace)
                        .name(this.jobName)
                        .facets(ol.newJobFacetsBuilder().jobType(jobTypeFacet).build())
                        .build(),
                ).build()
        transport.emit(event)
    }

    fun close() {
        transport.close()
    }

    fun buildIcebergOutputDataset(
        icebergCatalog: Catalog,
        tableIdentifier: TableIdentifier,
        datasetNamespace: String,
        datasetName: String,
    ): OpenLineage.OutputDataset {
        val table = icebergCatalog.loadTable(tableIdentifier)
        val schema = table.schema()

        val fields =
            schema.columns().map { field ->
                ol
                    .newSchemaDatasetFacetFieldsBuilder()
                    .name(field.name())
                    .type(field.type().toString().lowercase())
                    .build()
            }
        val schemaFacet = ol.newSchemaDatasetFacetBuilder().fields(fields).build()

        return ol
            .newOutputDatasetBuilder()
            .namespace(datasetNamespace)
            .name(datasetName)
            .facets(
                ol
                    .newDatasetFacetsBuilder()
                    .schema(schemaFacet)
                    .dataSource(ol.newDatasourceDatasetFacet("iceberg", URI.create(datasetNamespace)))
                    .build(),
            ).build()
    }

    private fun buildKafkaInputDataset(
        topicName: String,
        schemaType: String = "value",
    ): OpenLineage.InputDataset {
        val valueFields = buildSchemaFields(topicName, schemaType)
        val schemaFacet = ol.newSchemaDatasetFacetBuilder().fields(valueFields).build()
        val dsFacet = ol.newDatasourceDatasetFacet("kafka", URI.create(kafkaNamespace))
        val facets =
            ol
                .newDatasetFacetsBuilder()
                .schema(schemaFacet)
                .dataSource(dsFacet)
                .build()

        val dataSet =
            ol
                .newInputDatasetBuilder()
                .namespace(kafkaNamespace)
                .name(topicName)
                .facets(facets)
                .build()
        return dataSet
    }

    private fun buildSchemaFields(
        topicName: String,
        type: String,
    ): List<OpenLineage.SchemaDatasetFacetFields> {
        val subject = "$topicName-$type"
        return try {
            val client = initializeSchemaRegistryClient() ?: return emptyList()
            logger.info("Fetching schema for subject '$subject'.")
            val schemaMetadata = client.getLatestSchemaMetadata(subject)
            val parsedSchema = client.getSchemaById(schemaMetadata.id)
            if (parsedSchema.schemaType() == "AVRO") {
                val avroSchema = parsedSchema.rawSchema() as Schema
                avroSchema.fields.map { field ->
                    val fieldSchema = field.schema()
                    val fieldType =
                        if (fieldSchema.type == AvroSchema.Type.UNION) {
                            fieldSchema.types
                                .find { it.type != AvroSchema.Type.NULL }
                                ?.type
                                ?.name ?: fieldSchema.type.name
                        } else {
                            fieldSchema.type.name
                        }

                    ol
                        .newSchemaDatasetFacetFieldsBuilder()
                        .name(field.name())
                        .type(fieldType.lowercase())
                        .build()
                }
            } else {
                emptyList()
            }
        } catch (e: RestClientException) {
            logger.warn("Could not find schema for subject '$subject': ${e.message}")
            emptyList()
        } catch (e: Exception) {
            logger.error("Error fetching schema for subject '$subject'.", e)
            emptyList()
        }
    }

    private fun initializeSchemaRegistryClient(): SchemaRegistryClient? =
        try {
            val srConfig = mutableMapOf<String, Any>()
            srConfig["basic.auth.credentials.source"] = "USER_INFO"
            srConfig["basic.auth.user.info"] = "admin:admin"
            val restService = RestService(listOf(registryUrl))
            restService.configure(srConfig)
            CachedSchemaRegistryClient(restService, DEFAULT_CACHE_CAPACITY)
        } catch (_: Exception) {
            logger.error("Failed to initialize Schema Registry client")
            null
        }
}
