# End-to-end data lineage

## Kafka Connect

## Flink jobs

### Real-Time Supplier Stats (DataStream API)

This application uses Flink's DataStream API to perform real-time aggregations on a Kafka stream.

- **OpenLineage Integration Method:**

  - **Programmatic Listener Registration:** This job uses the **`OpenLineageFlinkJobListener`** (`v1.15.0`) provided by the `openlineage-flink` library. The listener is created using its `.builder()` and registered directly with the `StreamExecutionEnvironment` in the application code.
  - **Automatic Discovery:** The listener's internal "visitors" are responsible for automatically inspecting the Flink job graph to discover the input (`KafkaSource`) and output (`FlinkKafkaProducer`) datasets.

- **Key Workarounds and Limitations:**
  - **Older Library Version Required:** It must use `openlineage-flink:1.15.0`. Newer versions have a faulty version-check that crashes the application when used with modern Kafka connectors.
  - **Deprecated Flink Sink Required:** It must use the deprecated `FlinkKafkaProducer` instead of the modern `KafkaSink`. This is a necessary workaround because the listener's visitor in this version cannot correctly inspect the modern sink to discover the output dataset.
  - **CRITICAL LIMITATION - Final Status is Not Reported:** When the job is canceled via the Flink UI or the cluster is destroyed, the final `ABORT` status is **not** sent to Marquez. The job remains stuck in the `RUNNING` state. This is due to a fundamental lifecycle mismatch in Flink's Application Mode, where the process hosting the listener is terminated too abruptly for it to send the final event.

### Kafka to Iceberg Ingestion (Table API)

This application uses Flink's Table API to ingest data from a Kafka topic into an Apache Iceberg table.

- **OpenLineage Integration Method:**

  - **Manual Orchestration:** This job **bypasses the `OpenLineageFlinkJobListener` entirely**. It uses the core `openlineage-java` client (`v1.37.0`) to manually control the lineage lifecycle from within the application's `main` method.
  - **Dynamic Discovery:** Custom utility classes (`DataSet.kt`, `Integration.kt`) are used to dynamically discover the schemas of the input Kafka topic and the output Iceberg table.
  - **Explicit Event Emission:** The application uses a `try...catch...finally` block to guarantee the emission of the correct lifecycle events: `START` (before submission), `RUNNING` (after submission), and a final `COMPLETE`, `ABORT`, or `FAIL` based on the job's outcome.

- **Key Workarounds and Limitations:**
  - **More Explicit Code Required:** This approach requires more boilerplate code in the main application to manage the lifecycle, making it less of a "plug-and-play" solution.
  - **Application-Level Dependencies:** The application's fat JAR must include the Iceberg and Hive client libraries (`implementation` scope) so that the custom utility code can connect to the Hive Metastore and inspect the table schema at runtime.
  - **No Known Limitations:** This manual method is **fully robust**. It correctly discovers all datasets and, most importantly, **guarantees that the final `ABORT` or `FAIL` status is sent** when the job is canceled or fails.

## Spark job

## Viewing lineage on Marquez

## Shut down
