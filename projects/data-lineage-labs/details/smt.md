# OpenLineage Kafka Connect SMT

The SMT enables fully automated, real-time data lineage by emitting OpenLineage events to a compatible backend (such as Marquez). It acts as a **pass-through transform**: records are not modified, but lifecycle hooks in Kafka Connect are used to capture and report rich metadata about data pipelines.

## Supported Connectors

The SMT is designed to work with multiple Kafka Connect source and sink connectors. Currently, it supports:

- **Source Connectors**
  - Amazon MSK Data Generator (`com.amazonaws.mskdatagen.GeneratorSourceConnector`)
- **Sink Connectors**
  - Confluent S3 Sink Connector (`io.confluent.connect.s3.S3SinkConnector`)
  - Iceberg Sink Connector (`io.tabular.iceberg.connect.IcebergSinkConnector`)

## Datasets Creation

The SMT automatically discovers input and output datasets based on the explicit configuration you provide to it.

- **Kafka Topic Datasets**
  - **Identification:** The SMT identifies Kafka topics from the `topics` property in its configuration.
  - **Direction:** It uses the `connector.class` property to determine if the topics are **inputs** (for a sink connector) or **outputs** (for a source connector).
  - **Schema:** If a Schema Registry URL is provided, the SMT will automatically connect to it to fetch the latest Avro schema for the topics. It can be configured to fetch schemas for keys, values, or both. This provides rich, column-level lineage.
- **Sink Output Datasets (S3 & Iceberg)**
  - **Identification:** The SMT identifies the output dataset for sink connectors using sink-specific properties you provide, such as `s3.bucket.name` or `iceberg.catalog`.
  - **Schema:** The SMT intelligently reuses the schema from the connector's _input topic_ and attaches it to the sink's _output dataset_. This provides complete, end-to-end column-level lineage from Kafka into the final data store.

## Lifecycle & Status Management

The SMT automatically tracks the complete lifecycle of a connector run and reports it to OpenLineage.

**START Event**

- **Trigger:** The `START` event is emitted from the `apply()` method. The exact timing depends on the SMT's configuration:
  - **If Schema Reading is Enabled:** The event is emitted on the first record that is processed _after_ the SMT confirms that all required schemas (e.g., `orders-value`) are available in the Schema Registry. This creates a "lazy" emission that solves the race condition and guarantees the schema is correct.
  - **If Schema Reading is Disabled:** The event is emitted immediately on the **first record** processed by the SMT.
- **Event Details:** A **rich `START` event** is emitted. It contains the job, a unique `runId`, and the full input/output datasets with their schemas (if available). This provides immediate, real-time visibility of the job and its lineage as soon as the data is verifiably complete.

**FAIL Event**

- **Trigger:** An exception is caught during record processing in the `apply()` method.
- **Event Details:** A **minimal `FAIL` event** is emitted with the corresponding `runId`. The SMT then re-throws the original exception to allow Kafka Connect's error handling to proceed.

**COMPLETE Event**

- **Trigger:** The connector task is stopped cleanly (e.g., the connector is deleted or updated). This triggers the SMT's `close()` method.
- **Event Details:** A **minimal `COMPLETE` event** is emitted with the corresponding `runId`. This event's only purpose is to update the run's status; it does not contain datasets, preventing the creation of incorrect dataset versions on shutdown.

## Namespaces

The SMT uses a clear and consistent namespacing strategy that aligns with OpenLineage best practices.

| Entity              | Namespace Source                                               | Example              | Purpose                                                                                                           |
| :------------------ | :------------------------------------------------------------- | :------------------- | :---------------------------------------------------------------------------------------------------------------- |
| **Job**             | `OPENLINEAGE_NAMESPACE` environment variable.                  | `fh-local`           | Provides a logical grouping for all your Kafka Connect jobs.                                                      |
| **Kafka Dataset**   | `CONNECT_BOOTSTRAP_SERVERS` environment variable (first host). | `kafka-1:19092`      | Provides the **physical namespace** for the Kafka topic, allowing Marquez to correctly identify it as a `STREAM`. |
| **S3 Dataset**      | The S3 bucket name.                                            | `s3://fh-dev-bucket` | The physical namespace for the S3 bucket.                                                                         |
| **Iceberg Dataset** | The Iceberg catalog name.                                      | `iceberg://demo_ib`  | The physical namespace for the Iceberg catalog.                                                                   |

## Limitations

- **`tasks.max` Must Be `1`:** This SMT is designed for single-task connectors. It manages its state internally for each instance. To ensure a clean, one-to-one mapping between a connector and a single OpenLineage run, you **must** set `"tasks.max": "1"` in your connector configuration.
- **`ABORT` Status Not Supported:** The Kafka Connect SMT API does not provide a way to distinguish between a user-initiated stop (which should be `ABORT`) and a graceful shutdown for reconfiguration. Therefore, all clean shutdowns are reported as `COMPLETE`.
- **Initial Connector Failures:** If a connector fails during its own initialization _before_ any records are processed, the SMT will not have a chance to emit a `START` or `FAIL` event.
- **Schema Versioning on Redeployment:** The SMT waits for schemas to exist before emitting lineage, avoiding race conditions. However, it does not track schema **version changes**. Updating a topic schema (e.g., adding a field) will not create a new dataset version in Marquez.
