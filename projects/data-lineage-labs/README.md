# End-to-end data lineage

## Set Up the Environment

### Clone the Project

```bash
git clone https://github.com/factorhouse/examples.git
cd examples
```

### Start all environments

This project uses [Factor House Local](https://github.com/factorhouse/factorhouse-local) to spin up the Kafka and Flink environments, including **Kpow** and **Flex** for monitoring.

Before starting, make sure you have valid licenses for Kpow and Flex. See the [license setup guide](https://github.com/factorhouse/factorhouse-local?tab=readme-ov-file#update-kpow-and-flex-licenses) for instructions.

```bash
# Clone Factor House Local
git clone https://github.com/factorhouse/factorhouse-local.git

# Download necessary connectors and dependencies
./factorhouse-local/resources/setup-env.sh

# Configure edition and licenses
# Community:
# export KPOW_SUFFIX="-ce"
# export FLEX_SUFFIX="-ce"
# Or for Enterprise:
# unset KPOW_SUFFIX
# unset FLEX_SUFFIX
# Licenses:
# export KPOW_LICENSE=<path>
# export FLEX_LICENSE=<path>

# Start Kafka and Flink environments
docker compose -p kpow -f ./factorhouse-local/compose-kpow.yml up -d \
  && docker compose -p flex -f ./factorhouse-local/compose-flex.yml up -d \
  && docker compose -p obsv -f ./factorhouse-local/compose-obsv.yml up -d
```

## Kafka Connect

While Apache Kafka is not a natively supported integration in OpenLineage, lineage tracking can be enabled for Kafka Connect using the **`OpenLineageLifecycleSmt`** Single Message Transform (SMT). For a detailed technical overview, please refer to the [SMT details page](./details/smt.md).

### Kafka Connect Configuration

[Factor House Local](https://github.com/factorhouse/factorhouse-local) includes a Kafka Connect instance with the `OpenLineageLifecycleSmt` pre-installed. This SMT is designed to capture lineage events from Kafka connectors and transmit them to an OpenLineage-compatible backend, such as Marquez.

The SMT is integrated into the Kafka Connect instance via volume mapping and is accessible within the **Kpow environment** (`compose-kpow.yml`).

The necessary environment variables for the SMT are configured as follows:

- `OPENLINEAGE_URL`: The URL of the OpenLineage API endpoint (e.g., `http://marquez-api:5000`).
- `OPENLINEAGE_NAMESPACE`: A logical namespace for organizing Kafka Connect jobs (e.g., `fh-local`).
- `CONNECT_BOOTSTRAP_SERVERS`: The bootstrap servers for the Kafka cluster (e.g., `kafka-1:19092,kafka-2:19093,kafka-3:19094`).

The snippet below shows the OpenLineage integration details for the `connect` service configuration:

```yaml
connect:
  image: confluentinc/cp-kafka-connect:7.8.0
  container_name: connect
  ...
  ports:
    - 8083:8083
  ...
  environment:
    CONNECT_BOOTSTRAP_SERVERS: "kafka-1:19092,kafka-2:19093,kafka-3:19094"
    ...
    CONNECT_PLUGIN_PATH: /usr/share/java/,/etc/kafka-connect/jars,/etc/kafka-connect/plugins
    ## OpenLineage Configuration
    OPENLINEAGE_URL: http://marquez-api:5000
    OPENLINEAGE_NAMESPACE: fh-local
  volumes:
    - ./resources/deps/kafka/connector:/etc/kafka-connect/jars
    - ./resources/kpow/plugins:/etc/kafka-connect/plugins
```

### Connector Deployment

Individual connectors can be configured to use the SMT by adding the transform details to their configuration. The following connectors are available as examples:

- [**Source Connector**](./connectors/orders-source.json): Ingests mock order data into the `orders` topic using the _Amazon MSK Data Generator_.
- [**S3 Sink Connector**](./connectors/orders-s3-sink.json): Reads records from the `orders` topic and writes them to an object storage bucket (MinIO).
- [**Iceberg Sink Connector**](./connectors/orders-iceberg-sink.json): Reads records from the `orders` topic and writes them to an Iceberg table.

Below are the SMT configuration details for each connector. Kpow provides a user-friendly UI and API for deploying Kafka connectors. See [this page](../../fh-local-labs/lab-02/README.md) for more information.

#### Source Connector

```json
{
  "name": "orders-source",
  "config": {
    "connector.class": "com.amazonaws.mskdatagen.GeneratorSourceConnector",
    "tasks.max": "1", // Required to be 1 for the OpenLineage SMT

    //...

    "transforms": "openlineage,unwrapUnion,extractKey,flattenKey,convertBidTime",

    "transforms.openlineage.type": "io.factorhouse.smt.OpenLineageLifecycleSmt",
    "transforms.openlineage.connector.name": "orders-source", // Job name for lineage tracking
    "transforms.openlineage.connector.class": "com.amazonaws.mskdatagen.GeneratorSourceConnector", // Identifies the connector as a source
    "transforms.openlineage.topics": "orders", // Topic for schema discovery
    "transforms.openlineage.key.converter.schema.read": "false", // Skip key schema
    "transforms.openlineage.value.converter.schema.read": "true", // Read value schema (requires Schema Registry)
    "transforms.openlineage.value.converter.schema.registry.url": "http://schema:8081",
    "transforms.openlineage.value.converter.basic.auth.credentials.source": "USER_INFO",
    "transforms.openlineage.value.converter.basic.auth.user.info": "admin:admin"

    //...
  }
}
```

#### S3 Sink Connector

```json
{
  "name": "orders-s3-sink",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "tasks.max": "1", // Required to be 1 for the OpenLineage SMT

    //...

    "transforms": "openlineage",
    "transforms.openlineage.type": "io.factorhouse.smt.OpenLineageLifecycleSmt",
    "transforms.openlineage.connector.name": "orders-s3-sink", // Job name for lineage tracking
    "transforms.openlineage.connector.class": "io.confluent.connect.s3.S3SinkConnector", // Identifies the connector as a sink
    "transforms.openlineage.topics": "orders", // Topic for schema discovery
    "transforms.openlineage.s3.bucket.name": "fh-dev-bucket", // Sets the dataset namespace
    "transforms.openlineage.key.converter.schema.read": "false", // Skip key schema
    "transforms.openlineage.value.converter.schema.read": "true", // Read value schema (requires Schema Registry)
    "transforms.openlineage.value.converter.schema.registry.url": "http://schema:8081",
    "transforms.openlineage.value.converter.basic.auth.credentials.source": "USER_INFO",
    "transforms.openlineage.value.converter.basic.auth.user.info": "admin:admin"

    //...
  }
}
```

#### Iceberg Sink Connector

> **Note:** The target Iceberg table must be created before deploying the Iceberg sink connector.

```json
{
  "name": "orders-iceberg-sink",
  "config": {
    "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "1", // Required to be 1 for the OpenLineage SMT

    //...

    "transforms": "openlineage",
    "transforms.openlineage.type": "io.factorhouse.smt.OpenLineageLifecycleSmt",
    "transforms.openlineage.connector.name": "orders-iceberg-sink", // Job name for lineage tracking
    "transforms.openlineage.connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector", // Identifies the connector as a sink
    "transforms.openlineage.topics": "orders", // Topic for schema discovery
    "transforms.openlineage.iceberg.catalog": "demo_ib", // Sets the dataset namespace
    "transforms.openlineage.key.converter.schema.read": "false", // Skip key schema
    "transforms.openlineage.value.converter.schema.read": "true", // Read value schema (requires Schema Registry)
    "transforms.openlineage.value.converter.schema.registry.url": "http://schema:8081",
    "transforms.openlineage.value.converter.basic.auth.credentials.source": "USER_INFO",
    "transforms.openlineage.value.converter.basic.auth.user.info": "admin:admin"
  }
}
```

##### Create Iceberg Sink Table

1.  Connect to the Spark-Iceberg container:
    ```bash
    docker exec -it spark-iceberg /opt/spark/bin/spark-sql
    ```
2.  Execute the following SQL commands to create the `orders` table:

    ```sql
    USE demo_ib;

    USE `default`;

    CREATE TABLE orders (
        order_id STRING,
        item STRING,
        price DECIMAL(10, 2),
        supplier STRING,
        bid_time TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (DAY(bid_time))
    TBLPROPERTIES (
        'format-version' = '2',
        'write.format.default' = 'parquet',
        'write.target-file-size-bytes' = '134217728',
        'write.parquet.compression-codec' = 'snappy',
        'write.metadata.delete-after-commit.enabled' = 'true',
        'write.metadata.previous-versions-max' = '3',
        'write.delete.mode' = 'copy-on-write',
        'write.update.mode' = 'copy-on-write'
    );
    ```

### Viewing Lineage

After deploying the connectors, you can view the lineage graph in your OpenLineage UI, accessible at [`http://localhost:3003`](http://localhost:3003).

![](./images/connector-lineage.gif)

## Flink Jobs

- TO BE UPDATED

## Spark Job

- TO BE UPDATED

## Shut Down

When you're done, shut down all containers and unset any environment variables:

```bash
# Stop Factor House Local containers
docker compose -p obsv -f ./factorhouse-local/compose-obsv.yml down \
  && docker compose -p flex -f ./factorhouse-local/compose-flex.yml down \
  && docker compose -p kpow -f ./factorhouse-local/compose-kpow.yml down

# Clear environment variables
unset KPOW_SUFFIX FLEX_SUFFIX KPOW_LICENSE FLEX_LICENSE
```
