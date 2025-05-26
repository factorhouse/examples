## Flink SQL for Orders Sink in Parquet

This lab shows how to ingest Avro-encoded order records from a Kafka topic into a Flink SQL source table, and write them to an Iceberg table stored in object storage (MinIO) using the Iceberg connector. Since Flink SQL does not support defining Iceberg's hidden partitioning, the sink table is created using Spark SQL instead.

## How to start

### Clone project repository

```bash
git clone https://github.com/factorhouse/examples.git
cd examples
```

### Start Kafka, Flink and analytics environments

We'll use [Factor House Local](https://github.com/factorhouse/factorhouse-local) to quickly spin up Kafka and Flink environments that includes **Kpow** and **Flex** as well as an analytics environment for Iceberg. We can use either the Community or Enterprise editions of Kpow/Flex. **To begin, ensure valid licenses are available.** For details on how to request and configure a license, refer to [this section](https://github.com/factorhouse/factorhouse-local?tab=readme-ov-file#update-kpow-and-flex-licenses) of the project _README_.

```bash
git clone https://github.com/factorhouse/factorhouse-local.git

docker compose -p kpow -f ./factorhouse-local/compose-kpow-community.yml up -d \
  && docker compose -p flex -f ./factorhouse-local/compose-flex-community.yml up -d \
  && docker compose -p analytics -f ./factorhouse-local/compose-analytics.yml up -d
```

### Deploy source connector

We will create a source connector that generates fake order records to a Kafka topic (`orders`). See the [Kafka Connect via Kpow UI and API](../fh-local-kafka-connect-orders/) lab for details about how to create the connector.

Once deployed, we can check the connector and its tasks in Kpow.

![](./images/kafka-connector.png)

### Create sink table

Flink SQL doesn't support [Iceberg's hidden partitioning](https://iceberg.apache.org/docs/nightly/partitioning/#icebergs-hidden-partitioning). Therefore, we will create the sink table using Spark SQL.

```bash
docker exec -it spark-iceberg spark-sql
```

```sql
--// demo is the default catalog
show catalogs;
-- demo
-- spark_catalog

CREATE TABLE demo.db.orders (
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

Once created, the table will appear with metadata only (no data yet):

![](./images/sink-table-01.png)

### Create pipeline

This example runs in the Flink SQL client, which can be started as shown below.

```bash
docker exec -it jobmanager ./bin/sql-client.sh
```

#### Load dependent JARs

We begin by loading the necessary JAR files for the Apache Kafka SQL connector and Confluent Avro format support.

```sql
ADD JAR 'file:///tmp/connector/flink-sql-connector-kafka-3.3.0-1.20.jar';
ADD JAR 'file:///tmp/connector/flink-sql-avro-confluent-registry-1.20.1.jar';

show jars;
-- +-------------------------------------------------------------+
-- |                                                        jars |
-- +-------------------------------------------------------------+
-- |     /tmp/connector/flink-sql-connector-kafka-3.3.0-1.20.jar |
-- | /tmp/connector/flink-sql-avro-confluent-registry-1.20.1.jar |
-- +-------------------------------------------------------------+
-- 2 rows in set
```

#### Create source table

The source table is defined using the **Kafka SQL connector**, enabling Flink to consume **Avro-encoded messages** from the `orders` Kafka topic. To support time-based processing and potential windowed aggregations, an event-time watermark is introduced on `bid_time` using `WATERMARK FOR bid_time AS bid_time - INTERVAL '5' SECOND`. This watermark allows Flink to track event time progress and handle out-of-order events, which is required for time-based operations such as windowed aggregations or joins.

> 💡 The watermark definition can be omitted in this lab because the goal is simply to write Kafka records to an object storage (_MinIO_) without performing time-base transformations. However, including it prepares the pipeline for future event-time logic.

```sql
CREATE TABLE orders (
  order_id     STRING,
  item         STRING,
  price        STRING,
  supplier     STRING,
  bid_time     TIMESTAMP(3),
  WATERMARK FOR bid_time AS bid_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders',
  'properties.bootstrap.servers' = 'kafka-1:19092',
  'format' = 'avro-confluent',
  'avro-confluent.schema-registry.url' = 'http://schema:8081',
  'avro-confluent.basic-auth.credentials-source' = 'USER_INFO',
  'avro-confluent.basic-auth.user-info' = 'admin:admin',
  'avro-confluent.schema-registry.subject' = 'orders-value',
  'scan.startup.mode' = 'earliest-offset'
);

-- select * from orders;
```

#### Insert into sink table

A sink table, `orders_sink`, is defined to write processed order records to **object storage** (e.g., **MinIO**) in **Parquet format**. The data is **partitioned by bid date, hour, and minute** to enable efficient querying and organization.

The table uses the **Filesystem connector** with the `s3a://` scheme for writing to S3-compatible storage. Partition commit policies and rolling options are configured to optimize file size and latency.

```sql
SET 'parallelism.default' = '3';
SET 'execution.checkpointing.interval' = '60000';

CREATE CATALOG demo WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://rest:8181',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://minio:9000',
    's3.path-style-access' = 'true',
    's3.access-key-id' = 'admin',
    's3.secret-access-key' = 'password'
);

show catalogs;
-- +-----------------+
-- |    catalog name |
-- +-----------------+
-- | default_catalog |
-- |            demo |
-- +-----------------+
-- 2 rows in set

INSERT INTO demo.db.orders
SELECT
    order_id,
    item,
    CAST(price AS DECIMAL(10, 2)),
    supplier,
    bid_time
FROM orders;
```

We can monitor the Flink job via the Flink UI (`localhost:8081`) or Flex (`localhost:3001`). The screenshot below shows the job's logical plan as visualized in Flex.

![](./images/flex-01.png)

In addition to monitoring the job, we can verify the output by inspecting the Parquet files written by the sink. As shown in the screenshot below, the records are successfully written to the configured partitions in the MinIO bucket (`warehouse`).

![](./images/minio-01.png)

### Shutdown environment

Finally, stop and remove the Docker containers.

```bash
docker compose -p analytics -f ./factorhouse-local/compose-analytics.yml down \
  && docker compose -p flex -f ./factorhouse-local/compose-flex-community.yml down \
  && docker compose -p kpow -f ./factorhouse-local/compose-kpow-community.yml down
```
