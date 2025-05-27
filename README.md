![factorhouse](./images/factorhouse.jfif)

## Product Feature

- [Manage Kafka Consumer Offsets with Kpow](./offset-management/)
  - Python Kafka producer and consumer clients that are used to showcase consumer group offset management capabilities of Kpow.
  - [Manage Kafka Consumer Offsets with Kpow](https://factorhouse.io/blog/how-to/manage-kafka-consumer-offsets-with-kpow/) (blog post)

## Integration

- [Setting Up Kpow with Confluent Cloud](https://factorhouse.io/blog/how-to/set-up-kpow-with-confluent-cloud/)
  - A step-by-step guide to configuring Kpow with Confluent Cloud resources including Kafka clusters, Schema Registry, Kafka Connect, and kSQLDB.
- [Set Up Kpow with Amazon Managed Streaming for Apache Kafka](https://factorhouse.io/blog/how-to/set-up-kpow-with-aws/)
  - A comprehensive, step by step guide to provisioning AWS MSK infrastructure, configuring authentication with the OAUTHBEARER mechanism using AWS IAM, setting up a client EC2 instance within the same VPC, and deploying Kpow.
- [Set Up Kpow with Google Cloud Managed Service for Apache Kafka](https://factorhouse.io/blog/how-to/set-up-kpow-with-gcp/)
  - A practical, step-by-step guide on setting up a Google Cloud Managed Service for Apache Kafka cluster and connecting it from Kpow using the OAUTHBEARER mechanism.

## Factor House Local

Looking for local development environment? Check out [Factor House Local](https://github.com/factorhouse/factorhouse-local).

It provides a collection of pre-configured Docker Compose environments that demonstrate modern data platform architectures. Each setup is purpose-built around a specific use case and incorporates widely adopted technologies such as Kafka, Flink, Spark, Iceberg, and Pinot. These environments are further enhanced by enterprise-grade tools from Factor House: [**Kpow**](https://factorhouse.io/kpow), for Kafka management and control, and [**Flex**](https://factorhouse.io/flex/), for seamless integration with Flink.

### Factor House Local Labs

- [Lab 1: Kafka Clients for Orders with Avro and Schema Registry](./fh-local-kafka-clients-orders/)
  - This lab includes a hands-on example using Python-based Kafka producers and consumers to work with Avro serialization and Confluent Schema Registry. It showcases schema evolution with both generic and specific records.
- [Lab 2: Kafka Connect for Orders via the Kpow UI and API](./fh-local-kafka-connect-orders/)
  - This lab demonstrates how to deploy Kafka connectors via the Kpow UI and API. It walks through end-to-end examples for creating, monitoring, and managing source and sink connectors using both interfaces.
- [Lab 3: Kafka Streams for Supplier Statistics with the Kpow Streams Agent](./fh-local-kafka-streams-stats/)
  - This lab introduces a Kafka Streams application that reads records from the `orders` topic, calculates supplier statistics, and sends them to a new Kafka topic (`orders-supplier-stats`). It includes integration with the [Kpow Streams Agent](https://github.com/factorhouse/kpow-streams-agent) for live topology visualization.
- [Lab 4: Flink SQL with Kafka Source and Sink for Supplier Statistics](./fh-local-flink-sql-client-stats/)
  - This lab illustrates a Flink SQL pipeline that reads Avro-encoded order records from a Kafka topic into a source table, performs 5-second tumbling window aggregations to compute supplier statistics, and writes the results to a Kafka sink table with Confluent Avro format.
- Lab 5: Flink DataStream Application for Supplier Statistics
  - In progress
- [Lab 6: Flink SQL for Orders Sink with Parquet](./fh-local-flink-sql-orders-parquet/)
  - This lab ingests Avro-encoded orders from Kafka into a Flink SQL source table and writes them to MinIO as Parquet files using the Filesystem connector.
- [Lab 7: Flink SQL for Orders Sink in Iceberg](./fh-local-flink-sql-orders-iceberg/)
  - This lab shows how to ingest Avro-encoded order records from a Kafka topic into a Flink SQL source table, and write them to an Iceberg table stored in object storage (MinIO) using the Iceberg connector. Since Flink SQL does not support defining Iceberg's hidden partitioning, the sink table is created using Spark SQL instead.
- Lab 8: Flink Table Application for Orders Sink in Iceberg
  - In progress
- [Lab 9: Kafka Connect for Orders Sink in Iceberg](./fh-local-kafka-connect-iceberg/)
  - This lab demonstrates streaming Avro messages from Kafka into an Iceberg table using Kafka Connect. The target table is pre-defined via Spark SQL with custom partitions using Iceberg's hidden partitioning, and data is written to MinIO as partitioned Parquet files once deployed through Kpow.
- [Lab 10: Spark SQL for Orders Sink in Iceberg](./fh-local-spark-orders-iceberg/)
  - This lab shows how to stream Avro-encoded order records from a Kafka topic into a PySpark Structured Streaming job and write them to an Iceberg table in MinIO. The Avro schema is resolved via Schema Registry using ABRiS, and all dependencies are bundled in an Uber JAR.
- Lab 11: Flink SQL Gateway for Supplier Statistics
  - In progress
- Lab 12: Pinot Analytics for Supplier Statistics
  - In progress

## Support

Any issues? Contact [support](https://factorhouse.io/support/) or view our [docs](https://docs.factorhouse.io/).

## License

This repository is released under the Apache 2.0 License.

Copyright © Factor House.
