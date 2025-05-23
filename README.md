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

Looking for local development environment? Check out the [factorhouse-local](https://github.com/factorhouse/factorhouse-local) project.

It offers a comprehensive Docker Compose-based environment for running [Factor House products](https://factorhouse.io/) - Kpow and Flex - alongside essential supporting infrastructure. It includes pre-configured stacks for various scenarios, such as Kafka development and monitoring with Kpow, real-time analytics with Flex and Apache Pinot, and more, enabling users to explore and test these tools in a local setup.

### Factor House Local Labs

- [Lab 1: Kafka Clients for Orders with Avro and Schema Registry](./fh-local-kafka-clients-orders/)
  - Python Kafka producer and consumer that are are used to demonstrate Avro (de)serialization and schema registry integration.
- [Lab 2: Kafka Connect for Orders via the Kpow UI and API](./fh-local-kafka-connect-orders/)
  - This example demonstrates how to interact with Kafka Connect through both the Kpow UI and its Connect API. We’ll walk through end-to-end examples for creating, monitoring, and managing source and sink connectors using both interfaces.
- [Lab 3: Kafka Streams for Supplier Statistics with the Kpow Streams Agent](./fh-local-kafka-streams-stats/)
  - The Kafka Streams application reads records from the `orders` topic, calculates supplier statistics, and sends them to a new Kafka topic (`orders-supplier-stats`). It includes integration with the [Kpow Streams Agent](https://github.com/factorhouse/kpow-streams-agent) for live topology visualization.
- [Lab 4: Flink SQL with Kafka Source and Sink for Supplier Statistics](./fh-local-flink-sql-client-stats/)
  - This example demonstrates a Kafka SQL source connector that ingests order records from a Kafka topic into a Flink SQL source table, alongside a Kafka SQL sink connector that aggregates supplier statistics over 5-second tumbling windows and writes the results to a Kafka sink topic using Confluent Avro.
- Lab 5: Flink DataStream Application for Supplier Statistics
  - In progress
- [Lab 6: Flink SQL for Orders Sink with Parquet](./fh-local-flink-sql-orders-parquet/)
  - This example demonstrates how to ingest Avro-encoded order records from a Kafka topic into a Flink SQL source table, and write them to an object storage sink (MinIO) in Parquet format using the Filesystem connector.
- Lab 7: Flink SQL for Orders Sink in Iceberg
  - In progress
- Lab 8: Flink Table Application for Orders Sink in Iceberg
  - In progress
- Lab 9: Kafka Connect for Orders Sink in Iceberg
  - In progress
- Lab 10: Spark SQL for Orders Sink in Iceberg
  - In progress
- Lab 11: Flink SQL Gateway for Supplier Statistics
  - In progress
- Lab 12: Pinot Analytics for Supplier Statistics
  - In progress

## Support

Any issues? Contact [support](https://factorhouse.io/support/) or view our [docs](https://docs.factorhouse.io/).

## License

This repository is released under the Apache 2.0 License.

Copyright © Factor House.
