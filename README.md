![factorhouse](./images/factorhouse.jfif)

# Product Feature

## Kpow

- Manage Kafka Consumer Offsets with Kpow
  - [Manage Kafka Consumer Offsets with Kpow](https://factorhouse.io/blog/how-to/manage-kafka-consumer-offsets-with-kpow/) (blog)
  - [Source](./offset-management/) - Python Kafka producer and consumer clients that are used to showcase consumer group offset management capabilities of Kpow.
- Kafka Avro Clients with Schema Registry
  - [Source](./fh-local-kafka-avro-clients/) - Python Kafka producer and consumer that are are used to demonstrate Avro (de)serialization and schema registry integration.
- Kafka Connect via Kpow UI and API
  - [Source](./fh-local-kafka-connect/) - This example demonstrates how to interact with Kafka Connect through both the Kpow UI and its Connect API. We’ll walk through end-to-end examples for creating, monitoring, and managing source and sink connectors using both interfaces.
- Kafka Streams with Kpow Streams Agent
  - [Source](./fh-local-kafka-streams/) - The Kafka Streams application reads records from the `orders` topic, calculates supplier statistics, and sends them to a new Kafka topic (`orders-supplier-stats`). It includes integration with the [Kpow Streams Agent](https://github.com/factorhouse/kpow-streams-agent) for live topology visualization.

## Flex

- 🚧 More to come

# Integration

- [Setting Up Kpow with Confluent Cloud](https://factorhouse.io/blog/how-to/set-up-kpow-with-confluent-cloud/)
  - A step-by-step guide to configuring Kpow with Confluent Cloud resources including Kafka clusters, Schema Registry, Kafka Connect, and kSQLDB.
- [Set Up Kpow with Amazon Managed Streaming for Apache Kafka](https://factorhouse.io/blog/how-to/set-up-kpow-with-aws/)
  - A comprehensive, step by step guide to provisioning AWS MSK infrastructure, configuring authentication with the OAUTHBEARER mechanism using AWS IAM, setting up a client EC2 instance within the same VPC, and deploying Kpow.
- [Set Up Kpow with Google Cloud Managed Service for Apache Kafka](https://factorhouse.io/blog/how-to/set-up-kpow-with-gcp/)
  - A practical, step-by-step guide on setting up a Google Cloud Managed Service for Apache Kafka cluster and connecting it from Kpow using the OAUTHBEARER mechanism.

# Local Development

Looking for local development environment? Check out the [factorhouse-local](https://github.com/factorhouse/factorhouse-local) project.

It offers a comprehensive Docker Compose-based environment for running [Factor House products](https://factorhouse.io/) - Kpow and Flex - alongside essential supporting infrastructure. It includes pre-configured stacks for various scenarios, such as Kafka development and monitoring with Kpow, real-time analytics with Flex and Apache Pinot, and more, enabling users to explore and test these tools in a local setup.

## Support

Any issues? Contact [support](https://factorhouse.io/support/) or view our [docs](https://docs.factorhouse.io/).

## License

This repository is released under the Apache 2.0 License.

Copyright © Factor House.
