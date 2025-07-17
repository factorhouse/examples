![factorhouse](./images/factorhouse.jfif)

## Product Feature

<details>
  <summary><a href="./features/offset-management/">Manage Kafka Consumer Offsets with Kpow</a></summary>
  <ul>
    <li>
      Python Kafka producer and consumer clients that are used to showcase consumer group offset management capabilities of Kpow.
    </li>
    <li>
      Also see the related <a href="https://factorhouse.io/blog/how-to/manage-kafka-consumer-offsets-with-kpow/">blog post</a>.
    </li>
  </ul>
</details>

<details>
  <summary><a href="https://factorhouse.io/blog/how-to/integrate-confluent-compatible-registries-kpow/">Integrate Confluent-compatible Schema Registries with Kpow</a></summary>
  <ul>
    <li>
      In modern data architectures built on Apache Kafka, a Schema Registry is an essential component for enforcing data contracts and supporting strong data governance. While the Confluent Schema Registry set the original standard, the ecosystem has expanded to include powerful Confluent-compatible alternatives such as Red Hat’s Apicurio Registry and Aiven’s Karapace.
    </li>
    <li>
      Whether driven by a gradual migration, the need to support autonomous teams, or simply technology evaluation, many organizations find themselves running multiple schema registries in parallel. This inevitably leads to operational complexity and a fragmented view of their data governance.
    </li>
    <li>
      This guide demonstrates how Kpow directly solves this challenge. We will integrate these popular schema registries into a single Kafka environment and show how to manage them all seamlessly through Kpow's single, unified interface.
    </li>
  </ul>
</details>

## Integration

<details>
  <summary><a href="https://factorhouse.io/blog/how-to/set-up-kpow-with-confluent-cloud/">Setting Up Kpow with Confluent Cloud</a></summary>
  <ul>
    <li>
      A step-by-step guide to configuring Kpow with Confluent Cloud resources including Kafka clusters, Schema Registry, Kafka Connect, and kSQLDB.
    </li>
  </ul>
</details>

<details>
  <summary><a href="https://factorhouse.io/blog/how-to/set-up-kpow-with-aws/">Set Up Kpow with Amazon Managed Streaming for Apache Kafka</a></summary>
  <ul>
    <li>
      A comprehensive, step-by-step guide to provisioning AWS MSK infrastructure, configuring authentication with the OAUTHBEARER mechanism using AWS IAM, setting up a client EC2 instance within the same VPC, and deploying Kpow.
    </li>
  </ul>
</details>

<details>
  <summary><a href="https://factorhouse.io/blog/how-to/set-up-kpow-with-gcp/">Set Up Kpow with Google Cloud Managed Service for Apache Kafka</a></summary>
  <ul>
    <li>
      A practical, step-by-step guide on setting up a Google Cloud Managed Service for Apache Kafka cluster and connecting it from Kpow using the OAUTHBEARER mechanism.
    </li>
  </ul>
</details>

<details>
  <summary><a href="https://factorhouse.io/blog/how-to/integrate-kpow-with-google-schema-registry/">Integrate Kpow with Google Managed Schema Registry</a></summary>
  <ul>
    <li>
      Google Cloud has enhanced its platform with the launch of a managed <a href="https://cloud.google.com/managed-service-for-apache-kafka/docs/schema-registry/schema-registry-overview">Schema Registry for Apache Kafka</a>, a critical service for ensuring data quality and schema evolution in streaming architectures. Kpow 94.3 expands its support for Google Managed Service for Apache Kafka by integrating the managed schema registry. This allows users to manage Kafka clusters, topics, consumer groups, and schemas from a single interface.
    </li>
    <li>
      Building on our <a href="https://factorhouse.io/blog/how-to/set-up-kpow-with-gcp/">earlier setup guide</a>, this post details how to configure the new schema registry integration and demonstrates how to leverage the Kpow UI for working effectively with Avro schemas.
    </li>
  </ul>
</details>

<details>
  <summary><a href="https://factorhouse.io/blog/how-to/integrate-kpow-with-bufstream/">Integrate Kpow with Bufstream</a></summary>
  <ul>
    <li>
      Kpow supports a wide range of Apache Kafka and Kafka API–compatible platforms, providing robust tools to manage, monitor, and secure data streaming workloads. In this guide, we'll walkthrough how to integrate Bufstream — a cloud-native, Kafka-compatible streaming solution — with Kpow, enabling seamless use of Bufstream's advanced schema management alongside Kpow's comprehensive Kafka management capabilities for an optimized streaming experience.
    </li>
  </ul>
</details>

<details>
  <summary><a href="https://factorhouse.io/blog/how-to/integrate-kpow-with-redpanda/">Integrate Kpow with the Redpanda Streaming Platform</a></summary>
  <ul>
    <li>
      Redpanda offers a simple, powerful, and Kafka®-compatible streaming data platform. Kpow provides a rich, developer-focused UI to manage and monitor it. Together, they form a robust stack for building and operating real-time data pipelines.
    </li>
    <li>
      This guide will walk you through the process of setting up Kpow with a local Redpanda cluster using Docker. We will cover launching the environment, using Kpow to create and manage an Avro schema in Redpanda's built-in registry, producing schema-governed data to a topic, and finally, inspecting that data in a human-readable format.
    </li>
  </ul>
</details>

## Data Generator

<details>
  <summary><a href="./datagen/look-ecomm/">theLook eCommerce Data Generator</a></summary>
  <ul>
    <li>
      Generates a continuous stream of synthetic data based on the <strong>theLook eCommerce dataset</strong>, which represents a fictional online fashion retailer. The data simulates a live production database, making it ideal for demonstrating <strong>Change Data Capture (CDC)</strong> with Debezium and for real-time analytics using Apache Flink or Apache Spark.
    </li>
  </ul>
</details>

<details>
  <summary><a href="./datagen/mobile-game/">Mobile Game User Score</a></summary>
  <ul>
    <li>
      A Python port of Apache Beam's <a href="https://beam.apache.org/get-started/mobile-gaming-example/"><strong>Mobile Gaming Example</strong></a> data generator. This script simulates game events such as user scores, designed to support real-time analytics pipelines for leaderboards, scoring engines, and fraud detection.
    </li>
  </ul>
</details>

## Factor House Local

Looking for local development environment? Check out [Factor House Local](https://github.com/factorhouse/factorhouse-local).

It provides a collection of pre-configured Docker Compose environments that demonstrate modern data platform architectures. Each setup is purpose-built around a specific use case and incorporates widely adopted technologies such as Kafka, Flink, Spark, Iceberg, and Pinot. These environments are further enhanced by enterprise-grade tools from Factor House: [**Kpow**](https://factorhouse.io/kpow), for Kafka management and control, and [**Flex**](https://factorhouse.io/flex/), for seamless integration with Flink.

### Factor House Local Labs

<p align="center">
  <img width="600" height="500" src="./images/fh-local-labs.png">
</p>

The **Factor House Local labs** provide a fast and practical entry point for developers building real-time data pipelines using **Kafka**, **Flink**, **Spark**, **Iceberg**, and **Pinot**. These hands-on labs highlight key capabilities such as Avro serialization with Schema Registry, stream processing with Kafka Streams and Flink (via SQL and DataStream APIs), connector deployment with Kpow, modern lakehouse integrations using Iceberg, and real-time analytics with Pinot. Each lab is designed to be modular, hands-on, and production-inspired - making it easy to learn, prototype, and extend.

Visit [**the Labs**](./fh-local-labs/) to explore detailed walkthroughs, configuration examples, and practical exercises that will help you get up and running quickly with each tool in a real-world context.

## Support

Any issues? Contact [support](https://factorhouse.io/support/) or view our [docs](https://docs.factorhouse.io/).

## License

This repository is released under the Apache 2.0 License.

Copyright © Factor House.
