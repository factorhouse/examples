# Data Lineage Labs

## A Practical Guide to Data Lineage on Kafka Connect with OpenLineage

This guide provides a complete, end-to-end solution for capturing real-time data lineage from **Kafka Connect**. By the end of this tutorial, you will have a fully functional environment that tracks data from a source connector, through Kafka, and into data sinks like S3 and Apache Iceberg, with the entire graph visualized in [Marquez](https://marquezproject.github.io/marquez/).

The core of this solution is the **`OpenLineageLifecycleSmt`**, a custom Single Message Transform (SMT) that enables automated lineage without modifying your data records. We will walk through its setup, configuration, and limitations to provide a comprehensive understanding of how to achieve lineage for your Kafka Connect pipelines.

![](./images/connector-lineage.gif)

## End-to-End Data Lineage from Kafka to Flink and Spark

This guide provides a complete, end-to-end tutorial for capturing real-time and batch data lineage across a modern data stack: **Kafka, Flink, Spark, and Iceberg**. By the end of this lab, you will have a fully functional environment that tracks the flow of data from a Kafka topic, through a real-time Flink ingestion job into an Iceberg table, and finally into a batch Spark job that generates analytical summaries. The entire lineage graph, including column-level details, will be visualized in [Marquez](https://marquezproject.github.io/marquez/).

The core of this solution is the careful configuration of OpenLineage integrations for each component. The lab begins with deploying two Kafka connectors where lineage is captured using the `OpenLineageLifecycleSmt` as discussed in the [previous lab](./lab1_kafka-connect.md). From there, it explores two distinct Flink integration patterns: a simple listener-based approach and a more robust manual orchestration method. Finally, Spark is configured to consume the Flink job's output, completing a practical, end-to-end blueprint for establishing reliable data lineage in a production-style environment.
