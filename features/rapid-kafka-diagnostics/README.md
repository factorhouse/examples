## Rapid Kafka Diagnostics Demo

This demo simulates a **"Silent Stall"** production incident. In this scenario, a Kafka producer injects a "poison pill" (malformed) message into a specific topic partition.

The environment runs three consumer instances. When one of the instances consumes the malicious message, it enters a "Zombie" state—it continuously crashes and retries the same message logic while successfully sending heartbeats to the Kafka cluster. This mimics a difficult-to-diagnose issue where infrastructure looks healthy, but data processing has halted on a specific partition.

### Getting Started

We use [Factor House Local](https://github.com/factorhouse/factorhouse-local) to quickly spin up a Kafka environment that includes **Kpow**.

**Note:** This demo uses the Community Edition (CE) of Kpow. **Ensure you have a valid license available.** For details on how to request and configure a license, refer to [this section](https://github.com/factorhouse/factorhouse-local?tab=readme-ov-file#update-kpow-and-flex-licenses) of the project README.

#### Setup Infrastructure

First, clone the repository and start the core Kafka and Kpow infrastructure.

```bash
## Clone the Factor House Local Repository
git clone https://github.com/factorhouse/factorhouse-local.git

## (Optional) Download Kafka/Flink Connectors and Spark Iceberg Dependencies
./factorhouse-local/resources/setup-env.sh

## Export your license details
export KPOW_SUFFIX="-ce"
export KPOW_LICENSE_FILE=<path-to-your-license.yaml>

## Start Kafka and Kpow
docker compose -p kpow -f ./factorhouse-local/compose-kpow.yml up -d
```

> **Access Kpow:** Once running, access Kpow at [http://localhost:3000](http://localhost:3000).

### Launch the Demo

We will launch a producer to generate traffic and a consumer group scaled to 3 instances to process it.

#### Start the Producer

The producer will send valid orders, followed by a single "poison pill" message to **Partition 2**.

```bash
docker compose -p producer -f ./features/rapid-kafka-diagnostics/compose-producer.yml up -d
```

#### Start the Consumers

Scale the consumer application to 3 instances to ensure all partitions are covered.

```bash
docker compose -p consumer -f ./features/rapid-kafka-diagnostics/compose-consumer.yml up -d --scale consumer=6
```

### Diagnose the Issue

1. Open Kpow.
2. Observe that the **Consumer Group** state is `Stable` (Green).
3. Drill down into the Consumer Group details. You will see that **Partition 2** has a growing lag, while Partitions 0 and 1 are processing normally.
4. Use **Data Inspect** to view the message at the head of the lag to identify the schema mismatch.

### Shutdown Environment

To clean up, stop and remove all Docker containers and unset the environment variables.

```bash
# Stop Consumers, Producers, and Infrastructure
docker compose -p consumer -f ./features/rapid-kafka-diagnostics/compose-consumer.yml down \
  && docker compose -p producer -f ./features/rapid-kafka-diagnostics/compose-producer.yml down \
  && docker compose -p kpow -f ./factorhouse-local/compose-kpow.yml down

# Clean up variables
unset KPOW_SUFFIX KPOW_LICENSE_FILE
```
