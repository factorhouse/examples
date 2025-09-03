# End-to-end data lineage

## Set up the environment

### Clone the project

```bash
git clone https://github.com/factorhouse/examples.git
cd examples
```

### Start all environments

This project uses [Factor House Local](https://github.com/factorhouse/factorhouse-local) to spin up the Kafka and Flink environments.

Before starting, make sure you have valid licenses for Kpow and Flex. See the [license setup guide](https://github.com/factorhouse/factorhouse-local?tab=readme-ov-file#update-kpow-and-flex-licenses) for instructions.

**Note:**

- One of the requirements for integrating OpenLineage with Flink 1.x is that each Flink job must be deployed to an **application cluster**, meaning each job runs on its own dedicated cluster. Since we will be deploying two Flink jobs, we need to integrate **two Flink clusters** on Flex. The **community edition** does not support multiple Flink clusters simultaneously, so we use the **enterprise edition** here. If you are using the community edition, you can deploy the jobs **one at a time**.
- Additionally, Factor House Local deploys a **Flink session cluster**, which **cannot** be used for deploying the Flink jobs of this lab. When setting up the initial infrastructure, a **stripped-down version of the analytics environment** is used, which excludes both the Flink cluster and Flex. To avoid confusion, all new Docker Compose environments for this lab will be stored in this project folder: `./projects/data-lineage-labs`. These include:
  - `compose-stripped.yml`
  - `compose-flink-1.yml`
  - `compose-flink-2.yml`
  - `compose-flex.yml`

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
  && docker compose -p obsv -f ./factorhouse-local/compose-obsv.yml up -d \
  && docker compose -p stripped -f ./projects/data-lineage-labs/compose-stripped.yml up -d
```

## Kafka Connect

As discussed in [A Practical Guide to Data Lineage on Kafka Connect with OpenLineage](./01_kafka-connect.md), lineage tracking is enabled by integrating with OpenLineage using the **`OpenLineageLifecycleSmt`** Single Message Transform (SMT). The SMT is included in _Factor House Local_.

We will deploy the following two Kafka connectors:

- [**Source Connector**](./connectors/orders-source.json): Ingests mock order data into the `orders` topic using the _Amazon MSK Data Generator_.
- [**S3 Sink Connector**](./connectors/orders-s3-sink.json): Reads records from the `orders` topic and writes them to an object storage bucket (MinIO).

Kpow provides a **user-friendly UI and API** for deploying Kafka connectors. For more details, see [this page](../../fh-local-labs/lab-02/README.md).

## Flink jobs

This project includes two Flink applications, each demonstrating a different method for integrating with OpenLineage to capture data lineage.

1.  **Real-time supplier stats:** A DataStream API job that shows the out-of-the-box (but limited) `OpenLineageFlinkJobListener` integration.
2.  **Kafka to Iceberg ingestion:** A Table API job that demonstrates a robust, manual orchestration pattern using the core OpenLineage Java client for complete lifecycle tracking.

**Requirements**

To build and run these applications, you will need the following installed on your local machine:

- **Docker and Docker Compose:** Used to run the Flink clusters and the entire data platform provided by Factor House Local.
- **JDK 11:** Both applications are built to target Java 11.
- **Gradle:** The projects are built with Gradle, but the included Gradle Wrapper (`./gradlew`) will automatically download the correct version, so no manual installation is required.

### Real-time supplier stats (DataStream API)

This application uses Flink's DataStream API to perform real-time aggregations on a Kafka stream. Specifically, it consumes a stream of `orders` events, keys the data by supplier, and calculates the total price and count of orders within discrete, tumbling time windows. It leverages event-time processing based on timestamps within the data to ensure accurate handling of out-of-order events. The resulting aggregated statistics are written to a `supplier-stats` Kafka topic, demonstrating a common real-time analytics pattern used for live monitoring and dashboarding.

#### OpenLineage integration

- **Integration method:**
  - **Programmatic listener registration:** This job uses the **`OpenLineageFlinkJobListener`** (`v1.15.0`) provided by the `openlineage-flink` library. The listener is created using its `.builder()` and registered directly with the `StreamExecutionEnvironment` in the application code.
  - **Automatic discovery:** The listener's internal "visitors" are responsible for automatically inspecting the Flink job graph to discover the input (`KafkaSource`) and output (`FlinkKafkaProducer`) datasets.
- **Key workarounds and limitations:**
  - **Older library version required:** It must use `openlineage-flink:1.15.0`. Newer versions have a faulty version-check that crashes the application when used with modern Kafka connectors.
  - **Deprecated Flink sink required:** It must use the deprecated `FlinkKafkaProducer` instead of the modern `KafkaSink`. This is a necessary workaround because the listener's visitor in this version cannot correctly inspect the modern sink to discover the output dataset.
  - **CRITICAL LIMITATION - final status is not reported:** When the job is canceled via the Flink UI or the cluster is destroyed, the final `ABORT` status is **not** sent to Marquez. The job remains stuck in the `RUNNING` state. This is due to a fundamental lifecycle mismatch in Flink's Application Mode, where the process hosting the listener is terminated too abruptly for it to send the final event.

#### Build the application

Navigate to the project directory and use the Gradle wrapper to build the self-contained fat JAR.

```bash
# Move to the application folder
cd projects/data-lineage-labs/flink-supplier-stats

# Build the app
./gradlew clean build
```

This command compiles the code and packages it with all necessary dependencies into `projects/data-lineage-labs/build/libs/flink-supplier-stats-1.0.jar`.

#### Deploy the Flink job

This job is deployed to an Application Cluster. You can add its services to the `compose-flink-1.yml` file.

<details>

<summary><b>Flink cluster configuration</b></summary>

#### compose-flink-1.yml

```yaml
x-common-flink-config: &flink_image_pull_policy_config
  image: fh-flink-1.20.1
  build:
    context: ../../factorhouse-local/resources/flink/
    dockerfile: Dockerfile
  pull_policy: never

x-common-environment: &flink_common_env_vars
  ...
  ## OpenLineage configuration
  OPENLINEAGE_NAMESPACE: fh-local
  OPENLINEAGE_JOBNAME: supplier-stats

x-common-flink-volumes: &flink_common_volumes
  ...
  ## Dedicated flink configuration
  - ./resources/flink-conf-1.yaml:/opt/flink/conf/flink-conf.yaml:ro
  ...
  ## Add application JAR
  - ./resources/openlineage.yml:/opt/flink/.openlineage/openlineage.yml:ro
  - ./flink-supplier-stats/build/libs/flink-supplier-stats-1.0.jar:/opt/flink/usrlib/flink-supplier-stats-1.0.jar

services:
  jobmanager-1:
    <<: *flink_image_pull_policy_config
    container_name: jobmanager-1
    ## Run as a standalone job
    command: >
      standalone-job
      --job-classname io.factorhouse.demo.MainKt
      --jarfile /opt/flink/usrlib/flink-supplier-stats-1.0.jar
    ports:
      - "18081:8081"
    networks:
      - factorhouse
    environment:
      <<: *flink_common_env_vars
    volumes: *flink_common_volumes
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8081/config"]
      interval: 5s
      timeout: 5s
      retries: 5

  taskmanager-11:
    <<: *flink_image_pull_policy_config
    container_name: taskmanager-11
    command: taskmanager
    networks:
      - factorhouse
    environment:
      <<: *flink_common_env_vars
    volumes: *flink_common_volumes
    depends_on:
      jobmanager-1:
        condition: service_healthy
...
```

#### OpenLineage transport config

```yaml
transport:
  type: http
  url: http://marquez-api:5000
```

</details>

<br/>

```bash
# Move to the main examples folder
cd ../../..

# Deploy the Flink job
docker compose -p flink-1 -f ./projects/data-lineage-labs/compose-flink-1.yml up -d
```

### Kafka to Iceberg ingestion (Table API)

This application uses Flink's Table API to ingest data from a Kafka topic into an Apache Iceberg table, providing a robust example of a streaming ETL pipeline. The job defines a source table that connects to the `orders` Kafka topic, reading Avro records via the Schema Registry. It then performs a lightweight transformation—casting the price field to a `DECIMAL` type—before inserting the data directly into an Iceberg table named `default.orders`. The application also demonstrates how to programmatically create and register an Iceberg catalog pointing to a Hive Metastore, making it a foundational pattern for building a modern data lakehouse.

#### OpenLineage Integration

This job demonstrates a robust, manual integration pattern that provides complete and accurate lifecycle tracking for Flink Table API jobs.

- **Integration Method:**

  - **Manual rrchestration:** The integration **bypasses the `OpenLineageFlinkJobListener` entirely** and uses the core `openlineage-java` client (`v1.37.0`) directly. The application's main method acts as an orchestrator, controlling _when_ to emit lineage events.
  - **Separation of concerns:** The detailed logic is delegated to dedicated helper class (`Integration.kt`), which handles the construction and emission of the OpenLineage events as well as is responsible for dynamic metadata discovery.
  - **Explicit event emission:** The application uses a `try...catch...finally` block to guarantee the emission of the correct lifecycle events: a minimal `START` event before submission, a rich `RUNNING` event after submission, and a final `ABORT` or `FAIL` event based on the job's terminal state.

- **Advantages & trade-offs:**
  - **Increased code complexity:** This approach requires more explicit code in the application to manage the lineage lifecycle, making it less of a "plug-and-play" solution than a listener.
  - **Complete lifecycle tracking:** This manual method provides a fully robust solution. It correctly discovers all datasets and, most importantly, **guarantees that the final `ABORT` or `FAIL` status is sent** when the job is canceled or fails, providing a complete and accurate record of the job's lifecycle.

#### Build the application

Navigate to the project directory and use the Gradle wrapper to build the self-contained fat JAR.

```bash
# Move to the project folder
cd projects/data-lineage-labs/flink-iceberg-ingestion

# Build the app
./gradlew clean build
```

This creates the application JAR at `projects/data-lineage-labs/build/libs/flink-iceberg-ingestion-1.0.jar`.

#### Deploy the Flink job

This job is also deployed to an Application Cluster. You can add its services to the `compose-flink-2.yml` file.

<details>

<summary><b>Flink cluster configuration</b></summary>

#### compose-flink-2.yml

```yaml
x-common-flink-config: &flink_image_pull_policy_config
  image: fh-flink-1.20.1
  build:
    context: ../../factorhouse-local/resources/flink/
    dockerfile: Dockerfile
  pull_policy: never

x-common-environment: &flink_common_env_vars
  ...
  ## OpenLineage configuration
  OPENLINEAGE_NAMESPACE: fh-local
  OPENLINEAGE_JOBNAME: iceberg-ingestion
  ## Dependeny JAR
  CUSTOM_JARS_DIRS: "/tmp/hadoop;/tmp/hive;/tmp/iceberg;/tmp/parquet"

x-common-flink-volumes: &flink_common_volumes
  ...
  ## Dedicated flink configuration
  - ./resources/flink-conf-2.yaml:/opt/flink/conf/flink-conf.yaml:ro
  ...
  ## Add application JAR
  - ./flink-iceberg-ingestion/build/libs/flink-iceberg-ingestion-1.0.jar:/opt/flink/usrlib/flink-iceberg-ingestion-1.0.jar

services:
  jobmanager-2:
    <<: *flink_image_pull_policy_config
    container_name: jobmanager-2
    ## Run as a standalone job
    command: >
      standalone-job
      --job-classname io.factorhouse.demo.MainKt
      --jarfile /opt/flink/usrlib/flink-iceberg-ingestion-1.0.jar
    ports:
      - "18082:8081"
    networks:
      - factorhouse
    environment:
      <<: *flink_common_env_vars
    volumes: *flink_common_volumes
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8081/config"]
      interval: 5s
      timeout: 5s
      retries: 5

  taskmanager-21:
    <<: *flink_image_pull_policy_config
    container_name: taskmanager-21
    command: taskmanager
    networks:
      - factorhouse
    environment:
      <<: *flink_common_env_vars
    volumes: *flink_common_volumes
    depends_on:
      jobmanager-2:
        condition: service_healthy
...
```

</details>

<br/>

```bash
# Move to the main examples folder
cd ../../..

# Deploy the Flink job
docker compose -p flink-1 -f ./projects/data-lineage-labs/compose-flink-2.yml up -d
```

## Spark job

## Viewing lineage on Marquez

## Shut down
