# CDC with Debezium on Real-Time theLook eCommerce Data

This project unlocks the power of the popular [theLook eCommerce dataset](https://console.cloud.google.com/marketplace/product/bigquery-public-data/thelook-ecommerce) for modern event-driven applications. It uses a re-engineered [real-time data generator](../../datagen/thelook-ecomm/) that transforms the original static dataset into a continuous stream of simulated user activity, writing directly to a PostgreSQL database.

This stream becomes an ideal source for building and testing Change Data Capture (CDC) pipelines using Debezium and Kafka—enabling developers and analysts to work with a familiar, realistic schema in a real-time context.

![](../../datagen/thelook-ecomm/images/thelook-datagen.gif)

As a practical demonstration, this project includes deployment of a Debezium connector to stream database changes into Kafka topics.

## Introduction to CDC with Debezium

Change Data Capture (CDC) is a set of design patterns used to track changes in a database so that other systems can react to those changes. [Debezium](https://debezium.io/) is an open-source, distributed platform that turns your existing databases into event streams. This allows applications to respond to row-level changes in the database in real-time. Debezium provides a library of connectors for various databases that can monitor and record changes, publishing them to a streaming service like Apache Kafka.

This project utilizes the Debezium PostgreSQL connector to capture changes from a PostgreSQL database. Here’s a breakdown of the key components and configurations:

- **Debezium PostgreSQL Connector**: This connector is configured to monitor the `demo` schema within the PostgreSQL database. It reads the database's write-ahead log (WAL) to capture all `INSERT`, `UPDATE`, and `DELETE` operations committed to the tables within that schema. To enable this, the PostgreSQL server is started with the `wal_level` set to `logical` in the `compose-flex.yml` file, which allows the WAL to contain the information needed for logical decoding.

- **Logical Decoding Plugin (`pgoutput`)**: The Debezium PostgreSQL connector uses PostgreSQL's built-in `pgoutput` logical decoding plugin. This plugin is part of PostgreSQL's core and provides a way to stream a sequence of changes from the WAL.

- **Database Initialization**: Before Debezium can start capturing changes, the database is prepared with a specific schema and publication:

  - A new schema named `demo` is created to house the eCommerce dataset.
  - A `PUBLICATION` named `cdc_pub` is created for all tables within the `demo` schema. This publication acts as a logical grouping of tables whose changes should be made available to subscribers, in this case, the Debezium connector.

- **Push-Based Model**: Debezium operates on a push-based model. The PostgreSQL database actively sends changes to the Debezium connector as they occur in the transaction log. The connector then processes these changes and pushes them as events to Kafka topics. This approach ensures low-latency data streaming.

### CDC setup in [Factor House Local](https://github.com/factorhouse/factorhouse-local)

**Database Service Configuration**

```yaml
# factorhouse-local > compose-flex.yml
services:
  ...
  postgres:
    image: postgres:17
    container_name: postgres
    command: ["postgres", "-c", "wal_level=logical"]
    ports:
      - 5432:5432
    networks:
      - factorhouse
    volumes:
      - ./resources/postgres:/docker-entrypoint-initdb.d
    environment:
      POSTGRES_DB: fh_dev
      POSTGRES_USER: db_user
      POSTGRES_PASSWORD: db_password
      TZ: UTC
  ...
```

**Database Initialization Script**

```sql
--// factorhouse-local > resources/postgres/01-init-databases.sql
-- Create schema
CREATE SCHEMA IF NOT EXISTS demo;

-- Grant privileges on schema to the application user
GRANT ALL ON SCHEMA demo TO db_user;

-- Set search_path at the DB level
ALTER DATABASE fh_dev SET search_path TO demo, public;

-- Set search_path for current session too
SET search_path TO demo, public;

-- Create CDC publication for Debezium
CREATE PUBLICATION cdc_pub FOR TABLES IN SCHEMA demo;
```

## Set Up the Environment

### Clone the Project

```bash
git clone https://github.com/factorhouse/examples.git
cd examples
```

### Start Kafka and Flink

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
  && docker compose -p flex -f ./factorhouse-local/compose-flex.yml up -d
```

## Launch the theLook eCommerce Data Generator

Start the containerized data generator to simulate real-time activity.

```bash
docker compose -f projects/thelook-ecomm-cdc/docker-compose.yml up -d
```

This will populate the following tables under the `demo` schema in the `fh_dev` PostgreSQL database:

- `users`
- `products`
- `dist_centers`
- `orders`
- `order_items`
- `events`
- `heartbeat` (used internally by Debezium)

![](./images/thelook-db.gif)

## Deploy the Debezium Connector (`thelook-ecomm`)

The [**`debezium.json`**](./debezium.json) configuration defines a CDC pipeline using Debezium to stream changes from PostgreSQL into Kafka, capturing activity in the `demo` schema and serializing records in Avro format.

### Key Features

- **Connector**: PostgreSQL CDC using Debezium with the `pgoutput` plugin
- **Target Database**: Connects to `fh_dev` on `postgres:5432`
- **Monitored Tables**: All tables under the `demo` schema
- **Snapshot Mode**: `"initial"` - performs a full snapshot on first run, then streams all `INSERT`, `UPDATE`, and `DELETE` operations

### Serialization and Schema Management

- **Format**: Avro (`AvroConverter`)
- **Schema Registry**: Integrated with Confluent Schema Registry at `http://schema:8081`

### Kafka Topic Management

- **Naming Convention**: Topics follow the pattern `ecomm.schema_name.table_name`
- **Auto-Creation**: Enabled (`"topic.creation.enable": "true"`)
- **Cleanup Policy**: Set to `compact`, retaining the latest value for each key
- **Defaults**: 3 partitions, replication factor of 1 (suitable for development only)

> ⚠️ In production, increase replication factor to ensure fault tolerance.

### Heartbeat Table

The connector uses the `demo.heartbeat` table to emit regular heartbeat events:

- Triggers an `INSERT` or `UPDATE` every 10 seconds (`heartbeat.interval.ms`)
- Keeps the connector’s offset up to date and allows safe WAL file cleanup

### Deploy via Kpow

Visit [http://localhost:3000](http://localhost:3000) to deploy the Debezium connector using Kpow’s UI. Once deployed, the following Kafka topics will be created and populated:

- `ecomm.demo.users`
- `ecomm.demo.products`
- `ecomm.demo.dist_centers`
- `ecomm.demo.orders`
- `ecomm.demo.order_items`
- `ecomm.demo.events`

![](./images/thelook-debezium.gif)

## Conclusion

This project offers a practical, end-to-end environment for working with Change Data Capture using real-time eCommerce data. With a live stream of events feeding into Kafka, you can now:

- 🔍 Build **real-time analytics** with tools like Flink or Apache Pinot
- 🧊 Ingest data into a **lakehouse** with Spark, Flink, or Kafka Connect
- ⚙️ Develop **event-driven microservices** that respond to user or order changes

By combining a realistic dataset with open-source tooling, this project makes it easy to experiment, prototype, and build production-ready CDC pipelines.

## Shut Down

When you're done, shut down all containers and unset any environment variables:

```bash
# Stop the data generator
docker compose -f projects/thelook-ecomm-cdc/docker-compose.yml down

# Stop Factor House Local containers
docker compose -p flex -f ./factorhouse-local/compose-flex.yml down \
  && docker compose -p kpow -f ./factorhouse-local/compose-kpow.yml down

# Clear environment variables
unset KPOW_SUFFIX FLEX_SUFFIX KPOW_LICENSE FLEX_LICENSE
```
