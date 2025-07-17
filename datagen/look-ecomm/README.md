## theLook eCommerce Data Generator (`data_generator.py`)

Generates a continuous stream of synthetic data based on the **theLook eCommerce dataset**, which represents a fictional online fashion retailer. The data simulates a live production database, making it ideal for demonstrating **Change Data Capture (CDC)** with Debezium and for real-time analytics using Apache Flink or Apache Spark.

### Key Features

- **Real-Time Database Simulation**
  Continuously writes to a SQL database (e.g., PostgreSQL) at a configurable rate (`--avg-qps`), simulating a real workload.

- **Rich eCommerce Model**
  Automatically creates and populates realistic tables such as `users`, `orders`, `products`, `inventory_items`, and user `events` (e.g., page views, cart activity).

- **CDC-Optimized Output**
  Structured to enable Debezium to capture `INSERT`, `UPDATE`, and `DELETE` events and stream them into Kafka topics.

- **Ghost Event Simulation**
  Simulates user behavior without purchases (e.g., browsing or abandoned carts). The `--ghost-ratio` parameter controls this behavior.

- **Kafka Topic Management**
  Automatically creates required Kafka topics (unless disabled via `--no-create-topic`).

### Setup & Usage

1. Create and activate a virtual environment:

   ```bash
   python -m venv venv
   source venv/bin/activate
   ```

2. Install dependencies:

   ```bash
   pip install -r datagen/look-ecommerce/requirements.txt
   ```

3. Run the generator:

   ```bash
   python datagen/look-ecomm/data_generator.py --help
   ```

#### Available CLI Options

```text
usage: data_generator.py [-h]
                         [--avg-qps AVG_QPS]
                         [--if_exists {fail,replace,append}]
                         [--max_iter MAX_ITER]
                         [--ghost_ratio GHOST_RATIO]
                         [--host HOST]
                         [--user USER]
                         [--password PASSWORD]
                         [--db DB]
                         [--schema SCHEMA]
                         [--bootstrap-servers BOOTSTRAP_SERVERS]
                         [--topic-prefix TOPIC_PREFIX]
                         [--create-topic | --no-create-topic]

options:
  -h, --help            Show help message and exit.
  --avg-qps             Average events per second.
  --if_exists           Behavior if table exists: 'fail', 'replace', or 'append'.
  --max_iter            Max number of iterations. Use -1 for infinite.
  --ghost_ratio, -g     Ratio of "ghost" (anonymous) user events.
  --host                Database host.
  --user                Database user.
  --password            Database password.
  --db                  Database name.
  --schema              Database schema.
  --bootstrap-servers   Kafka bootstrap servers.
  --topic-prefix        Prefix for Kafka topics.
  --create-topic        Enable automatic topic creation.
  --no-create-topic     Disable automatic topic creation.
```
