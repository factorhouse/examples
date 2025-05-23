## Kafka Clients for Orders with Avro and Schema Registry

Python Kafka producer and consumer clients in this folder are use to demonstrate Avro (de)serialization and schema registry integration.

## How to start

### Clone project repository

```bash
git clone https://github.com/factorhouse/examples.git
cd examples
```

### Start Kafka environment

We can get our Kafka environment including **Kpow** up and running using [Factor House Local](https://github.com/factorhouse/factorhouse-local). We can use either the Kpow Community or Enterprise edition. **To get started, let's make sure a valid Kpow license is available.** For details on how to request and configure a license, refer to [this section](https://github.com/factorhouse/factorhouse-local?tab=readme-ov-file#update-kpow-and-flex-licenses) of the project _README_.

```bash
git clone https://github.com/factorhouse/factorhouse-local.git
docker compose -f ./factorhouse-local/compose-kpow-community.yml up -d
```

### Start Kafka applications

Create a virual environment and install dependent packages.

```bash
python -m venv venv
source venv/bin/activate
# windows
# .\venv\Scripts\activate
pip install -r fh-local-kafka-clients-orders/requirements.txt
```

Start the producer.

- `MODEL_VERSION` can be either 1 or 2.
- `MAX_RECORDS` controls the maximum number of records to produce. Or don't set for producing messages indefinitely.

```bash
MODEL_VERSION=<num> MAX_RECORDS=<num> python fh-local-kafka-clients-orders/producer.py
```

Start the consumer.

- `MODEL_VERSION` can be (unset), 1 or 2.

```bash
# deserialize generic records
python fh-local-kafka-clients-orders/consumer.py

# deserialize specific records
MODEL_VERSION=<num> python fh-local-kafka-clients-orders/consumer.py
```

### Shutdown environment

Stop and remove the Docker containers.

```bash
docker compose -f ./factorhouse-local/compose-kpow-community.yml down
```
