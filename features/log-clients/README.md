## Kafka producer and consumer for app log ingestion and processing

## How to start

### Clone project repository

```bash
git clone https://github.com/factorhouse/examples.git
cd examples
```

### Start Kafka environment

We can get our Kafka environment including **Kpow** up and running using [Factor House Local](https://github.com/factorhouse/factorhouse-local). We can use either the Kpow Community or Enterprise edition. **To get started, let's make sure a valid Kpow license is available.** For details on how to request and configure a license, refer to [this section](https://github.com/factorhouse/factorhouse-local?tab=readme-ov-file#update-kpow-and-flex-licenses) of the project _README_.

```bash
## Clone the Factor House Local Repository
git clone https://github.com/factorhouse/factorhouse-local.git

## Download Kafka/Flink Connectors and Spark Iceberg Dependencies
./factorhouse-local/resources/setup-env.sh

## Uncomment the sections to enable the edition and license.
# Edition (choose one):
# unset KPOW_SUFFIX         # Enterprise
# export KPOW_SUFFIX="-ce"  # Community
# License:
# export KPOW_LICENSE=<path-to-license-file>

docker compose -p kpow -f ./factorhouse-local/compose-kpow.yml up -d
```

### Start Kafka Apps

Create a virual environment and install dependent packages.

```bash
python -m venv venv
source venv/bin/activate
# windows
# .\venv\Scripts\activate
pip install -r features/log-clients/requirements.txt
```

Start the producer and consumer.

```bash
# producer
python features/log-clients/producer.py
# consumer
python features/log-clients/consumer.py
```

## Shutdown environment

Stop and remove the Docker containers.

> If you're not already in the project root directory, navigate there first.
> Then, stop and remove the Docker containers by running:

```bash
# Stops the containers and unsets environment variables
docker compose -p kpow -f ./factorhouse-local/compose-kpow.yml down

unset KPOW_SUFFIX KPOW_LICENSE
```
