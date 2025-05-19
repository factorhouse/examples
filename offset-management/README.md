## Kafka apps to showcase consumer offset management

Python Kafka producer and consumer clients in this folder are use to showcase consumer group offset management capabilities of Kpow.

## How to start

### Clone project repository

```bash
git clone https://github.com/factorhouse/examples.git
cd examples
```

### Start Kafka Environment

We can get our Kafka environment including **Kpow** up and running using [Factor House Local](https://github.com/factorhouse/factorhouse-local). We can use either the Kpow Community or Enterprise edition. **To get started, let's make sure a valid Kpow license is available.** For details on how to request and configure a license, refer to [this section](https://github.com/factorhouse/factorhouse-local?tab=readme-ov-file#update-kpow-and-flex-licenses) of the project _README_.

```
git clone https://github.com/factorhouse/factorhouse-local.git
docker compose -f ./factorhouse-local/compose-kpow-community.yml up -d
```

### Start Kafka Apps

Create a virual environment and install dependent packages.

```bash
python -m venv venv
source venv/bin/activate
# windows
# .\venv\Scripts\activate
pip install -r offset-management/requirements.txt
```

Start the producer and consumer.

```bash
# producer
python offset-management/producer.py
# consumer
python offset-management/consumer.py
```
