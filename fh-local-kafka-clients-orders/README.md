## Kafka Clients for Orders with Avro and Schema Registry

This folder contains Python-based Kafka producer and consumer clients that demonstrate Avro (de)serialization with Confluent Schema Registry integration, showcasing schema evolution using both generic and specific records to illustrate how changes - such as adding new fields - affect message production and consumption.

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

#### Kafka producer

We can start the producer using either version 1 or 2. The newer version includes an additional field, `is_premium`, which is randomly assigned.

- `MODEL_VERSION` can be set to `1` or `2`.
- `MAX_RECORDS` defines how many records to produce. If omitted, the producer will run indefinitely.

```bash
MODEL_VERSION=1 MAX_RECORDS=3 \
  python fh-local-kafka-clients-orders/producer.py

# [2025-05-26 09:59:36,098] INFO: bootstrap: localhost:9092, topic: orders, model: 1
# [2025-05-26 09:59:36,240] INFO: Topic 'orders' created
# [2025-05-26 09:59:37,281] INFO: Sent to orders into partition 2, offset 0
# [2025-05-26 09:59:38,341] INFO: Sent to orders into partition 0, offset 0
# [2025-05-26 09:59:39,354] INFO: Sent to orders into partition 0, offset 1
# [2025-05-26 09:59:39,354] INFO: Max records (3) reached. Stop producing messages.
```

```bash
MODEL_VERSION=2 MAX_RECORDS=3 \
  python fh-local-kafka-clients-orders/producer.py

# [2025-05-26 10:02:04,953] INFO: bootstrap: localhost:9092, topic: orders, model: 2
# [2025-05-26 10:02:05,007] WARNING: Topic 'orders' already exists.
# [2025-05-26 10:02:06,039] INFO: Sent to orders into partition 0, offset 2
# [2025-05-26 10:02:07,048] INFO: Sent to orders into partition 0, offset 3
# [2025-05-26 10:02:08,087] INFO: Sent to orders into partition 1, offset 0
# [2025-05-26 10:02:08,087] INFO: Max records (3) reached. Stop producing messages.
```

#### Check schema and messages

After sending messages, we can inspect the Avro schema in the Schema menu, where the latest version appears in the subjects table. By default, the **Confluent Schema Registry** uses **backward compatibility**, allowing new schema versions to evolve without breaking existing consumers. This setting is configured on the server side and applies unless explicitly changed. By clicking the icon next to the subject name, we can view, edit, or delete the subject, as well as update its compatibility settings.

![](./images/schema-01.png)

Also, we can inspect the messages in the `orders` topic. For proper Avro decoding in Kpow, set the **Key Deserializer** to _String_, the **Value Deserializer** to _AVRO_, and select _Local Schema Registry_. Then, click the _Search_ button to view the records.

![](./images/messages-01.png)

Messages produced with the updated schema (version 2) will include the new field `is_premium`:

![](./images/messages-02.png)

On the other hand, those produced with the earlier schema (version 1) will not include this field:

![](./images/messages-03.png)

#### Start consumer

To consume messages, we can choose between **generic records** and **specific records**:

- **Generic record**: Messages are deserialized into plain Python dictionaries, without binding to a specific model. This mode works for any schema version.
- **Specific record**: Messages are deserialized into typed objects (`OrderV1` or `OrderV2`) by setting the `MODEL_VERSION` environment variable.

If using a specific record and the message lacks a field (e.g., `is_premium` in version 1), a default value (e.g., `false`) will be used during deserialization.

**Generic record**

```bash
python fh-local-kafka-clients-orders/consumer.py

# ...
# [2025-05-26 10:18:05,705] INFO: Received: {'order_id': '4d66a232-3087-4929-a26e-dc2b77d40e36', ..., 'is_premium': True}
# [2025-05-26 10:18:05,711] INFO: Received: {'order_id': '0309f917-1eea-4dd2-abd8-a2dbfa1a08a0', ...}
# [2025-05-26 10:18:05,711] INFO: Received: {'order_id': '18f3d53c-2c9c-44fd-9fc2-930d087bda8f', ...}
# [2025-05-26 10:18:05,711] INFO: Received: {'order_id': '33548c72-21dc-4806-9774-2d0a1e770f43', ..., 'is_premium': True}
# [2025-05-26 10:18:05,711] INFO: Received: {'order_id': '310625d2-a4b6-43b9-8bab-ae8ad71c53cf', ..., 'is_premium': False}
# [2025-05-26 10:18:05,712] INFO: Received: {'order_id': '2b549793-5617-4f1d-b9ef-ecbc9750477e', ..., }
```

**Specific record**

```bash
MODEL_VERSION=1 python fh-local-kafka-clients-orders/consumer.py

# ...
# [2025-05-26 10:20:08,722] INFO: Received: OrderV1(order_id='2b549793-5617-4f1d-b9ef-ecbc9750477e'...)
# [2025-05-26 10:20:08,722] INFO: Received: OrderV1(order_id='0309f917-1eea-4dd2-abd8-a2dbfa1a08a0'...)
# [2025-05-26 10:20:08,722] INFO: Received: OrderV1(order_id='18f3d53c-2c9c-44fd-9fc2-930d087bda8f'...)
# [2025-05-26 10:20:08,730] INFO: Received: OrderV1(order_id='33548c72-21dc-4806-9774-2d0a1e770f43'...)
# [2025-05-26 10:20:08,731] INFO: Received: OrderV1(order_id='310625d2-a4b6-43b9-8bab-ae8ad71c53cf'...)
# [2025-05-26 10:20:08,732] INFO: Received: OrderV1(order_id='4d66a232-3087-4929-a26e-dc2b77d40e36'...)

MODEL_VERSION=2 python fh-local-kafka-clients-orders/consumer.py

# ...
# [2025-05-26 10:21:42,063] INFO: Received: OrderV2(order_id='4d66a232-3087-4929-a26e-dc2b77d40e36', ..., is_premium=True)
# [2025-05-26 10:21:42,070] INFO: Received: OrderV2(order_id='2b549793-5617-4f1d-b9ef-ecbc9750477e', ..., is_premium=False)
# [2025-05-26 10:21:42,071] INFO: Received: OrderV2(order_id='0309f917-1eea-4dd2-abd8-a2dbfa1a08a0', ..., is_premium=False)
# [2025-05-26 10:21:42,071] INFO: Received: OrderV2(order_id='18f3d53c-2c9c-44fd-9fc2-930d087bda8f', ..., is_premium=False)
# [2025-05-26 10:21:42,072] INFO: Received: OrderV2(order_id='33548c72-21dc-4806-9774-2d0a1e770f43', ..., is_premium=True)
# [2025-05-26 10:21:42,072] INFO: Received: OrderV2(order_id='310625d2-a4b6-43b9-8bab-ae8ad71c53cf', ..., is_premium=False)
```

### Shutdown environment

Stop and remove the Docker containers.

```bash
docker compose -f ./factorhouse-local/compose-kpow-community.yml down
```
