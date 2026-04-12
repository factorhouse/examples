## Queues for Kafka

**Start a single producer and multiple instances of consumer**

```bash
## A producer will send a message to a topic that has three partitions
docker compose -f ./features/queues-for-kafka/docker-compose.yml up -d producer

## Can create more consumer instances than the number of partitions
docker compose -f ./features/queues-for-kafka/docker-compose.yml up -d --scale consumer=6
```

**Monitor consumer instances**

```bash
## 1 to 6 instances can be monitored e.g.
docker logs queues-for-kafka-consumer-6 | tail

# [2026-04-12 23:53:16,226] WARN Share groups and KafkaShareConsumer are part of a preview feature introduced by KIP-932, and are not recommended for use in production. (org.apache.kafka.clients.consumer.internals.ShareConsumerDelegateCreator)
# Partition:0     Offset:1117     User_15 Order_ID_1615
# Partition:0     Offset:1118     User_17 Order_ID_1617
# Partition:0     Offset:1126     User_44 Order_ID_1644
# Partition:0     Offset:1127     User_48 Order_ID_1648
# Partition:0     Offset:1128     User_49 Order_ID_1649
# Partition:0     Offset:1129     User_0  Order_ID_1650
# Partition:0     Offset:1130     User_2  Order_ID_1652
# Partition:0     Offset:1131     User_4  Order_ID_1654
# Partition:0     Offset:1132     User_10 Order_ID_1660
# Partition:0     Offset:1133     User_11 Order_ID_1661
```
