import os
import logging
from typing import List

from confluent_kafka import Consumer, KafkaException, TopicPartition, Message


def assignment_callback(_: Consumer, partitions: List[TopicPartition]):
    ## callback function that gets triggered when a topic partion is assigned
    for p in partitions:
        logging.info(f"Assigned to {p.topic}, partiton {p.partition}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
    TOPIC_NAME = os.getenv("TOPIC_NAME", "offset-management")

    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": f"{TOPIC_NAME}-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC_NAME], on_assign=assignment_callback)

    try:
        while True:
            message: Message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            else:
                value = message.value().decode("utf8")
                partition = message.partition()
                logging.info(
                    f"Reveived {value} from partition {message.partition()}, offset {message.offset()}."
                )
                consumer.commit(message)
    except KeyboardInterrupt:
        logging.warning("cancelled by user")
    finally:
        consumer.close()
