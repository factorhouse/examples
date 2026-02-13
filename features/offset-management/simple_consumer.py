import os
import logging
from typing import List

from confluent_kafka import Consumer, KafkaException, TopicPartition, Message


# The on_assign callback is not typically used with the manual assign() method,
# as the assignment is done explicitly in the code.
# However, we can keep it for logging purposes if needed, but it won't be triggered
# by a rebalance.
def assignment_callback(_: Consumer, partitions: List[TopicPartition]):
    ## Callback function for logging assigned partitions
    for p in partitions:
        logging.info(f"Manually assigned to {p.topic}, partition {p.partition}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
    TOPIC_NAME = os.getenv("TOPIC_NAME", "offset-management")

    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": f"{TOPIC_NAME}-group",  # group.id is still needed for committing offsets
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(conf)

    # --- KEY CHANGE FROM subscribe() to assign() ---
    # Instead of subscribing to a topic and letting Kafka handle partition assignment,
    # we manually assign a specific partition.
    # Here, we are assigning partition 0 of the topic.
    # In a real-world scenario, you might want to fetch topic metadata to
    # determine the available partitions first.
    partitions_to_assign = [TopicPartition(TOPIC_NAME, 0)]
    consumer.assign(partitions_to_assign)
    assignment_callback(consumer, partitions_to_assign)

    try:
        while True:
            message: Message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            else:
                value = message.value().decode("utf8")
                logging.info(
                    f"Received {value} from partition {message.partition()}, offset {message.offset()}."
                )
                consumer.commit(message)
    except KeyboardInterrupt:
        logging.warning("Cancelled by user")
    finally:
        # It's good practice to unassign partitions before closing
        consumer.unassign()
        consumer.close()
