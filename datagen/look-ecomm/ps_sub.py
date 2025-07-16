import os
import json
import argparse

from google.cloud import pubsub_v1

from src import ps_utils


def callback(message):
    print(json.loads(message.data.decode())["payload"])
    message.ack()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create PubSub resources")
    parser.add_argument(
        "--emulator-host",
        "-e",
        default="localhost:8085",
        help="PubSub emulator host address",
    )
    parser.add_argument(
        "--project-id", "-p", default="test-project", help="GCP project id"
    )
    parser.add_argument(
        "--topic",
        "-t",
        default="demo.ecommerce.orders",
        help="PubSub topic name",
    )
    args = parser.parse_args()
    os.environ["PUBSUB_EMULATOR_HOST"] = args.emulator_host
    os.environ["PUBSUB_PROJECT_ID"] = args.project_id

    with pubsub_v1.SubscriberClient() as subscriber:
        future = subscriber.subscribe(
            ps_utils.set_sub_name(args.project_id, f"{args.topic}.sub"), callback
        )
        try:
            future.result()
        except KeyboardInterrupt:
            future.cancel()
