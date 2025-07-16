import os
import argparse

from src import ps_utils


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
        "--topics",
        "-t",
        action="append",
        default=["demo.ecommerce.orders", "demo.ecommerce.order_items"],
        help="PubSub topic names",
    )
    args = parser.parse_args()
    os.environ["PUBSUB_EMULATOR_HOST"] = args.emulator_host
    os.environ["PUBSUB_PROJECT_ID"] = args.project_id

    for name in set(args.topics):
        ps_utils.create_topic(project_id=args.project_id, topic_name=name)
        ps_utils.create_subscription(
            project_id=args.project_id, sub_name=f"{name}.sub", topic_name=name
        )
    ps_utils.show_resources("topics", args.emulator_host, args.project_id)
    ps_utils.show_resources("subscriptions", args.emulator_host, args.project_id)
