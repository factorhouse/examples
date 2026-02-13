import os
import sys

from confluent_kafka.admin import AdminClient, KafkaException


def list_consumer_groups(bootstrap_servers):
    """
    Lists all consumer groups in the Kafka cluster.
    """
    conf = {
        "bootstrap.servers": bootstrap_servers,
    }
    admin_client = AdminClient(conf)

    print(f"Fetching consumer groups from {bootstrap_servers}...")

    try:
        list_groups_future = admin_client.list_consumer_groups()
        result = list_groups_future.result(timeout=10)
    except KafkaException as e:
        print(f"Failed to list groups: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Generic error: {e}")
        sys.exit(1)

    # Process and print the valid groups
    print("\n--- Consumer Groups ---")
    print(f"{'Group ID':<50} {'State':<20} {'Type'}")
    print("-" * 80)

    # valid contains a list of ConsumerGroupListing objects
    for group in result.valid:
        # .state is an Enum, usually converted to string automatically or needs str()
        # .is_simple_consumer_group tells us if it's modern or legacy
        group_type = "Simple" if group.is_simple_consumer_group else "Classic"

        print(f"{group.group_id:<50} {str(group.state):<20} {group_type}")

    # Handle errors (if permissions denied for specific groups, etc.)
    if result.errors:
        print("\n--- Errors ---")
        for error in result.errors:
            print(f"Error: {error}")


if __name__ == "__main__":
    # Change this to your Kafka broker address
    BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")

    list_consumer_groups(BOOTSTRAP_SERVERS)
