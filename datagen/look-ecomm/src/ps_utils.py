import requests

from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists


def set_topic_name(project_id: str, topic_name: str):
    return f"projects/{project_id}/topics/{topic_name}"


def set_sub_name(project_id: str, sub_name: str):
    return f"projects/{project_id}/subscriptions/{sub_name}"


def show_resources(resources: str, api_endpoint: str, project_id: str):
    resp = requests.get(f"http://{api_endpoint}/v1/projects/{project_id}/{resources}")
    for resource in resp.json()[resources]:
        print(resource["name"])


def list_subs(api_endpoint: str):
    print(
        pubsub_v1.PublisherClient(
            client_options={"api_endpoint": api_endpoint}
        ).list_topic_subscriptions()
    )


def create_topic(project_id: str, topic_name: str):
    try:
        pubsub_v1.PublisherClient().create_topic(
            name=set_topic_name(project_id, topic_name)
        )
    except AlreadyExists:
        pass
    except Exception as ex:
        raise RuntimeError(ex) from ex


def create_subscription(project_id: str, sub_name: str, topic_name: str):
    try:
        pubsub_v1.SubscriberClient().create_subscription(
            name=set_sub_name(project_id, sub_name),
            topic=set_topic_name(project_id, topic_name),
        )
    except AlreadyExists:
        pass
    except Exception as ex:
        raise RuntimeError(ex) from ex
