import os
import csv
import random
import logging
import asyncio
import functools
from typing import List

from sqlalchemy.exc import SQLAlchemyError
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaException, KafkaError

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)

SOURCE_DIR = os.getenv(
    "SOURCE_DIR", os.path.join(os.path.dirname(os.path.realpath(__file__)), "data")
)


def generate_from_csv(file_name: str) -> List[dict]:
    records = []
    with open(f"{SOURCE_DIR}/{file_name}", encoding="utf-8") as csv_file:
        csvReader = csv.DictReader(csv_file)
        for rows in csvReader:
            records.append(rows)
    return records


def get_product_map(file_nmae: str = "products.csv"):
    prod_map = {}
    for product in generate_from_csv(file_nmae):
        prod_map[product["id"]] = {
            "retail_price": product["retail_price"],
            "department": product["department"].lower(),
            "category": product["category"].lower(),
        }
    return prod_map


def get_location(
    *,
    country: str = "*",
    state: str = "*",
    postal_code: str = "*",
    location_data: list = generate_from_csv("world_pop.csv"),
) -> dict:
    """
    Returns random location based off specified distribution

    country = '*' OR country = 'USA' OR country={'USA':.75,'UK':.25}
    state = '*' OR state = 'California' OR state={'California':.75,'New York':.25}
    postal_code = '*' OR postal_code = '95060' OR postal_code={'94117':.75,'95060':.25}
    type checking is used to provide flexibility of inputs to function (ie. can be dict with proportions, or could be single string value)
    """
    universe = []
    if postal_code != "*":
        if isinstance(postal_code, str):
            universe += list(
                filter(lambda row: row["postal_code"] == postal_code, location_data)
            )
        elif isinstance(postal_code, dict):
            universe += list(
                filter(
                    lambda row: row["postal_code"] in postal_code.keys(), location_data
                )
            )
    if state != "*":
        if isinstance(state, str):
            universe += list(filter(lambda row: row["state"] == state, location_data))
        elif isinstance(state, dict):
            universe += list(
                filter(lambda row: row["state"] in state.keys(), location_data)
            )
    if country != "*":
        if isinstance(country, str):
            universe += list(
                filter(lambda row: row["country"] == country, location_data)
            )
        elif isinstance(country, dict):
            universe += list(
                filter(lambda row: row["country"] in country.keys(), location_data)
            )
    if len(universe) == 0:
        universe = location_data

    total_pop = sum([int(loc["population"]) for loc in universe])

    for loc in universe:
        loc["population"] = int(loc["population"])
        if isinstance(postal_code, dict):
            if loc["postal_code"] in postal_code.keys():
                loc["population"] = postal_code[loc["postal_code"]] * total_pop
        if isinstance(state, dict):
            if loc["state"] in state.keys():
                loc["population"] = (
                    state[loc["state"]]
                    * (
                        loc["population"]
                        / sum(
                            [
                                loc2["population"]
                                for loc2 in universe
                                if loc["state"] == loc2["state"]
                            ]
                        )
                    )
                    * total_pop
                )
        if isinstance(country, dict):
            if loc["country"] in country.keys():
                loc["population"] = (
                    country[loc["country"]]
                    * (
                        loc["population"]
                        / sum(
                            [
                                loc2["population"]
                                for loc2 in universe
                                if loc["country"] == loc2["country"]
                            ]
                        )
                    )
                    * total_pop
                )

    loc = random.choices(
        universe, weights=[loc["population"] / total_pop for loc in universe]
    )[0]
    return {
        "city": loc["city"],
        "state": loc["state"],
        "postal_code": loc["postal_code"],
        "country": loc["country"],
        "latitude": loc["latitude"],
        "longitude": loc["longitude"],
    }


def async_retry_on_db_error(max_retries=3, initial_delay=1):
    """
    A decorator factory that makes an async function retry on SQLAlchemyError.

    Args:
        max_retries (int): Maximum number of retries before giving up.
        initial_delay (int): The initial delay in seconds for the first retry.
                             The delay uses exponential backoff.
    """

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    # Attempt to run the decorated async function
                    return await func(*args, **kwargs)
                except SQLAlchemyError as e:
                    if attempt == max_retries:
                        logging.error(
                            f"DB operation '{func.__name__}' failed after {max_retries} attempts. Final error: {e}"
                        )
                        raise  # Re-raise the last exception to exit the program

                    # Calculate delay with exponential backoff
                    delay = initial_delay * (2 ** (attempt - 1))
                    logging.warning(
                        f"DB error in '{func.__name__}' on attempt {attempt}/{max_retries}. "
                        f"Retrying in {delay}s... Error: {e}"
                    )
                    await asyncio.sleep(delay)

        return wrapper

    return decorator


def create_topics_if_not_exists(
    bootstrap_servers: str,
    topic_names: List[str],
    num_partitions: int = 3,
    replication_factor: int = 1,
):
    """Uses an AdminClient to create topics."""
    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
    topics = [
        NewTopic(name, num_partitions, replication_factor) for name in topic_names
    ]
    result_dict = admin_client.create_topics(topics)

    for topic, future in result_dict.items():
        try:
            future.result(timeout=3)
            logging.info(f"Topic '{topic}' created.")
        except KafkaException as e:
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                logging.warning(f"Topic '{topic}' already exists.")
            else:
                raise RuntimeError(f"Failed to create topic '{topic}'.") from e
