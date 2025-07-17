import os
import collections
import csv
import datetime
import random
import typing
import logging
from typing import List

from faker import Faker
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaException, KafkaError

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)

fake = Faker()

SOURCE_DIR = os.getenv(
    "SOURCE_DIR", os.path.join(os.path.dirname(os.path.realpath(__file__)), "data")
)

SECONDS_IN_MINUTE = 60
MINUTES_IN_HOUR = 60
MINUTES_IN_DAY = 1440
MIN_AGE = 12
MAX_AGE = 71


def generate_products() -> typing.List[dict]:
    """
    Read from local csv and return products.
    """
    product_brand_dict = {}  # products partitioned by brand - unused
    product_category_dict = {}  # product partitioned by cateogry - unused
    gender_category_dict = {}  # products partitioned by gender and category - unused
    product_id_dict = {}  # products to generate events table - unused
    product_gender_dict = {}  # product partitioned by gender
    product_by_id_dict = {}  # products partitioned by product ID

    products = collections.defaultdict(list)
    with open(f"{SOURCE_DIR}/products.csv", encoding="utf-8") as productcsv:
        csv_reader = csv.DictReader(productcsv)
        for rows in csv_reader:
            for k, v in rows.items():
                products[k].append(v)

    product_id = products["id"]
    brands = products["brand"]
    name = products["name"]
    cost = products["cost"]
    category = products["category"]
    department = products["department"]
    sku = products["sku"]
    retail_price = products["retail_price"]
    distribution_center_id = products["distribution_center_id"]
    for _ in range(len(brands)):
        product_brand_dict[brands[_]] = []
        product_category_dict[category[_]] = []
        product_id_dict[product_id[_]] = []
        product_by_id_dict[product_id[_]] = []
        if department[_] == "Men":
            product_gender_dict["M"] = []
            gender_category_dict["M" + category[_]] = []
        if department[_] == "Women":
            product_gender_dict["F"] = []
            gender_category_dict["F" + category[_]] = []
    for col in list(
        zip(
            product_id,
            brands,
            name,
            cost,
            category,
            department,
            sku,
            retail_price,
            distribution_center_id,
        )
    ):
        product_id = col[0]
        brand = col[1]
        name = col[2]
        cost = col[3]
        category = col[4]
        department = col[5]
        sku = col[6]
        retail_price = col[7]
        distribution_center_id = col[8]

        product_by_id_dict[product_id] = {
            "brand": brand,
            "name": name,
            "cost": cost,
            "category": category,
            "department": department,
            "sku": sku,
            "retail_price": retail_price,
            "distribution_center_id": distribution_center_id,
        }
        product_brand_dict[brand].append(col)
        product_category_dict[category].append(col)
        if department == "Men":
            product_gender_dict["M"].append(col)
            gender_category_dict["M" + category].append(col)
        if department == "Women":
            product_gender_dict["F"].append(col)
            gender_category_dict["F" + category].append(col)

    # helper dict to generate events
    for col in list(zip(product_id, brands, category, department)):
        product_id_dict[col[0]] = {
            "brand": col[1],
            "category": col[2],
            "department": col[3],
        }
    return product_gender_dict, product_by_id_dict, products


def generate_from_csv(file_name: str) -> typing.List[dict]:
    records = []
    with open(f"{SOURCE_DIR}/{file_name}", encoding="utf-8") as worldcsv:
        csvReader = csv.DictReader(worldcsv)
        for rows in csvReader:
            records.append(rows)
    return records


# returns random address based off specified distribution
def get_address(
    *,
    country: str = "*",
    state: str = "*",
    postal_code: str = "*",
    location_data: list = generate_from_csv("world_pop.csv"),
) -> dict:
    # country = '*' OR country = 'USA' OR country={'USA':.75,'UK':.25}
    # state = '*' OR state = 'California' OR state={'California':.75,'New York':.25}
    # postal_code = '*' OR postal_code = '95060' OR postal_code={'94117':.75,'95060':.25}
    # type checking is used to provide flexibility of inputs to function (ie. can be dict with proportions, or could be single string value)
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
        "street": fake.street_address(),
        "city": loc["city"],
        "state": loc["state"],
        "postal_code": loc["postal_code"],
        "country": loc["country"],
        "latitude": loc["latitude"],
        "longitude": loc["longitude"],
    }


def created_at(start_date: datetime.datetime) -> datetime.datetime:
    """
    Generates random date between now and specified date.
    """
    end_date = datetime.datetime.now()
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    if days_between_dates <= 1:
        days_between_dates = 2
    random_number_of_days = random.randrange(1, days_between_dates)
    created_at = (
        start_date
        + datetime.timedelta(days=random_number_of_days)
        + datetime.timedelta(minutes=random.randrange(MINUTES_IN_HOUR * 19))
    )
    return created_at


def generate_uri(event: str, product: str) -> str:
    """
    Generate URI for events table.
    """
    if event == "product":
        return f"/{event}/{product[0]}"
    elif event == "department":
        return f"""/{event}/{product[5].lower()}/category/{product[4].lower().replace(" ", "")}/brand/{product[1].lower().replace(" ", "")}"""
    else:
        return f"/{event}"


def get_topic_names(prefix: str, schema: str, table_map: dict):
    """
    Get all unique topic names from the nested dictionary map.
    """
    topic_names = set()
    for category in table_map.values():
        if "data" in category:
            topic_names.update(
                [f"{prefix}.{schema}.{key}" for key in category["data"].keys()]
            )
    return list(topic_names)


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
