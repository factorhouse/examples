import dataclasses
import datetime
import random
import logging
import typing
import uuid

from src.utils import (
    fake,
    generate_products,
    get_address,
    created_at,
    generate_uri,
    MIN_AGE,
    MAX_AGE,
    MINUTES_IN_HOUR,
    MINUTES_IN_DAY,
    SECONDS_IN_MINUTE,
)

products = generate_products()
logging.info("generating products helper dict")
logging.info("generating locations data")
PRODUCT_GENDER_DICT = products[0]
PRODUCT_BY_ID_DICT = products[1]


# utility class
class DataUtil:
    def child_created_at(
        self, probability: str = "uniform"
    ) -> datetime.datetime:  # returns a random timestamp between now and parent date
        time_between_dates = datetime.datetime.now() - self.parent.created_at
        days_between_dates = time_between_dates.days
        if days_between_dates <= 1:
            days_between_dates = 2
        random_number_of_days = random.randrange(
            1, days_between_dates
        )  # generates random day between now and when user initially got created
        created_at = self.parent.created_at + datetime.timedelta(
            days=random_number_of_days
        )
        return created_at

    def random_item(
        self, population, **distribution
    ) -> str:  # returns single random item from a list based off distribution
        if distribution:
            return random.choices(
                population=population, weights=distribution["distribution"]
            )[0]
        else:
            return random.choices(population=population)[0]

    def asdict(self, excludes=[]) -> dict:
        d = dataclasses.asdict(self)
        if len(excludes) > 0:
            d = {k: v for k, v in d.items() if k not in excludes}
        return d


@dataclasses.dataclass
class Address:
    def __init__(self, data: dict):
        self.street = data["street"]
        self.city = data["city"]
        self.state = data["state"]
        self.postal_code = data["postal_code"]
        self.country = data["country"]
        self.latitude = data["latitude"]
        self.longitude = data["longitude"]

    def __str__(self):
        return f"{self.street} \n{self.city}, {self.state} \n{self.postal_code} \n{self.country} \n{self.latitude} \n{self.longitude}"


@dataclasses.dataclass
class User(DataUtil):
    logging.info("generating user")
    id: str = dataclasses.field(init=False)
    first_name: str = dataclasses.field(init=False)
    last_name: str = dataclasses.field(init=False)
    email: str = dataclasses.field(init=False)
    age: int = dataclasses.field(init=False)
    gender: str = dataclasses.field(init=False)
    state: str = dataclasses.field(init=False)
    street_address: str = dataclasses.field(init=False)
    postal_code: str = dataclasses.field(init=False)
    city: str = dataclasses.field(init=False)
    country: str = dataclasses.field(init=False)
    latitude: float = dataclasses.field(init=False)
    longitude: float = dataclasses.field(init=False)
    traffic_source: str = dataclasses.field(init=False)
    created_at: datetime.datetime = dataclasses.field(init=False)
    # extras
    orders: typing.List[typing.Any] = dataclasses.field(init=False)

    def __post_init__(self):
        self.id = fake.uuid4()
        self.gender = self.random_item(population=["M", "F"])  # uniform distribution
        if self.gender == "M":
            self.first_name = fake.first_name_male()
            self.traffic_source = self.random_item(
                population=["Organic", "Facebook", "Search", "Email", "Display"],
                distribution=[0.15, 0.06, 0.7, 0.05, 0.04],
            )
        if self.gender == "F":
            self.first_name = fake.first_name_female()
            self.traffic_source = self.random_item(
                population=["Organic", "Facebook", "Search", "Email", "Display"],
                distribution=[0.15, 0.06, 0.7, 0.05, 0.04],
            )
        self.last_name = fake.last_name_nonbinary()
        address = Address(get_address())
        self.state = address.state
        self.street_address = address.street
        self.postal_code = address.postal_code
        self.city = address.city
        self.country = address.country
        self.latitude = address.latitude
        self.longitude = address.longitude
        self.email = f"{self.first_name.lower()}{self.last_name.lower()}@{fake.safe_domain_name()}"
        self.age = random.randrange(MIN_AGE, MAX_AGE)
        # weight newer users/orders
        choice = random.choices([0, 1], weights=[0.975, 0.025])[0]
        if choice == 0:
            self.created_at = created_at(datetime.datetime(2019, 1, 1))
        if choice == 1:
            self.created_at = created_at(
                datetime.datetime.now() - datetime.timedelta(days=7)
            )
        num_of_orders = random.choices(
            population=[0, 1, 2, 3, 4], weights=[0.2, 0.5, 0.2, 0.05, 0.05]
        )[0]
        if num_of_orders == 0:
            self.orders = []
        else:
            self.orders = [Order(user=self) for _ in range(num_of_orders)]

    def __str__(self):
        return f"{self.id}, {self.first_name}, {self.last_name}, {self.email}, {self.age}, {self.gender}, {self.state}, {self.street_address}, {self.postal_code}, {self.city}, {self.traffic_source}, {self.created_at}, {len(self.orders)} orders"


@dataclasses.dataclass
class Order(DataUtil):
    logging.info("generating order")
    order_id: str = dataclasses.field(init=False)
    user_id: str = dataclasses.field(init=False)
    status: str = dataclasses.field(init=False)
    gender: str = dataclasses.field(init=False)
    created_at: datetime.datetime = dataclasses.field(init=False)
    returned_at: datetime.datetime = dataclasses.field(init=False)
    shipped_at: datetime.datetime = dataclasses.field(init=False)
    delivered_at: datetime.datetime = dataclasses.field(init=False)
    num_of_item: int = dataclasses.field(init=False)
    user: dataclasses.InitVar[typing.Any] = None
    # extras
    order_items: typing.List[typing.Any] = dataclasses.field(init=False)

    def __post_init__(self, user=None):
        self.order_id = fake.uuid4()
        self.parent = user
        self.user_id = user.id
        self.gender = user.gender
        self.status = self.random_item(
            population=["Complete", "Cancelled", "Returned", "Processing", "Shipped"],
            distribution=[0.25, 0.15, 0.1, 0.2, 0.3],
        )
        self.created_at = self.child_created_at()
        # add random generator for days it takes to ship, deliver, return etc.
        if self.status == "Returned":
            self.shipped_at = self.created_at + datetime.timedelta(
                minutes=random.randrange(MINUTES_IN_DAY * 3)
            )  # shipped between 0-3 days after order placed
            self.delivered_at = self.shipped_at + datetime.timedelta(
                minutes=random.randrange(MINUTES_IN_DAY * 5)
            )  # delivered between 0-5 days after ship date
            self.returned_at = self.delivered_at + datetime.timedelta(
                minutes=random.randrange(MINUTES_IN_DAY * 3)
            )  # returned 0-3 days after order is delivered
        elif self.status == "Complete":
            self.shipped_at = self.created_at + datetime.timedelta(
                minutes=random.randrange(MINUTES_IN_DAY * 3)
            )  # shipped between 0-3 days after order placed
            self.delivered_at = self.shipped_at + datetime.timedelta(
                minutes=random.randrange(MINUTES_IN_DAY * 5)
            )  # delivered between 0-5 days after ship date
            self.returned_at = None
        elif self.status == "Shipped":
            self.shipped_at = self.created_at + datetime.timedelta(
                minutes=random.randrange(MINUTES_IN_DAY * 3)
            )  # shipped between 0-3 days after order placed
            self.delivered_at = None
            self.returned_at = None
        else:
            self.shipped_at = None
            self.delivered_at = None
            self.returned_at = None
        self.user = user  # pass person object to order_items
        # randomly generate number of items in an order
        num_of_items = self.random_item(
            population=[1, 2, 3, 4], distribution=[0.7, 0.2, 0.05, 0.05]
        )
        self.num_of_item = num_of_items
        self.order_items = [OrderItem(order=self) for _ in range(num_of_items)]

    def __str__(self):
        return f"{self.order_id}, {self.user_id}, {self.status}, {self.created_at}, {self.shipped_at}, {self.delivered_at}, {self.returned_at}, {len(self.order_items)} items"


@dataclasses.dataclass
class Product:
    logging.info("generating product")
    product_id: int = dataclasses.field(init=False)
    brand: str = dataclasses.field(init=False)
    name: str = dataclasses.field(init=False)
    cost: float = dataclasses.field(init=False)
    category: str = dataclasses.field(init=False)
    department: str = dataclasses.field(init=False)
    sku: str = dataclasses.field(init=False)
    retail_price: float = dataclasses.field(init=False)
    distribution_center_id: int = dataclasses.field(init=False)

    def __post_init__(self):
        person = User()
        random_idx = fake.random_int(
            min=0, max=len(len(PRODUCT_GENDER_DICT[person.gender]))
        )
        product = PRODUCT_GENDER_DICT[person.gender][random_idx]
        self.brand = product[0]
        self.name = product[1]
        self.cost = product[2]
        self.category = product[3]
        self.department = product[4]
        self.sku = product[5]
        self.retail_price = product[6]
        self.distribution_center_id = product[7]

    def __str__(self):
        return f"{self.brand}, {self.name}, {self.cost}, {self.category}, {self.department}, {self.sku}, {self.retail_price}, {self.distribution_center_id}"


@dataclasses.dataclass
class Event(DataUtil):
    logging.info("generating event")
    id: str = dataclasses.field(init=False)
    user_id: int = dataclasses.field(init=False)
    sequence_number: int = dataclasses.field(init=False)
    session_id: str = dataclasses.field(init=False)
    created_at: datetime.datetime = dataclasses.field(init=False)
    ip_address: str = dataclasses.field(init=False)
    city: str = dataclasses.field(init=False)
    state: str = dataclasses.field(init=False)
    postal_code: str = dataclasses.field(init=False)
    browser: str = dataclasses.field(init=False)
    traffic_source: str = dataclasses.field(init=False)
    uri: str = dataclasses.field(init=False)
    event_type: str = dataclasses.field(init=False)
    order_item: dataclasses.InitVar[typing.Any] = None

    def __post_init__(self, order_item=None):
        self.id = fake.uuid4()
        self.user_id = order_item.user_id
        self.sequence_number = order_item.sequence_number
        self.created_at = order_item.created_at
        self.session_id = order_item.session_id
        self.ip_address = order_item.ip_address
        self.city = order_item.person.city
        self.state = order_item.person.state
        self.postal_code = order_item.person.postal_code
        self.event_type = order_item.event_type
        self.browser = order_item.browser
        self.uri = order_item.uri
        self.traffic_source = order_item.traffic_source

    def __str__(self):
        return f"{self.created_at}, {self.ip_address}, {self.city}, {self.state}, {self.postal_code}"


@dataclasses.dataclass
class OrderItem(DataUtil):
    logging.info("generating order item")
    id: str = dataclasses.field(init=False)
    order_id: str = dataclasses.field(init=False)
    user_id: str = dataclasses.field(init=False)
    product_id: int = dataclasses.field(init=False)
    inventory_item_id: str = dataclasses.field(init=False)
    status: str = dataclasses.field(init=False)
    created_at: datetime.datetime = dataclasses.field(init=False)
    shipped_at: datetime.datetime = dataclasses.field(init=False)
    delivered_at: datetime.datetime = dataclasses.field(init=False)
    returned_at: datetime.datetime = dataclasses.field(init=False)
    order: dataclasses.InitVar[typing.Any] = None
    sale_price: float = dataclasses.field(init=False)
    # extras
    event_type: str = dataclasses.field(init=False)
    ip_address: str = dataclasses.field(init=False)
    browser: str = dataclasses.field(init=False)
    traffic_source: str = dataclasses.field(init=False)
    session_id: str = dataclasses.field(init=False)
    sequence_number: int = dataclasses.field(init=False)
    uri: str = dataclasses.field(init=False)
    is_sold: bool = dataclasses.field(init=False)
    events: typing.List[typing.Any] = dataclasses.field(init=False)
    inventory_items: typing.List[typing.Any] = dataclasses.field(init=False)

    def __post_init__(self, order=None):
        global inv_item_id

        self.events = []
        self.inventory_items = []

        self.id = fake.uuid4()
        self.order_id = order.order_id
        self.user_id = order.user_id
        self.inventory_item_id = str(uuid.uuid4())
        self.status = order.status
        self.created_at = order.created_at - datetime.timedelta(
            seconds=random.randrange(SECONDS_IN_MINUTE * 240)
        )  # order purchased within 4 hours

        self.shipped_at = order.shipped_at
        self.delivered_at = order.delivered_at
        self.returned_at = order.returned_at

        random_idx = fake.random_int(min=0, max=len(PRODUCT_GENDER_DICT[order.gender]))
        product = PRODUCT_GENDER_DICT[order.gender][random_idx]
        self.product_id = product[0]
        self.sale_price = product[7]
        self.ip_address = fake.ipv4()
        self.browser = self.random_item(
            population=["IE", "Chrome", "Safari", "Firefox", "Other"],
            distribution=[0.05, 0.5, 0.2, 0.2, 0.05],
        )
        self.traffic_source = self.random_item(
            population=["Email", "Adwords", "Organic", "YouTube", "Facebook"],
            distribution=[0.45, 0.3, 0.05, 0.1, 0.1],
        )
        self.session_id = str(uuid.uuid4())

        self.person = order.user  # pass person object to events
        self.is_sold = True
        previous_created_at = None

        # Generate Events Table
        if order.num_of_item == 1:  # if only 1 item in order go through flow
            for idx, val in enumerate(
                ["home", "department", "product", "cart", "purchase"]
            ):
                self.sequence_number = idx + 1
                self.event_type = val
                self.uri = generate_uri(val, product)
                self.events.append(Event(order_item=self))
                previous_created_at = self.created_at
                self.created_at = previous_created_at + datetime.timedelta(
                    seconds=random.randrange(SECONDS_IN_MINUTE * 3)
                )
        else:  # if multiple items
            sequence_num = 0  # track sequence num of purchase event
            for _ in range(order.num_of_item):
                for event in ["department", "product", "cart"]:
                    sequence_num += 1
                    self.sequence_number = sequence_num
                    self.event_type = event
                    self.uri = generate_uri(event, product)
                    self.events.append(Event(order_item=self))
                    sequence_num = self.sequence_number
                    previous_created_at = self.created_at
                    self.created_at = previous_created_at + datetime.timedelta(
                        seconds=random.randrange(180)
                    )
            self.sequence_number = sequence_num + 1
            self.created_at += datetime.timedelta(random.randrange(5))
            self.event_type = "purchase"
            self.uri = generate_uri("purchase", product)
            self.events.append(Event(order_item=self))

        # sold inventory item
        self.inventory_items.append(InventoryItem(order_item=self))

        # unsold inventory items
        num_of_items = self.random_item(
            population=[1, 2, 3], distribution=[0.5, 0.3, 0.2]
        )
        for _ in range(num_of_items):
            self.is_sold = False
            # inv_item_id += 1
            # self.inventory_item_id = inv_item_id
            self.inventory_item_id = str(uuid.uuid4())
            self.inventory_items.append(InventoryItem(order_item=self))

    def __str__(self):
        return f"{self.id}, {self.order_id}, {self.user_id}, {self.product_id}, {self.inventory_item_id}, {self.status}, {len(self.events)} events, {len(self.inventory_items)} inv items"


@dataclasses.dataclass
class InventoryItem(DataUtil):
    id: str = dataclasses.field(init=False)
    product_id: int = dataclasses.field(init=False)
    created_at: datetime.datetime = dataclasses.field(init=False)
    sold_at: datetime.datetime = dataclasses.field(init=False)
    cost: float = dataclasses.field(init=False)
    product_category: str = dataclasses.field(init=False)
    product_name: str = dataclasses.field(init=False)
    product_brand: str = dataclasses.field(init=False)
    product_retail_price: float = dataclasses.field(init=False)
    product_department: str = dataclasses.field(init=False)
    product_sku: str = dataclasses.field(init=False)
    product_distribution_center_id: int = dataclasses.field(init=False)
    order_item: dataclasses.InitVar[typing.Any] = None

    def __post_init__(self, order_item=None):
        self.id = order_item.inventory_item_id
        self.product_id = order_item.product_id
        if order_item.is_sold is True:
            self.created_at = order_item.created_at - datetime.timedelta(
                minutes=random.randrange(86400)
            )  # in inventory between 0 and 60 days
            self.sold_at = (
                order_item.created_at
            )  # sold on the date/time the order_items was logged
        if order_item.is_sold is False:
            self.created_at = created_at(datetime.datetime(2020, 1, 1))
            self.sold_at = None
        self.cost = PRODUCT_BY_ID_DICT[self.product_id]["cost"]
        self.product_category = PRODUCT_BY_ID_DICT[self.product_id]["category"]
        self.product_name = PRODUCT_BY_ID_DICT[self.product_id]["name"]
        self.product_brand = PRODUCT_BY_ID_DICT[self.product_id]["brand"]
        self.product_retail_price = PRODUCT_BY_ID_DICT[self.product_id]["retail_price"]
        self.product_department = PRODUCT_BY_ID_DICT[self.product_id]["department"]
        self.product_sku = PRODUCT_BY_ID_DICT[self.product_id]["sku"]
        self.product_distribution_center_id = PRODUCT_BY_ID_DICT[self.product_id][
            "distribution_center_id"
        ]

    def __str__(self):
        return f"{self.id}, {self.product_id}, {self.created_at}, {self.cost}, {self.product_category}, {self.product_name}, {self.product_brand}, {self.product_retail_price}, {self.product_department}, {self.product_sku}, {self.product_distribution_center_id}"


@dataclasses.dataclass
class GhostEvents(DataUtil):
    id: int = dataclasses.field(init=False)
    user_id: int = dataclasses.field(init=False)
    sequence_number: int = dataclasses.field(init=False)
    session_id: str = dataclasses.field(init=False)
    created_at: datetime.datetime = dataclasses.field(init=False)
    ip_address: str = dataclasses.field(init=False)
    city: str = dataclasses.field(init=False)
    state: str = dataclasses.field(init=False)
    postal_code: str = dataclasses.field(init=False)
    browser: str = dataclasses.field(init=False)
    traffic_source: str = dataclasses.field(init=False)
    uri: str = dataclasses.field(init=False)
    event_type: str = dataclasses.field(init=False)
    # extras
    events: typing.List[typing.Any] = dataclasses.field(init=False)

    def __post_init__(self):
        address = get_address()
        self.sequence_number = 0
        self.user_id = None
        self.created_at = created_at(datetime.datetime(2019, 1, 1))
        self.session_id = str(uuid.uuid4())
        self.ip_address = fake.ipv4()
        self.city = address["city"]
        self.state = address["state"]
        self.postal_code = address["postal_code"]
        self.browser = self.random_item(
            population=["IE", "Chrome", "Safari", "Firefox", "Other"],
            distribution=[0.05, 0.5, 0.2, 0.2, 0.05],
        )
        self.traffic_source = self.random_item(
            population=["Email", "Adwords", "Organic", "YouTube", "Facebook"],
            distribution=[0.45, 0.3, 0.05, 0.1, 0.1],
        )

        products = PRODUCT_GENDER_DICT[
            self.random_item(population=["M", "F"], distribution=[0.5, 0.5])
        ]
        product = self.random_item(products)

        # different event type combinations
        cancelled_browsing = ["product", "cart", "cancel"]
        abandoned_cart = ["department", "product", "cart"]
        viewed_product = ["product"]
        viewed_department = ["department", "product"]

        random_events = self.random_item(
            population=[
                cancelled_browsing,
                abandoned_cart,
                viewed_product,
                viewed_department,
            ]
        )

        self.events = []
        for event in random_events:
            event_id = str(uuid.uuid4())
            self.id = event_id + 1
            event_id = self.id

            self.event_type = event
            self.uri = generate_uri(event, product)
            self.sequence_number += 1
            self.created_at = self.created_at + datetime.timedelta(
                minutes=random.randrange(int(MINUTES_IN_HOUR * 0.5))
            )
            self.events.append(dataclasses.asdict(self))

    def __str__(self):
        return f"{self.created_at}, {self.ip_address}, {self.city}, {self.state}, {self.postal_code}"
