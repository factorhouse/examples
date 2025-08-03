import datetime
import dataclasses
import random
import logging
import inspect
from enum import Enum
from collections import OrderedDict
from typing import List, Optional, Self

from faker import Faker

from src.utils import get_location, get_product_map

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)


PRODUCT_MAP = get_product_map("products.csv")


def get_additional_ddls(schema: str):
    return {
        "products": inspect.cleandoc(f"""
            CREATE TABLE IF NOT EXISTS {schema}.products (
                id BIGINT PRIMARY KEY,
                cost DOUBLE PRECISION,
                category TEXT,
                name TEXT,
                brand TEXT,
                retail_price DOUBLE PRECISION,
                department TEXT,
                sku TEXT,
                distribution_center_id BIGINT
            );"""),
        "dist_centers": inspect.cleandoc(f"""
            CREATE TABLE IF NOT EXISTS {schema}.dist_centers (
                id BIGINT PRIMARY KEY,
                name TEXT,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION
            );"""),
        "heartbeat": inspect.cleandoc(f"""
            CREATE TABLE IF NOT EXISTS {schema}.heartbeat (
                id INT PRIMARY KEY,
                ts TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
            );
        """),
    }


class OrderStatus(Enum):
    PROCESSING = "Processing"
    SHIPPED = "Shipped"
    DELIVERED = "Delivered"
    CANCELLED = "Cancelled"
    RETURNED = "Returned"


class EventCategory(Enum):
    PURCHASE = "purchase"
    GHOST = "ghost"
    CANCEL = "cancel"
    RETURN = "return"


class ModelMixin:
    @classmethod
    def from_dict(cls, data: dict):
        valid_keys = {f.name for f in dataclasses.fields(cls)}
        filtered = {k: v for k, v in data.items() if k in valid_keys}
        return cls(**filtered)

    @classmethod
    def from_rows(cls, rows: List[dict]):
        return [cls.from_dict(row) for row in rows]


@dataclasses.dataclass
class User(ModelMixin):
    id: str
    first_name: str
    last_name: str
    email: str
    age: int
    gender: str
    street_address: str
    postal_code: str
    city: str
    state: str
    country: str
    latitude: float
    longitude: float
    traffic_source: str
    created_at: datetime.datetime
    updated_at: datetime.datetime

    @classmethod
    def new(
        cls,
        *,
        country: str = "*",
        state: str = "*",
        postal_code: str = "*",
        fake: Faker,
    ) -> Self:
        gender = fake.random_element(elements=("M", "F"))
        first_name = (
            fake.first_name_male() if gender == "M" else fake.first_name_female()
        )
        last_name = fake.last_name_nonbinary()
        location = get_location(country=country, state=state, postal_code=postal_code)
        traffic_source = fake.random_choices(
            elements=OrderedDict(
                zip(
                    ["Organic", "Facebook", "Search", "Email", "Display"],
                    [0.15, 0.06, 0.7, 0.05, 0.04],
                )
            ),
            length=1,
        )[0]
        return cls(
            id=fake.uuid4(),
            first_name=first_name,
            last_name=last_name,
            email=f"{first_name.lower()}.{last_name.lower()}@{fake.safe_domain_name()}",
            age=random.randrange(12, 71),
            gender=gender,
            street_address=fake.street_address(),
            postal_code=location["postal_code"],
            city=location["city"],
            state=location["state"],
            country=location["country"],
            latitude=location["latitude"],
            longitude=location["longitude"],
            traffic_source=traffic_source,
            created_at=datetime.datetime.now(),
            updated_at=datetime.datetime.now(),
        )

    def update_address(
        self,
        *,
        country: str = "*",
        state: str = "*",
        postal_code: str = "*",
        fake: Faker,
    ) -> Self:
        current_data = dataclasses.asdict(self)
        updated_fields = {
            "street_address": fake.street_address(),
            **get_location(country=country, state=state, postal_code=postal_code),
        }
        merged_data = {**current_data, **updated_fields}
        return self.from_dict(merged_data)

    def __str__(self):
        return f"User(id={self.id}, name='{self.first_name} {self.last_name}', location='{self.country}|{self.state}|{self.city}', source='{self.traffic_source}')"

    @staticmethod
    def ddl(schema: str):
        return inspect.cleandoc(f"""
        CREATE TABLE IF NOT EXISTS {schema}.users (
            id              TEXT PRIMARY KEY,
            first_name      TEXT,
            last_name       TEXT,
            email           TEXT,
            age             INT,
            gender          TEXT,
            street_address  TEXT,
            postal_code     TEXT,
            city            TEXT,
            state           TEXT,
            country         TEXT,
            latitude        DOUBLE PRECISION,
            longitude       DOUBLE PRECISION,
            traffic_source  TEXT,
            created_at      TIMESTAMP WITHOUT TIME ZONE,
            updated_at      TIMESTAMP WITHOUT TIME ZONE
        );
        """)


@dataclasses.dataclass
class Order(ModelMixin):
    id: str
    user_id: str
    status: str
    num_of_items: int
    created_at: datetime.datetime
    updated_at: datetime.datetime
    returned_at: Optional[datetime.datetime]
    shipped_at: Optional[datetime.datetime]
    delivered_at: Optional[datetime.datetime]
    cancelled_at: Optional[datetime.datetime]

    @classmethod
    def new(cls, user: User, fake: Faker) -> Self:
        return cls(
            id=fake.uuid4(),
            user_id=user.id,
            status=OrderStatus.PROCESSING.value,
            num_of_items=fake.random_choices(
                OrderedDict(zip([1, 2, 3, 4], [0.7, 0.2, 0.05, 0.05])),
                1,
            )[0],
            created_at=datetime.datetime.now(),
            updated_at=datetime.datetime.now(),
            returned_at=None,
            shipped_at=None,
            delivered_at=None,
            cancelled_at=None,
        )

    def __str__(self):
        return f"Order(id={self.id}, user_id={self.user_id}, status='{self.status}', items={self.num_of_items})"

    def update_status(self, fake: Faker, return_probability: float = 0.02) -> Self:
        status_changed = False

        if self.status == OrderStatus.PROCESSING.value:
            # Transition from Processing to either Shipped or Cancelled
            new_status = fake.random_element(
                elements=OrderedDict(
                    [
                        (OrderStatus.SHIPPED.value, 0.95),
                        (OrderStatus.CANCELLED.value, 0.05),
                    ]
                )
            )
            if new_status == OrderStatus.SHIPPED.value:
                self.status = OrderStatus.SHIPPED.value
                self.shipped_at = datetime.datetime.now()
            else:
                self.status = OrderStatus.CANCELLED.value
                self.cancelled_at = datetime.datetime.now()
            status_changed = True

        elif self.status == OrderStatus.SHIPPED.value:
            # Deterministic transition from Shipped to Delivered
            self.status = OrderStatus.DELIVERED.value
            self.delivered_at = datetime.datetime.now()
            status_changed = True

        elif self.status == OrderStatus.DELIVERED.value:
            # Probabilistic transition from Delivered to Returned
            if random.random() < return_probability:
                self.status = OrderStatus.RETURNED.value
                self.returned_at = datetime.datetime.now()
                status_changed = True

        if status_changed:
            self.updated_at = datetime.datetime.now()
        return self

    @staticmethod
    def ddl(schema: str):
        return inspect.cleandoc(f"""
        CREATE TABLE IF NOT EXISTS {schema}.orders (
            id              TEXT PRIMARY KEY,
            user_id         TEXT,
            status          TEXT,
            num_of_items    INT,
            created_at      TIMESTAMP WITHOUT TIME ZONE,
            updated_at      TIMESTAMP WITHOUT TIME ZONE,
            returned_at     TIMESTAMP WITHOUT TIME ZONE,
            shipped_at      TIMESTAMP WITHOUT TIME ZONE,
            delivered_at    TIMESTAMP WITHOUT TIME ZONE,
            cancelled_at    TIMESTAMP WITHOUT TIME ZONE
        );
        """)


@dataclasses.dataclass
class OrderItem(ModelMixin):
    id: str
    order_id: str
    product_id: int
    status: str
    quantity: int
    sale_price: float
    created_at: datetime.datetime
    updated_at: datetime.datetime
    returned_at: Optional[datetime.datetime]
    shipped_at: Optional[datetime.datetime]
    delivered_at: Optional[datetime.datetime]
    cancelled_at: Optional[datetime.datetime]

    @classmethod
    def new(cls, order: Order, fake: Faker) -> Self:
        product_id = fake.random_element(PRODUCT_MAP.keys())
        return cls(
            id=fake.uuid4(),
            order_id=order.id,
            product_id=product_id,
            status=order.status,
            quantity=fake.random_element(range(1, 4)),
            sale_price=PRODUCT_MAP[product_id]["retail_price"],
            created_at=order.created_at,
            updated_at=order.updated_at,
            shipped_at=order.shipped_at,
            delivered_at=order.delivered_at,
            returned_at=order.returned_at,
            cancelled_at=order.cancelled_at,
        )

    def __str__(self):
        return f"OrderItem(id={self.id}, order_id={self.order_id}, product_id={self.product_id}, status={self.status}, quantity={self.quantity})"

    def update_status(self, order: Order) -> Self:
        self.status = order.status
        self.created_at = order.created_at
        self.updated_at = order.updated_at
        self.shipped_at = order.shipped_at
        self.delivered_at = order.delivered_at
        self.returned_at = order.returned_at
        self.cancelled_at = order.cancelled_at
        return self

    @staticmethod
    def ddl(schema: str):
        return inspect.cleandoc(f"""
        CREATE TABLE IF NOT EXISTS {schema}.order_items (
            id              TEXT PRIMARY KEY,
            order_id        TEXT,
            product_id      BIGINT,
            status          TEXT,
            quantity        INT,
            sale_price      DOUBLE PRECISION,
            created_at      TIMESTAMP WITHOUT TIME ZONE,
            updated_at      TIMESTAMP WITHOUT TIME ZONE,
            shipped_at      TIMESTAMP WITHOUT TIME ZONE,
            delivered_at    TIMESTAMP WITHOUT TIME ZONE,
            returned_at     TIMESTAMP WITHOUT TIME ZONE,
            cancelled_at    TIMESTAMP WITHOUT TIME ZONE
        );
        """)


@dataclasses.dataclass
class Event(ModelMixin):
    id: str
    user_id: Optional[str]
    sequence_number: int
    session_id: str
    ip_address: str
    city: str
    state: str
    postal_code: str
    browser: str
    traffic_source: str
    uri: str
    event_type: str
    created_at: datetime.datetime

    @staticmethod
    def new(
        user: Optional[User],
        order_item: Optional[OrderItem],
        event_category: str,
        fake: Faker,
    ) -> List[Self]:
        if event_category in ["purchase", "return", "cancel"]:
            assert order_item is not None
            user_id = user.id
            city = user.city
            state = user.state
            postal_code = user.postal_code
            product_id = order_item.product_id
            order_item_id = order_item.id
            if event_category == "purchase":
                created_at = order_item.created_at
                event_types = list(
                    set(fake.random_choices(["home", "department", "product"], 3))
                ) + ["cart", "purchase"]
            else:
                created_at = datetime.datetime.now()
                event_types = ["product", "cart", event_category]
        elif event_category == "ghost":
            location = get_location()
            user_id = None
            city = location["city"]
            state = location["state"]
            postal_code = location["postal_code"]
            product_id = fake.random_element(PRODUCT_MAP.keys())
            order_item_id = None
            created_at = datetime.datetime.now()
            event_types = fake.random_elements(
                [
                    "home",
                    "department",
                    "category",
                    "product",
                    "cancel",
                    "purchase",
                    "return",
                ],
                length=fake.random_element(range(3, 7)),
            )
        else:
            raise RuntimeError(
                f"Unsupported event category: '{event_category}'. Allowed categories are: {', '.join(sorted(['purchase', 'cancel', 'ghost']))}."
            )
        session_id = fake.uuid4()
        ip_address = fake.ipv4()
        browser = fake.random_choices(
            OrderedDict(
                zip(
                    ["IE", "Edge", "Chrome", "Safari", "Firefox", "Other"],
                    [0.05, 0.1, 0.45, 0.2, 0.15, 0.05],
                )
            ),
            1,
        )[0]
        traffic_source = fake.random_choices(
            OrderedDict(
                zip(
                    ["Email", "Adwords", "Organic", "YouTube", "Facebook"],
                    [0.45, 0.3, 0.05, 0.1, 0.1],
                )
            ),
            1,
        )[0]
        events = [
            Event(
                id=fake.uuid4(),
                user_id=user_id,
                sequence_number=idx + 1,
                session_id=session_id,
                ip_address=ip_address,
                city=city,
                state=state,
                postal_code=postal_code,
                browser=browser,
                traffic_source=traffic_source,
                uri=Event._generate_uri(event_type, order_item_id, product_id),
                event_type=event_type,
                created_at=created_at
                - Event._calculate_event_delay(len(event_types), idx, fake),
            )
            for idx, event_type in enumerate(event_types)
        ]
        return events

    def __str__(self):
        return f"Event(id={self.id}, is_ghost={self.user_id is None}, sequence_number={self.sequence_number}, event_type={self.event_type}, created_at={self.created_at})"

    @staticmethod
    def _generate_uri(event_type: str, item_id: Optional[str], product_id: int) -> str:
        if event_type == "product":
            return f"/{event_type}/{product_id}"
        elif event_type == "department":
            department = PRODUCT_MAP[product_id]["department"]
            category = PRODUCT_MAP[product_id]["category"]
            return f"/{event_type}/{department}/category/{category}"
        elif event_type in ["cancel", "return"]:
            return (
                f"/{event_type}/item/{item_id}"
                if item_id is not None
                else f"/{event_type}"
            )
        else:
            return f"/{event_type}"

    @staticmethod
    def _calculate_event_delay(
        num_events: int, idx: int, fake: Faker
    ) -> datetime.timedelta:
        if num_events == idx + 1:
            return datetime.timedelta(seconds=0)
        base_delay = (num_events - idx + 1) * 20
        jitter = fake.random_element(range(1, 10))
        return datetime.timedelta(seconds=base_delay + jitter)

    @staticmethod
    def ddl(schema: str):
        return inspect.cleandoc(f"""
        CREATE TABLE IF NOT EXISTS {schema}.events (
            id                  TEXT PRIMARY KEY,
            user_id             TEXT,
            sequence_number     INT,
            session_id          TEXT,
            ip_address          TEXT,
            city                TEXT,
            state               TEXT,
            postal_code         TEXT,
            browser             TEXT,
            traffic_source      TEXT,
            uri                 TEXT,
            event_type          TEXT,
            created_at          TIMESTAMP WITHOUT TIME ZONE
        );
        """)
