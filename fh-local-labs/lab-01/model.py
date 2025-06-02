import uuid
import dataclasses
from datetime import datetime, timezone

from confluent_kafka.serialization import SerializationContext
from faker import Faker
import faker_commerce


faker = Faker()
faker.add_provider(faker_commerce.Provider)


def create_item(version: int = 1):
    item = {
        "order_id": str(uuid.uuid4()),
        "bid_time": round(datetime.now(timezone.utc).timestamp() * 1000),
        "price": faker.pyfloat(
            right_digits=2, min_value=1, max_value=150, positive=True
        ),
        "item": faker.ecommerce_name(),
        "supplier": faker.random_element(
            ["Alice", "Bob", "Carol", "Alex", "Joe", "James", "Jane", "Jack"]
        ),
    }
    if version > 1:
        item = {**item, "is_premium": faker.pybool()}
    return item


@dataclasses.dataclass
class OrderBase:
    order_id: str
    bid_time: datetime  # logicalType: timestamp-millis
    price: float
    item: str
    supplier: str

    @classmethod
    def from_dict(cls, d: dict, ctx: SerializationContext):
        bid_time = d.get("bid_time")
        if bid_time is not None:
            d["bid_time"] = datetime.fromtimestamp(bid_time / 1000, tz=timezone.utc)
        return cls(**d)

    def to_dict(self, ctx: SerializationContext):
        return dataclasses.asdict(self)

    @staticmethod
    def _make_schema(fields: list[dict]):
        schema = {
            "namespace": "io.factorhouse.avro",
            "type": "record",
            "name": "Order",
            "fields": fields,
        }
        import json

        return json.dumps(schema, indent=2)


@dataclasses.dataclass
class OrderV1(OrderBase):
    @classmethod
    def create(cls):
        return cls(**create_item(version=1))

    @staticmethod
    def schema_str():
        return OrderBase._make_schema(
            [
                {"name": "order_id", "type": "string"},
                {"name": "bid_time", "type": "long", "logicalType": "timestamp-millis"},
                {"name": "price", "type": "double"},
                {"name": "item", "type": "string"},
                {"name": "supplier", "type": "string"},
            ]
        )


@dataclasses.dataclass
class OrderV2(OrderBase):
    is_premium: bool

    @classmethod
    def create(cls):
        return cls(**create_item(version=2))

    @staticmethod
    def schema_str():
        return OrderBase._make_schema(
            [
                {"name": "order_id", "type": "string"},
                {"name": "bid_time", "type": "long", "logicalType": "timestamp-millis"},
                {"name": "price", "type": "double"},
                {"name": "item", "type": "string"},
                {"name": "supplier", "type": "string"},
                {"name": "is_premium", "type": "boolean", "default": False},
            ]
        )
