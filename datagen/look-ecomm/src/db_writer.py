import logging
import pandas as pd
import inspect
from sqlalchemy import Connection, create_engine, text

from src.models import (
    User,
    Order,
    OrderItem,
    InventoryItem,
    Event,
    Product,
    GhostEvents,
)
from src.utils import generate_from_csv

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)

# Headers to be excluded from the final data dictionary
extraneous_headers = [
    "event_type",
    "ip_address",
    "browser",
    "traffic_source",
    "session_id",
    "sequence_number",
    "uri",
    "is_sold",
]


class DataWriter:
    """
    Handles the creation and writing of data to the database.
    """

    def __init__(self, conn: Connection, schema_name: str):
        self.conn = conn
        self.schema_name = schema_name
        # A map of all tables, categorized for static and dynamic data
        self.all_tbl_map = {
            "dynamic": {
                "data": {
                    "users": [],
                    "orders": [],
                    "order_items": [],
                    "inventory_items": [],
                    "events": [],
                },
                "schema": {
                    "users": User.ddl(self.schema_name),
                    "orders": Order.ddl(self.schema_name),
                    "order_items": OrderItem.ddl(self.schema_name),
                    "inventory_items": InventoryItem.ddl(self.schema_name),
                    "events": Event.ddl(self.schema_name),
                },
            },
            "ghost": {"data": {"events": []}},
            "static": {
                "data": {
                    "products": generate_from_csv("products.csv"),
                    "dist_centers": generate_from_csv("distribution_centers.csv"),
                },
                "schema": {
                    "products": Product.ddl(self.schema_name),
                    "dist_centers": inspect.cleandoc(f"""
                        CREATE TABLE IF NOT EXISTS {self.schema_name}.dist_centers (
                            id BIGINT PRIMARY KEY,
                            name TEXT,
                            latitude DOUBLE PRECISION,
                            longitude DOUBLE PRECISION
                        );
                    """),
                },
            },
            "heartbeat": {
                "schema": {
                    "heartbeat": inspect.cleandoc("""
                        CREATE TABLE IF NOT EXISTS demo.heartbeat (
                            id INT PRIMARY KEY,
                            ts TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
                        );
                    """)
                }
            },
        }

    def _execute_ddl(self, ddl_statement: str):
        """Helper to execute a DDL statement within a transaction."""
        with self.conn.begin():
            self.conn.execute(text(ddl_statement))

    def _insert_to_db(self, df: pd.DataFrame, tbl_name: str, if_exists: str):
        """A private helper method to insert a DataFrame into the database."""
        if not df.empty:
            logging.info(
                f"Writing {len(df)} records to table '{tbl_name}' with if_exists='{if_exists}'."
            )
            df.to_sql(
                name=tbl_name,
                schema=self.schema_name,
                con=self.conn,
                index=False,
                if_exists=if_exists,
            )
        else:
            logging.info(f"Skipping write for table '{tbl_name}' as there is no data.")

    def setup_database_schema(self):
        """
        Creates all necessary tables using predefined DDL. This should be run once at startup.
        """
        schemas = [
            {"tbl": tbl_name, "ddl": ddl_str}
            for category_dict in self.all_tbl_map.values()
            if "schema" in category_dict
            for tbl_name, ddl_str in category_dict["schema"].items()
        ]
        for schema in schemas:
            logging.info(f"Creating table '{self.schema_name}.{schema['tbl']}'.")
            self._execute_ddl(schema["ddl"])

    def write_static_data(self, if_exists: str = "replace"):
        """Writes static data (products, distribution centers) to the database."""
        tbl_map = self.all_tbl_map["static"]["data"]
        for tbl, data in tbl_map.items():
            df = pd.DataFrame(data)
            self._insert_to_db(df=df, tbl_name=tbl, if_exists=if_exists)

    def write_dynamic_data(self, if_exists: str = "append"):
        """Generates and writes user-centric dynamic data (users, orders, etc.)."""
        tbl_map = {k: [] for k in self.all_tbl_map["dynamic"]["data"]}

        user = User()
        logging.info(f"Creating user events for user_id: {user.id}")
        tbl_map["users"].append(user.asdict(["orders"]))

        orders = user.orders
        tbl_map["orders"].extend([o.asdict(["order_items"]) for o in orders])

        for order in orders:
            order_items = order.order_items
            tbl_map["order_items"].extend(
                [
                    o.asdict(["events", "inventory_items"] + extraneous_headers)
                    for o in order_items
                ]
            )
            for order_item in order_items:
                tbl_map["inventory_items"].extend(
                    [i.asdict() for i in order_item.inventory_items]
                )
                tbl_map["events"].extend([e.asdict() for e in order_item.events])

        for tbl, data in tbl_map.items():
            df = pd.DataFrame(data)
            self._insert_to_db(df=df, tbl_name=tbl, if_exists=if_exists)

    def write_ghost_data(self, if_exists: str = "append"):
        """Generates and writes 'ghost' events (events not tied to a user)."""
        tbl_map = {k: [] for k in self.all_tbl_map["ghost"]["data"]}

        ghost_event = GhostEvents()
        if ghost_event.events:
            session_id = ghost_event.events[0].get("session_id", "N/A")
            logging.info(
                f"Creating ghost event session {session_id} with {len(ghost_event.events)} events."
            )
            tbl_map["events"].extend(ghost_event.events)
            for tbl, data in tbl_map.items():
                df = pd.DataFrame(data)
                self._insert_to_db(df=df, tbl_name=tbl, if_exists=if_exists)
        else:
            logging.info("GhostEvents created, but no events were generated.")


def create_db_connection(
    user: str, password: str, host: str, db_name: str, echo: bool = False
) -> Connection:
    """
    Establishes and returns a SQLAlchemy connection.
    """
    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}/{db_name}", echo=echo
    )
    return engine.connect()
