import argparse
import asyncio
import random
import logging

from faker import Faker
from sqlalchemy.exc import SQLAlchemyError, OperationalError

from src.db_writer import DataWriter
from src.models import User, Order, OrderItem, Event, OrderStatus, EventCategory
from src.utils import generate_from_csv, create_topics_if_not_exists

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)


class TheLookECommSimulator:
    """
    Manages the state and execution of the e-commerce data generation simulation.
    """

    def __init__(self, args: argparse.Namespace, fake: Faker):
        """Initializes the simulator and the DataWriter."""
        self.args = args
        self.fake = fake
        self.writer = DataWriter(
            args.user,
            args.password,
            args.host,
            args.db_name,
            args.schema,
            args.batch_size,
            args.echo,
        )
        self.consecutive_db_errors = 0
        self.max_consecutive_errors = 3

    async def initialize(self):
        """
        Performs the one-time setup for the simulation.
        This includes creating database tables, loading initial data, and creating Kafka topics.
        """
        try:
            logging.info("Setting up database schema...")
            await asyncio.to_thread(self.writer.create_tables_if_not_exists)

            logging.info("Writing initial data...")
            init_users = [
                User.new(
                    country=self.args.country,
                    state=self.args.state,
                    postal_code=self.args.postal_code,
                    fake=self.fake,
                )
                for _ in range(self.args.init_num_users)
            ]

            logging.info(f"Writing {len(init_users)} initial users...")
            await asyncio.to_thread(
                self.writer.upsert, table="users", data=init_users, conflict_keys=["id"]
            )

            logging.info("Writing initial products...")
            await asyncio.to_thread(
                self.writer.upsert,
                table="products",
                data=generate_from_csv("products.csv"),
                conflict_keys=["id"],
            )

            logging.info("Writing initial distribution centers...")
            await asyncio.to_thread(
                self.writer.upsert,
                table="dist_centers",
                data=generate_from_csv("distribution_centers.csv"),
                conflict_keys=["id"],
            )

            if self.args.create_topic:
                logging.info("Creating topics...")
                topic_names = [
                    f"{self.args.topic_prefix}.{self.args.schema}.{tbl}"
                    for tbl in self.writer.get_all_tables()
                ]
                create_topics_if_not_exists(self.args.bootstrap_servers, topic_names)
            else:
                logging.info("Skipping topic creation...")

            logging.info("Initialization successful.")
            return True
        except (SQLAlchemyError, OperationalError) as e:
            logging.critical(
                f"A fatal error occurred during initial setup. Cannot continue. Error: {e}"
            )
            return False

    async def _simulate_purchases(self):
        """Generates and writes a new purchase transaction, the primary simulation event."""
        # Select a random user
        user_list = await asyncio.to_thread(
            self.writer.select, table="users", order_by="RANDOM()", limit=1
        )
        if not user_list:
            logging.warning(
                "A random user is not retrieved. Skipping a purchase event generation."
            )
            return
        random_user = User.from_dict(user_list[0])

        # Potentially update the user's address
        if (
            self.args.user_update_prob > 0
            and random.random() < self.args.user_update_prob
        ):
            logging.info("Updating a user address...")
            random_user = random_user.update_address(
                country=self.args.country,
                state=self.args.state,
                postal_code=self.args.postal_code,
                fake=self.fake,
            )
            await asyncio.to_thread(
                self.writer.upsert,
                table="users",
                data=[random_user],
                conflict_keys=["id"],
            )

        # Generate the order and its associated events
        order = Order.new(user=random_user, fake=self.fake)
        order_items = []
        purchase_events = []
        for _ in range(order.num_of_items):
            order_item = OrderItem.new(order=order, fake=self.fake)
            purchase_events.extend(
                Event.new(
                    user=random_user,
                    order_item=order_item,
                    event_category=EventCategory.PURCHASE.value,
                    fake=self.fake,
                )
            )
            order_items.append(order_item)

        # Write all purchase-related data concurrently
        await asyncio.gather(
            asyncio.to_thread(
                self.writer.upsert, table="orders", data=[order], conflict_keys=["id"]
            ),
            asyncio.to_thread(
                self.writer.upsert,
                table="order_items",
                data=order_items,
                conflict_keys=["id"],
            ),
            asyncio.to_thread(
                self.writer.upsert,
                table="events",
                data=purchase_events,
                conflict_keys=["id"],
            ),
        )

    def _simulate_order_update(self):
        """Synchronous helper containing the logic for updating an order. To be run in a thread."""
        random_order_list = self.writer.select(
            table="orders", order_by="RANDOM()", limit=1
        )
        if not random_order_list:
            logging.warning(
                "A random order is not retrieved. Skipping random order event upate."
            )
            return

        random_order = Order.from_dict(random_order_list[0])
        random_user = User.from_dict(
            self.writer.select(
                table="users",
                where_clause="id = :uid",
                where_params={"uid": random_order.user_id},
            )[0]
        )
        random_items = [
            OrderItem.from_dict(d)
            for d in self.writer.select(
                table="order_items",
                where_clause="order_id = :oid",
                where_params={"oid": random_order.id},
            )
        ]

        random_order.update_status(fake=self.fake)
        updated_items = [
            item.update_status(order=random_order) for item in random_items
        ]

        random_events = []
        for item in updated_items:
            event_category = None
            if item.status == OrderStatus.CANCELLED.value:
                event_category = EventCategory.CANCEL.value
            elif item.status == OrderStatus.RETURNED.value:
                event_category = EventCategory.RETURN.value

            if event_category:
                random_events.extend(
                    Event.new(
                        user=random_user,
                        order_item=item,
                        event_category=event_category,
                        fake=self.fake,
                    )
                )

        self.writer.upsert(table="orders", data=[random_order], conflict_keys=["id"])
        self.writer.upsert(
            table="order_items", data=updated_items, conflict_keys=["id"]
        )
        if random_events:
            self.writer.upsert(table="events", data=random_events, conflict_keys=["id"])

    async def _simulate_side_tasks(self):
        """Runs secondary simulation events based on their respective probabilities."""
        side_tasks = []

        if (
            self.args.user_create_prob > 0
            and random.random() < self.args.user_create_prob
        ):
            logging.info("Creating a new user...")
            new_user = User.new(
                country=self.args.country,
                state=self.args.state,
                postal_code=self.args.postal_code,
                fake=self.fake,
            )
            side_tasks.append(
                asyncio.to_thread(
                    self.writer.upsert,
                    table="users",
                    data=[new_user],
                    conflict_keys=["id"],
                )
            )

        if (
            self.args.ghost_create_prob > 0
            and random.random() < self.args.ghost_create_prob
        ):
            logging.info("Creating a ghost event...")
            ghost_events = Event.new(
                user=None,
                order_item=None,
                event_category=EventCategory.GHOST.value,
                fake=self.fake,
            )
            side_tasks.append(
                asyncio.to_thread(
                    self.writer.upsert,
                    table="events",
                    data=ghost_events,
                    conflict_keys=["id"],
                )
            )

        if (
            self.args.order_update_prob > 0
            and random.random() < self.args.order_update_prob
        ):
            logging.info("Update an order status...")
            side_tasks.append(asyncio.to_thread(self._simulate_order_update))

        if side_tasks:
            await asyncio.gather(*side_tasks)

    async def run(self):
        """The main simulation loop, which orchestrates primary and side tasks."""
        current_iteration = 0
        while True:
            if 0 < self.args.max_iter <= current_iteration:
                logging.info(
                    f"Stopping data generation after reaching {current_iteration} iterations."
                )
                break

            try:
                wait_time = random.expovariate(self.args.avg_qps)
                await asyncio.sleep(wait_time)

                await self._simulate_purchases()
                await self._simulate_side_tasks()
            except (SQLAlchemyError, OperationalError) as e:
                self.consecutive_db_errors += 1
                logging.warning(
                    f"A database error occurred. Consecutive error count: {self.consecutive_db_errors}/{self.max_consecutive_errors}. "
                    f"Error: {e}"
                )
                if self.consecutive_db_errors >= self.max_consecutive_errors:
                    logging.critical(
                        f"Exceeded max consecutive DB error limit ({self.max_consecutive_errors}). "
                        "The simulation will now stop."
                    )
                    break
                await asyncio.sleep(5)
            else:
                if self.consecutive_db_errors > 0:
                    logging.info(
                        "Database operation successful. Resetting consecutive error counter."
                    )
                self.consecutive_db_errors = 0
                current_iteration += 1
        logging.info("Simulation loop has finished.")

    async def close(self):
        """Gracefully closes the database connection."""
        logging.info("Closing database connection...")
        if self.writer.conn and not self.writer.conn.closed:
            await asyncio.to_thread(self.writer.close)
        logging.info("Database connection closed.")


async def run_simulation(args: argparse.Namespace):
    """Orchestrates the creation, initialization, and execution of the simulator."""
    simulator = TheLookECommSimulator(args, Faker())
    try:
        if await simulator.initialize():
            await simulator.run()
    except asyncio.CancelledError:
        logging.warning("Data generation task was cancelled.")
    finally:
        await simulator.close()


def main():
    """
    Parses command-line arguments and starts the data generation simulation.
    """
    # fmt: off
    parser = argparse.ArgumentParser(description="Generate theLook eCommerce data")
    ## --- General Arguments ---
    parser.add_argument("--avg-qps", type=float, default=5.0, help="Average events per second.")
    parser.add_argument("--max-iter", type=int, default=-1, help="Max number of successful iterations. Default -1 for infinite.")
    ## --- User Arguments ---
    parser.add_argument("--init-num-users", type=int, default=1000, help="Initial number of users to create.")
    parser.add_argument("--country", default="*", help="User country.")
    parser.add_argument("--state", default="*", help="User state.")
    parser.add_argument("--postal-code", default="*", help="User postal code.")
    parser.add_argument("--user-create-prob", type=float, default=0.05, help="Probability of generating a new user. Default is 0.05. Set to 0 to disable.")
    parser.add_argument("--user-update-prob", type=float, default=0.5, help="Probability of updating a user address. Default is 0. Set to 0 to disable.")
    ## --- Order Arguments ---
    parser.add_argument("--order-update-prob", type=float, default=0.5, help="Probability of updating an order status. Default is 0. Set to 0 to disable.")
    ## --- Ghost Event Arguments ---
    parser.add_argument("--ghost-create-prob", type=float, default=0.5, help="Probability of generating a ghost event. Default is 0.05. Set to 0 to disable.")
    ## --- Database Arguments ---
    parser.add_argument("--host", default="localhost", help="Database host.")
    parser.add_argument("--user", default="db_user", help="Database user.")
    parser.add_argument("--password", default="db_password", help="Database password.")
    parser.add_argument("--db-name", default="fh_dev", help="Database name.")
    parser.add_argument("--schema", default="demo", help="Database schema.")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--echo", action=argparse.BooleanOptionalAction, default=False)
    ## --- Kafka Arguments ---
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092", help="Bootstrap server addresses.")
    parser.add_argument("--topic-prefix", type=str, default="ecomm", help="Kafka topic prefix.")
    parser.add_argument("--create-topic", action=argparse.BooleanOptionalAction, default=False, help="Enable or disable automatic topic creation.")
    # fmt: on

    args = parser.parse_args()
    logging.info(args)

    try:
        asyncio.run(run_simulation(args))
    except KeyboardInterrupt:
        logging.warning("Data generator stopped by user.")
    except Exception as e:
        logging.critical(f"An unexpected top-level error occurred: {e}", exc_info=True)
    finally:
        logging.info("Application shutdown complete.")


if __name__ == "__main__":
    main()
