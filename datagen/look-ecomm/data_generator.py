import argparse
import asyncio
import random
import logging

from src.db_writer import DataWriter, create_db_connection
from src.utils import get_topic_names, create_topics_if_not_exists

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)


async def run_simulation(args: argparse.Namespace):
    """
    Runs the main asynchronous loop for data generation and injection.
    """
    logging.info(f"Starting data generator with configuration: {args}")

    if args.create_topic:
        logging.info("Topic creation is enabled.")
        _dummy_writer = DataWriter(conn=None, schema_name=args.schema)
        topic_names = get_topic_names(
            args.topic_prefix, args.schema, _dummy_writer.all_tbl_map
        )
        create_topics_if_not_exists(args.bootstrap_servers, topic_names)
    else:
        logging.info("Skipping topic creation as per --no-create-topic flag.")

    conn = create_db_connection(args.user, args.password, args.host, args.db)
    writer = DataWriter(conn=conn, schema_name=args.schema)
    logging.info("Setting up database schema...")
    writer.setup_database_schema()
    logging.info("Writing static data...")
    writer.write_static_data(if_exists="replace")

    curr_iter = 0
    try:
        while True:
            wait_time = random.expovariate(args.avg_qps)
            await asyncio.sleep(wait_time)

            if random.random() < args.ghost_ratio:
                await asyncio.to_thread(
                    writer.write_ghost_data, if_exists=args.if_exists
                )
            else:
                await asyncio.to_thread(
                    writer.write_dynamic_data, if_exists=args.if_exists
                )

            curr_iter += 1
            if 0 < args.max_iter <= curr_iter:
                logging.info(
                    f"Stopping data generation after reaching {curr_iter} iterations."
                )
                break
    except asyncio.CancelledError:
        logging.warning("Data generation task was cancelled.")
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")


def main():
    """
    Parses command-line arguments and starts the data generation simulation.
    """
    # fmt: off
    parser = argparse.ArgumentParser(description="Generate theLook eCommerce data")
    # --- General Arguments ---
    parser.add_argument("--avg-qps", type=float, default=10.0, help="Average events per second.")
    parser.add_argument(
        "--if_exists", type=str, default="append", choices=["fail", "replace", "append"], 
        help="Action if the table already exists: 'fail', 'replace', or 'append'.")
    parser.add_argument("--max_iter", type=int, default=-1, help="Max number of iterations. Default -1 for infinite.")
    parser.add_argument(
        "--ghost_ratio", "-g", type=float, default=0.05, 
        help="The proportion of generated events that should be 'ghost' events (without a user).")
    # --- Database Arguments ---
    parser.add_argument("--host", default="localhost", help="Database host.")
    parser.add_argument("--user", default="db_user", help="Database user.")
    parser.add_argument("--password", default="db_password", help="Database password.")
    parser.add_argument("--db", default="fh_dev", help="Database name.")
    parser.add_argument("--schema", default="demo", help="Database schema.")
    # --- Kafka Arguments ---
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092", help="Bootstrap server addresses.")
    parser.add_argument("--topic-prefix", type=str, default="ecomm", help="Kafka topic prifix.")
    parser.add_argument(
        "--create-topic", action=argparse.BooleanOptionalAction, default=True, 
        help="Enable or disable automatic topic creation (e.g., --no-create-topic).")
    # fmt: on

    args = parser.parse_args()
    logging.info(args)

    try:
        asyncio.run(run_simulation(args))
    except KeyboardInterrupt:
        logging.warning("Data generator stopped by user.")


if __name__ == "__main__":
    main()
