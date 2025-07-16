import argparse
import time
import logging

import pandas as pd

from src.models import User
from src.utils import create_connection, insert_to_db, Connection, generate_from_csv

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


def write_dynamic_data(
    conn: Connection, schema_name: str = "ecommerce", if_exists: bool = "replace"
):
    tbl_map = {
        "users": [],
        "orders": [],
        "order_items": [],
        "inventory_items": [],
        "events": [],
    }
    user = User()
    logging.info(f"start to create user events - user id: {user.id}")
    tbl_map["users"].extend([user.asdict(["orders"])])
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

    for tbl in tbl_map:
        df = pd.DataFrame(tbl_map[tbl])
        if len(df) > 0:
            logging.info(f"{if_exists} records, table - {tbl}, # records - {len(df)}")
            insert_to_db(
                df=df,
                tbl_name=tbl,
                schema_name=schema_name,
                conn=conn,
                if_exists=if_exists,
            )
        else:
            logging.info(
                f"skip records as no user event, table - {tbl}, # records - {len(df)}"
            )


def write_static_data(
    conn: Connection, schema_name: str = "ecommerce", if_exists: bool = "replace"
):
    tbl_map = {
        "products": generate_from_csv("products.csv"),
        "dist_centers": generate_from_csv("distribution_centers.csv"),
    }
    for tbl in tbl_map:
        df = pd.DataFrame(tbl_map[tbl])
        if len(df) > 0:
            logging.info(f"{if_exists} records, table - {tbl}, # records - {len(df)}")
            insert_to_db(
                df=df,
                tbl_name=tbl,
                schema_name=schema_name,
                conn=conn,
                if_exists=if_exists,
            )
        else:
            logging.info(f"skip writing, table - {tbl}, # records - {len(df)}")


def main(wait_for: float, max_iter: int, if_exists: str):
    conn = create_connection()
    write_static_data(conn=conn, if_exists="replace")
    curr_iter = 0
    while True:
        write_dynamic_data(conn=conn, if_exists=if_exists)
        time.sleep(wait_for)
        curr_iter += 1
        if max_iter > 0 and curr_iter >= max_iter:
            logging.info(f"stop generating records after {curr_iter} iterations")
            break


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Generate theLook eCommerce data...")

    parser = argparse.ArgumentParser(description="Generate theLook eCommerce data")
    parser.add_argument(
        "--if_exists",
        "-i",
        type=str,
        default="append",
        choices=["fail", "replace", "append"],
        help="The time to wait before generating new user records",
    )
    parser.add_argument(
        "--wait_for",
        "-w",
        type=float,
        default=1,
        help="The time to wait before generating new user records",
    )
    parser.add_argument(
        "--max_iter",
        "-m",
        type=int,
        default=-1,
        help="The maxium number of iterations to generate user records",
    )
    args = parser.parse_args()
    logging.info(args)
    main(args.wait_for, args.max_iter, if_exists=args.if_exists)
