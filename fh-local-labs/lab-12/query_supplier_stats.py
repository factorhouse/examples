import os
import json
import urllib3
import inspect
import dataclasses
import argparse
import time
from datetime import datetime, timezone

BASE_URL = os.getenv("BASE_URL", "http://localhost:18099")
QUERY_ENDPOINT = f"{BASE_URL}/query/sql"
TABLE_NAME = "orders"
WINDOW_INTERVAL_SECONDS = 5
DML_STMT = inspect.cleandoc("""
    SELECT
        window_start,
        window_end,
        supplier,
        total_price,
        count
    FROM (
        SELECT
            *
            , ROW_NUMBER() OVER (PARTITION BY supplier ORDER BY window_start DESC) AS row_num
        FROM (
            SELECT
                FLOOR(bid_time / 5000) * 5000 AS window_start,
                FLOOR(bid_time / 5000) * 5000 + 5000 AS window_end,
                supplier,
                ROUND(SUM(CAST(price AS DOUBLE)), 2) AS total_price,
                COUNT(*) AS count
            FROM orders
            WHERE bid_time >= (NOW() - 50000)
            GROUP BY FLOOR(bid_time / 5000), supplier
            ORDER BY window_start DESC, supplier
        )
    )
    WHERE row_num = 1""")

http = urllib3.PoolManager()


@dataclasses.dataclass
class SupplierStats:
    window_start: str
    window_end: str
    supplier: str
    total_price: float
    count: int

    @classmethod
    def from_list(cls, lst: list):
        window_start = datetime.fromtimestamp(lst[0] / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        window_end = datetime.fromtimestamp(lst[1] / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        return cls(window_start, window_end, lst[2], lst[3], lst[4])


def execute_pinot_query(query: str):
    """Executes a SQL query against Pinot and returns results."""
    payload = {"sql": query, "queryOptions": "useMultistageEngine=true"}
    try:
        response = http.request(
            "POST",
            QUERY_ENDPOINT,
            body=json.dumps(payload),
            headers={"Content-Type": "application/json"},
        )
        if response.status == 200:
            response_data = json.loads(response.data.decode("utf-8"))
            if (
                response_data.get("exceptions")
                and response_data["exceptions"][0].get("errorCode") != 0
            ):
                print(f"Pinot query execution error. Status: {response.status}")
                print("Response:", json.dumps(response_data, indent=2))
                return None
            return response_data
        else:
            print(f"Error executing query. Status: {response.status}")
            print("Response:", response.data.decode())
            return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def get_supplier_stats(query: str):
    """
    Queries supplier statistics.
    """
    query_results = execute_pinot_query(query)

    if query_results and query_results.get("resultTable"):
        results = query_results["resultTable"]["rows"]
        if not results:
            print("No data found for the specified time range.")
            return

        for row_data in results:
            print(SupplierStats.from_list(row_data))

    else:
        print("Failed to fetch supplier statistics or no results returned.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run SQL statements on Flink SQL Gateway."
    )
    parser.add_argument(
        "--max-seconds",
        "-m",
        type=int,
        default=-1,
        help="Max seconds (if > 0) to collect streaming results (default: -1)",
    )
    args = parser.parse_args()

    start_time = time.time()
    try:
        while True:
            if args.max_seconds > 0 and (time.time() - start_time) >= args.max_seconds:
                print("Max time reached. Stopping result fetch.")
                break
            get_supplier_stats(DML_STMT)
            time.sleep(2)
    except KeyboardInterrupt:
        print("🛑 Interrupted by user.")
