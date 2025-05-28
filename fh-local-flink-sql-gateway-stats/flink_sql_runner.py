import os
import json
import urllib3
import time
import inspect
import argparse
import dataclasses


@dataclasses.dataclass
class SupplierStats:
    window_start: str
    window_end: str
    supplier: str
    total_price: float
    count: int

    @classmethod
    def from_list(cls, lst: list):
        return cls(lst[0], lst[1], lst[2], lst[3], lst[4])


def get_session_handle(http: urllib3.PoolManager, base_url: str):
    resp = http.request("POST", f"{base_url}/v1/sessions")
    data = json.loads(resp.data.decode())
    session_handle = data.get("sessionHandle")
    if not session_handle:
        raise RuntimeError("Failed to create session.")
    print(f"🆔 Session Handle: {session_handle}")
    return session_handle


def get_operation_handle(
    http: urllib3.PoolManager, base_url: str, session_handle: str, statement: str
):
    resp = http.request(
        "POST",
        f"{base_url}/v1/sessions/{session_handle}/statements/",
        body=json.dumps({"statement": statement}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    data = json.loads(resp.data.decode())
    operation_handle = data.get("operationHandle")
    if not operation_handle:
        raise RuntimeError(f"Failed to get operation handle for:\n{statement}")
    return operation_handle


def execute_statement(
    http: urllib3.PoolManager, base_url: str, session_handle: str, statement: str
):
    operation_handle = get_operation_handle(http, base_url, session_handle, statement)
    return operation_handle


def query_results(
    http: urllib3.PoolManager,
    base_url: str,
    session_handle: str,
    operation_handle: str,
    max_seconds: int,
):
    result_url = f"{base_url}/v1/sessions/{session_handle}/operations/{operation_handle}/result/0"
    start_time = time.time()
    while result_url:
        if max_seconds > 0 and (time.time() - start_time) >= max_seconds:
            print("Max time reached. Stopping result fetch.")
            break
        try:
            resp = http.request("GET", result_url)
            payload = json.loads(resp.data.decode())
            if payload.get("resultType") == "PAYLOAD" and payload.get("isQueryResult"):
                data = payload["results"]["data"]
                for r in data:
                    print(SupplierStats.from_list(r["fields"]))
            next_uri = payload.get("nextResultUri")
            result_url = f"{base_url}{next_uri}" if next_uri else None
        except Exception as e:
            print(f"Error: {e}")

        time.sleep(1)


def cancel_operation(
    http: urllib3.PoolManager,
    base_url: str,
    session_handle: str,
    operation_handle: str,
):
    resp = http.request(
        "DELETE",
        f"{base_url}/v1/sessions/{session_handle}/operations/{operation_handle}/close",
    )
    if resp.status == 200:
        print("✅ Operation canceled.")
    else:
        print(f"Failed to cancel operation. Status: {resp.status}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run SQL statements on Flink SQL Gateway."
    )
    parser.add_argument(
        "--max-seconds",
        type=int,
        default=-1,
        help="Max seconds (if > 0) to collect streaming results (default: -1)",
    )
    args = parser.parse_args()

    HTTP = urllib3.PoolManager()
    BASE_URL = os.getenv("BASE_URL", "http://localhost:9090")
    DDL_STMT = inspect.cleandoc("""
        CREATE TABLE orders (
            order_id     STRING,
            item         STRING,
            price        STRING,
            supplier     STRING,
            bid_time     TIMESTAMP(3),
            WATERMARK FOR bid_time AS bid_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'orders',
            'properties.bootstrap.servers' = 'kafka-1:19092',
            'format' = 'avro-confluent',
            'avro-confluent.schema-registry.url' = 'http://schema:8081',
            'avro-confluent.basic-auth.credentials-source' = 'USER_INFO',
            'avro-confluent.basic-auth.user-info' = 'admin:admin',
            'avro-confluent.schema-registry.subject' = 'orders-value',
            'scan.startup.mode' = 'earliest-offset'
        );
    """)

    DML_STMT = inspect.cleandoc("""
        SELECT
            DATE_FORMAT(window_start, 'yyyy-MM-dd''T''HH:mm:ss''Z''') AS window_start,
            DATE_FORMAT(window_end,   'yyyy-MM-dd''T''HH:mm:ss''Z''') AS window_end,
            supplier,
            SUM(CAST(price AS DECIMAL(10, 2))) AS total_price,
            count(*) AS `count`
        FROM TABLE(
            TUMBLE(TABLE orders, DESCRIPTOR(bid_time), INTERVAL '5' SECOND))
        GROUP BY window_start, window_end, supplier;
    """)

    session_handle = None
    operation_handle = None
    try:
        session_handle = get_session_handle(HTTP, BASE_URL)
        execute_statement(HTTP, BASE_URL, session_handle, DDL_STMT)
        time.sleep(2)
        operation_handle = execute_statement(HTTP, BASE_URL, session_handle, DML_STMT)
        query_results(
            HTTP, BASE_URL, session_handle, operation_handle, args.max_seconds
        )
    except KeyboardInterrupt:
        print("🛑 Interrupted by user.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cancel_operation(HTTP, BASE_URL, session_handle, operation_handle)
