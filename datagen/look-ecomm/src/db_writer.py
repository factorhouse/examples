import dataclasses
import logging
from typing import List, Union, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import OperationalError
from psycopg2.extras import execute_values

from src.models import User, Order, OrderItem, Event, get_additional_ddls

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)


class DataWriter:
    def __init__(
        self,
        user: str,
        password: str,
        host: str,
        db_name: str,
        schema: str,
        batch_size: int = 1000,
        echo: bool = False,
    ):
        self.schema = schema
        self.batch_size = batch_size
        self.echo = echo
        self._user = user
        self._password = password
        self._host = host
        self._db_name = db_name
        self.conn: Optional[Connection] = None
        self._connect()

    def select(
        self,
        table: str,
        columns: Optional[List[str]] = None,
        where_clause: Optional[str] = None,
        where_params: Optional[tuple] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
    ):
        self._ensure_connection()

        cols = ", ".join(columns) if columns else "*"
        sql = f"SELECT {cols} FROM {self.schema}.{table}"
        if where_clause:
            sql += f" WHERE {where_clause}"
        if order_by:
            sql += f" ORDER BY {order_by}"
        if limit is not None:
            sql += f" LIMIT {limit}"

        result = self.conn.execute(text(sql), where_params or {})
        rows = result.mappings().all()
        return rows

    def upsert(
        self,
        table: str,
        data: List[Union[dict, object]],
        conflict_keys: List[str],
        update_fields: Optional[List[str]] = None,
    ):
        if not data:
            logging.info(f"[{table}] No rows to insert.")
            return

        rows = self._normalize_rows(data)
        columns = list(rows[0].keys())

        if update_fields is None:
            update_fields = [col for col in columns if col not in conflict_keys]

        insert_cols = ", ".join(columns)
        conflict_clause = ", ".join(conflict_keys)
        update_clause = ", ".join(f"{col} = EXCLUDED.{col}" for col in update_fields)

        sql = f"""
            INSERT INTO {self.schema}.{table} ({insert_cols})
            VALUES %s
            ON CONFLICT ({conflict_clause}) DO UPDATE SET
            {update_clause}
        """
        self._ensure_connection()

        try:
            for i in range(0, len(rows), self.batch_size):
                batch = rows[i : i + self.batch_size]
                values = [tuple(row[col] for col in columns) for row in batch]
                with self.conn.connection.cursor() as cur:
                    execute_values(cur, sql, values)

            self.conn.connection.commit()
            logging.info(
                f"[{table}] Inserted {len(rows)} rows in batches of {self.batch_size}."
            )

        except Exception as e:
            logging.error(f"Upsert failed for {table}: {e}")
            self.conn.connection.rollback()
            raise

    def get_all_tables(self):
        return [table for table, _ in self._ddl_stmts().items()]

    def create_tables_if_not_exists(self):
        for table, ddl in self._ddl_stmts().items():
            self._ensure_connection()
            logging.info(f"Creating table {self.schema}.{table}")
            self.conn.execute(text(ddl))

    def close(self):
        if self.conn:
            self.conn.close()

    def _ddl_stmts(self):
        return {
            "users": User.ddl(self.schema),
            "orders": Order.ddl(self.schema),
            "order_items": OrderItem.ddl(self.schema),
            "events": Event.ddl(self.schema),
            **get_additional_ddls(self.schema),
        }

    def _connect(self):
        engine = create_engine(
            f"postgresql+psycopg2://{self._user}:{self._password}@{self._host}/{self._db_name}",
            echo=self.echo,
        )
        self.conn = engine.connect()

    def _ensure_connection(self):
        try:
            if self.conn.closed:
                logging.warning("Reconnecting because connection is closed.")
                self._connect()
        except OperationalError:
            logging.warning("Reconnecting because connection is broken.")
            self._connect()

    @staticmethod
    def _normalize_rows(data: List[Union[dict, object]]) -> List[dict]:
        if not data:
            return []
        if dataclasses.is_dataclass(data[0]):
            return [dataclasses.asdict(obj) for obj in data]
        if isinstance(data[0], dict):
            return data
        raise TypeError("Each item must be a dataclass or dict.")
