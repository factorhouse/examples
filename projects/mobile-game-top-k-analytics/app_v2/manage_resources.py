import argparse
import inspect
import logging

import psycopg2
from psycopg2 import sql

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)


def get_ddl_stmts(schema: str):
    """
    Returns a dictionary of table names to their CREATE TABLE DDL statements.
    """
    return {
        "top_teams": inspect.cleandoc(f"""
            CREATE TABLE IF NOT EXISTS {schema}.top_teams (
                rnk             BIGINT PRIMARY KEY,
                team_id         TEXT,
                team_name       TEXT,
                total_score     BIGINT
            );"""),
        "top_players": inspect.cleandoc(f"""
            CREATE TABLE IF NOT EXISTS {schema}.top_players (
                rnk             BIGINT PRIMARY KEY,
                user_id         TEXT,
                team_name       TEXT,
                total_score     BIGINT
            );"""),
        "hot_streaks": inspect.cleandoc(f"""
            CREATE TABLE IF NOT EXISTS {schema}.hot_streaks (
                rnk             BIGINT PRIMARY KEY,
                user_id         TEXT,
                short_term_avg  DOUBLE PRECISION,
                long_term_avg   DOUBLE PRECISION,
                peak_hotness    DOUBLE PRECISION
            );"""),
        "team_mvps": inspect.cleandoc(f"""
            CREATE TABLE IF NOT EXISTS {schema}.team_mvps (
                rnk             BIGINT PRIMARY KEY,
                user_id         TEXT,
                team_name       TEXT,
                player_total    BIGINT,
                team_total      BIGINT,
                contrib_ratio   DOUBLE PRECISION
            );
        """),
    }


class DBManager:
    def __init__(self, args: argparse.Namespace):
        """
        Initializes the DBManager with database connection parameters.
        """
        self.db_params = {
            "host": args.host,
            "port": args.port,
            "user": args.username,
            "password": args.password,
            "dbname": args.database,
        }
        self.schema = args.schema
        self.all_ddl_stmts = get_ddl_stmts(self.schema)

    def create_tables(self, table_names: list[str]):
        """
        Creates a schema and specified tables in the database.
        """
        logging.info("Connecting to the database...")
        try:
            with psycopg2.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    logging.info(
                        f"Creating schema '{self.schema}' if it does not exist."
                    )
                    cur.execute(
                        sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                            sql.Identifier(self.schema)
                        )
                    )

                    for table_name in table_names:
                        if table_name in self.all_ddl_stmts:
                            stmt = self.all_ddl_stmts[table_name]
                            logging.info(f"Creating table: {self.schema}.{table_name}")
                            cur.execute(stmt)
                        else:
                            logging.warning(
                                f"No DDL statement found for table '{table_name}'. Skipping."
                            )
            logging.info("Table creation process completed successfully.")
        except psycopg2.Error as e:
            logging.error(f"Database error during table creation: {e}")

    def delete_tables(self, table_names: list[str]):
        """
        Deletes specified tables from the database.
        """
        logging.info("Connecting to the database...")
        try:
            with psycopg2.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    for table_name in table_names:
                        logging.info(f"Dropping table: {self.schema}.{table_name}")
                        stmt = sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                            sql.Identifier(self.schema), sql.Identifier(table_name)
                        )
                        cur.execute(stmt)
            logging.info("Table deletion process completed successfully.")
        except psycopg2.Error as e:
            logging.error(f"Database error during table deletion: {e}")


def main():
    # fmt: off
    parser = argparse.ArgumentParser(
        description="A utility script to create or delete PostgreSQL tables required for the mobile game leaderboard V2."
    )
    parser.add_argument(
        "--action", 
        choices=["create", "delete"], 
        required=True, 
        help="The action to perform:\n'create' to build tables, or 'delete' to remove them."
    )
    parser.add_argument(
        "--table-names", 
        type=str, 
        default="top_teams,top_players,hot_streaks,team_mvps",
        help="A comma-separated list of table names to manage.\nDefaults to all tables: top_teams,top_players,hot_streaks,team_mvps."
    )
    parser.add_argument("--host", type=str, default="localhost", help="The hostname or IP address of the PostgreSQL server.")
    parser.add_argument("--port", type=int, default=5432, help="The port number of the PostgreSQL server.")
    parser.add_argument("--username", type=str, default="db_user", help="The username for connecting to the PostgreSQL database.")
    parser.add_argument("--password", type=str, default="db_password", help="The password for the specified user.")
    parser.add_argument("--database", type=str, default="fh_dev", help="The name of the PostgreSQL database to connect to.")
    parser.add_argument("--schema", type=str, default="demo", help="The PostgreSQL schema to create or delete tables within.")
    # fmt: on

    args = parser.parse_args()
    table_names = [t.strip() for t in args.table_names.split(",") if t.strip()]
    manager = DBManager(args)

    if not table_names:
        logging.error("No table names specified. Exiting.")
        return

    logging.info(
        f"Performing '{args.action}' action on tables: {', '.join(table_names)}"
    )

    if args.action == "create":
        manager.create_tables(table_names)
    elif args.action == "delete":
        manager.delete_tables(table_names)

    logging.info("Script finished.")


if __name__ == "__main__":
    main()
