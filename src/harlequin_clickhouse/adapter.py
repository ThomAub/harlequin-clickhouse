from __future__ import annotations

from typing import Any, Sequence

from clickhouse_driver.dbapi import Connection, connect
from harlequin import (
    HarlequinAdapter,
    HarlequinConnection,
    HarlequinCursor,
)
from harlequin.autocomplete.completion import HarlequinCompletion
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinConnectionError, HarlequinQueryError
from textual_fastdatatable.backend import AutoBackendType

from harlequin_clickhouse.cli_options import CLICKHOUSE_OPTIONS


class ClickHouseCatalogMixin:
    """Shared catalog and completion logic for both native and HTTP connections."""

    def _execute_catalog_query(self, sql: str) -> list[tuple]:
        raise NotImplementedError

    def get_catalog(self) -> Catalog:
        databases = self._list_databases()
        database_items: list[CatalogItem] = []
        for (db,) in databases:
            relations = self._list_relations_in_database(db)
            rel_items: list[CatalogItem] = []
            for rel, rel_type in relations:
                cols = self._list_columns_in_relation(db, rel)
                col_items = [
                    CatalogItem(
                        qualified_identifier=f'"{db}"."{rel}"."{col}"',
                        query_name=f'"{col}"',
                        label=col,
                        type_label=self._get_short_type(col_type),
                    )
                    for col, col_type in cols
                ]
                rel_items.append(
                    CatalogItem(
                        qualified_identifier=f'"{db}"."{rel}"',
                        query_name=f'"{db}"."{rel}"',
                        label=rel,
                        type_label="v" if rel_type == "VIEW" else "t",
                        children=col_items,
                    ),
                )
            database_items.append(
                CatalogItem(
                    qualified_identifier=f'"{db}"',
                    query_name=f'"{db}"',
                    label=db,
                    type_label="s",
                    children=rel_items,
                ),
            )
        return Catalog(items=database_items)

    def get_completions(self) -> list[HarlequinCompletion]:
        extra_keywords = ["foo", "bar", "baz"]
        return [
            HarlequinCompletion(
                label=item,
                type_label="kw",
                value=item,
                priority=1000,
                context=None,
            )
            for item in extra_keywords
        ]

    def _list_databases(self) -> list[tuple[str]]:
        return self._execute_catalog_query(
            """
            SELECT name
            FROM system.databases
            WHERE name NOT IN ('INFORMATION_SCHEMA', 'system', 'information_schema')
            """
        )

    def _list_relations_in_database(self, db: str) -> list[tuple[str, str]]:
        return self._execute_catalog_query(
            f"""
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = '{db}'
            ORDER BY table_name ASC
            """
        )

    def _list_columns_in_relation(self, db: str, relation: str) -> list[tuple[str, str]]:
        return self._execute_catalog_query(
            f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{db}'
              AND table_name = '{relation}'
            ORDER BY ordinal_position ASC
            """
        )

    @staticmethod
    def _get_short_type(type_name: str) -> str:
        MAPPING = {
            "UInt8": "#",
            "UInt16": "#",
            "UInt32": "#",
            "UInt64": "##",
            "UInt128": "##",
            "UInt256": "##",
            "Int8": "#",
            "Int16": "#",
            "Int32": "#",
            "Int64": "##",
            "Int128": "##",
            "Int256": "##",
            "Float32": "#.#",
            "Float64": "#.#",
            "Decimal": "#.#",
            "Boolean": "t/f",
            "String": "s",
            "FixedString": "s",
            "Date": "d",
            "Date32": "d",
            "DateTime": "ts",
            "DateTime64": "ts",
            "JSON": "{}",
            "UUID": "uid",
            "Enum": "e",
            "LowCardinality": "lc",
            "Array": "[]",
            "Map": "{}->{}",
            "SimpleAggregateFunction": "saf",
            "AggregateFunction": "af",
            "Nested": "tbl",
            "Tuple": "()",
            "Nullable": "?",
            "IPv4": "ip",
            "IPv6": "ip",
            "Point": "•",
            "Ring": "○",
            "Polygon": "▽",
            "MultiPolygon": "▽▽",
            "Expression": "expr",
            "Set": "set",
            "Nothing": "nil",
            "Interval": "|-|",
        }
        return MAPPING.get(type_name.split("(")[0].split(" ")[0], "?")


class HarlequinClickHouseCursor(HarlequinCursor):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.cur = args[0]
        self._limit: int | None = None

    def columns(self) -> list[tuple[str, str]]:
        return self.cur.columns_with_types

    def set_limit(self, limit: int) -> HarlequinClickHouseCursor:
        self._limit = limit
        return self

    def fetchall(self) -> AutoBackendType:
        try:
            if self._limit is None:
                return self.cur.fetchall()
            else:
                return self.cur.fetchmany(self._limit)
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while executing your query.",
            ) from e


class HarlequinClickHouseConnection(ClickHouseCatalogMixin, HarlequinConnection):
    def __init__(
        self,
        conn_str: Sequence[str],
        *args: Any,
        init_message: str = "Welcome to ClickHouse with harlequin",
        options: dict[str, Any],
    ) -> None:
        self.init_message = init_message
        self.conn_str = conn_str
        try:
            if len(conn_str) == 1:
                self.conn = connect(conn_str[0], **options)
            else:
                self.conn = connect(**options)
            cur = self.conn.cursor()
            cur.execute("SELECT 1")
        except Exception as e:
            raise HarlequinConnectionError(
                msg=str(e),
                title="Harlequin could not connect to your ClickHouse with clickhouse_driver.",
            ) from e

    def execute(self, query: str) -> HarlequinCursor | None:
        try:
            cur = self.conn.cursor()
            cur.execute(query)
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while executing your query.",
            ) from e
        else:
            if cur.description:
                return HarlequinClickHouseCursor(cur)
            else:
                return None

    def _execute_catalog_query(self, sql: str) -> list[tuple]:
        conn: Connection = self.conn
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()


class HarlequinClickHouseAdapter(HarlequinAdapter):
    ADAPTER_OPTIONS = CLICKHOUSE_OPTIONS

    def __init__(self, conn_str: Sequence[str], **options: Any) -> None:
        self.conn_str = conn_str
        self.options = options

    def connect(self) -> HarlequinConnection:
        options = dict(self.options)
        protocol = options.pop("protocol", "native")

        if options.get("port") is None:
            options["port"] = "8123" if protocol == "http" else "9000"

        if protocol == "http":
            try:
                import clickhouse_connect  # noqa: F401
            except ImportError as e:
                raise HarlequinConnectionError(
                    msg='To use HTTP protocol, install the http extra: pip install "harlequin-clickhouse[http]"',
                    title="Missing dependency for HTTP protocol.",
                ) from e
            from harlequin_clickhouse.http_connection import HarlequinClickHouseHttpConnection

            return HarlequinClickHouseHttpConnection(
                conn_str=self.conn_str,
                options=options,
            )
        else:
            return HarlequinClickHouseConnection(
                conn_str=self.conn_str,
                options=options,
            )
