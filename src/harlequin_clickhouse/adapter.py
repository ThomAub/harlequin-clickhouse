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


class HarlequinClickHouseCursor(HarlequinCursor):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.cur = args[0]
        self._limit: int | None = None

    def columns(self) -> list[tuple[str, str]]:
        # names = self.cur.column_names
        # types = self.cur.column_types
        # return list(zip(names, types))
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


class HarlequinClickHouseConnection(HarlequinConnection):
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

    def get_catalog(self) -> Catalog:
        # This is a small hack to overcome the fact that clickhouse doesn't
        # have the concept of schemas
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
        conn: Connection = self.conn
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    name
                FROM system.databases
                where name not in
                    ('INFORMATION_SCHEMA', 'system', 'information_schema');
            """,
            )
            results: list[tuple[str]] = cur.fetchall()
        return results

    def _list_relations_in_database(self, db: str) -> list[tuple[str, str]]:
        conn: Connection = self.conn
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    table_name,
                    table_type
                FROM information_schema.tables
                WHERE
                    table_schema = '{db}'
                ORDER BY table_name asc
                    """,
            )
            results: list[tuple[str]] = cur.fetchall()
        return results

    def _list_columns_in_relation(
        self,
        db: str,
        relation: str,
    ) -> list[tuple[str, str]]:
        conn: Connection = self.conn
        with conn.cursor() as cur:
            cur.execute(
                f"""
                select
                    column_name, data_type
                from information_schema.columns
                where
                    table_schema = '{db}'
                    and table_name = '{relation}'
                    order by ordinal_position asc
                    """,
            )
            results: list[tuple[str]] = cur.fetchall()
        return results

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


class HarlequinClickHouseAdapter(HarlequinAdapter):
    ADAPTER_OPTIONS = CLICKHOUSE_OPTIONS

    def __init__(self, conn_str: Sequence[str], **options: Any) -> None:
        self.conn_str = conn_str
        self.options = options

    def connect(self) -> HarlequinClickHouseConnection:
        conn = HarlequinClickHouseConnection(
            conn_str=self.conn_str,
            options=self.options,
        )
        return conn
